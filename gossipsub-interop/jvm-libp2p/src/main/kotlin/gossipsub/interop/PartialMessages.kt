package gossipsub.interop

import io.libp2p.core.PeerId
import io.libp2p.pubsub.Topic
import io.libp2p.pubsub.gossip.Gossip
import io.libp2p.pubsub.gossip.GossipRouter
import io.libp2p.pubsub.gossip.partialmessages.PartialMessagesHandler
import io.libp2p.pubsub.gossip.partialmessages.PartialMessagesPeerFeedback
import io.libp2p.pubsub.gossip.partialmessages.PublishAction
import io.libp2p.pubsub.gossip.partialmessages.PublishActionsFn
import pubsub.pb.Rpc
import java.nio.ByteBuffer

const val PART_LEN = 1024

/**
 * Per-peer state for a partial-messages group.
 *
 * [theirKnownParts] bitmap of parts the peer has told us they have.
 * [ourSentMetadata] bitmap of parts we have told this peer we have.
 * [sentInitialMetadata] whether we have sent any partsMetadata to this peer.
 * [receivedInitialMetadata] whether we have received any partsMetadata from this peer.
 */
data class PartialPeerState(
    val theirKnownParts: Int = 0,
    val ourSentMetadata: Int = 0,
    val sentInitialMetadata: Boolean = false,
    val receivedInitialMetadata: Boolean = false,
)

class InteropPartialMessagesHandler : PartialMessagesHandler<PartialPeerState> {
    // gossip and router are set after construction to avoid circular init
    var gossip: Gossip? = null
    var router: GossipRouter? = null

    // (topicId:groupIdHex) -> 8-slot parts array; access synchronized on the array
    private val groupParts = HashMap<String, Array<ByteArray?>>()
    // Set of keys that have already logged "All parts received"
    private val completedGroups = HashSet<String>()

    private fun groupKey(topicId: String, groupId: ByteArray) =
        "$topicId:${groupId.joinToString("") { "%02x".format(it) }}"

    fun groupIdToBytes(groupID: Long): ByteArray =
        ByteBuffer.allocate(8).putLong(groupID).array()

    /** Called from the script thread for AddPartialMessage. */
    fun addParts(topicId: String, groupId: ByteArray, partsBitmap: Int) {
        val key = groupKey(topicId, groupId)
        val parts = synchronized(groupParts) {
            groupParts.getOrPut(key) { arrayOfNulls(8) }
        }
        var complete = false
        synchronized(parts) {
            for (i in 0..7) {
                if (partsBitmap and (1 shl i) != 0 && parts[i] == null) {
                    parts[i] = generatePart(i)
                }
            }
            complete = parts.all { it != null }
        }
        if (complete) {
            checkAndLogComplete(key, groupId)
        }
    }

    private fun generatePart(partIndex: Int): ByteArray = ByteArray(PART_LEN) { partIndex.toByte() }

    private fun checkAndLogComplete(key: String, groupId: ByteArray) {
        val alreadyLogged = synchronized(completedGroups) { !completedGroups.add(key) }
        if (!alreadyLogged) {
            val groupIdLong = ByteBuffer.wrap(groupId).getLong()
            JsonLogger.logStdout("All parts received", "group id" to groupIdLong)
        }
    }

    fun getCollectedBitmap(topicId: String, groupId: ByteArray): Int {
        val key = groupKey(topicId, groupId)
        val parts = synchronized(groupParts) { groupParts[key] } ?: return 0
        return synchronized(parts) {
            parts.indices.fold(0) { acc, i -> if (parts[i] != null) acc or (1 shl i) else acc }
        }
    }

    fun buildPublishActionsFn(topicId: String, groupId: ByteArray): PublishActionsFn<PartialPeerState> {
        val key = groupKey(topicId, groupId)
        return PublishActionsFn { peerStates, peerRequestsPartial ->
            val parts = synchronized(groupParts) { groupParts[key] }
            val myBitmap = if (parts != null) {
                synchronized(parts) {
                    parts.indices.fold(0) { acc, i -> if (parts[i] != null) acc or (1 shl i) else acc }
                }
            } else 0

            // Bootstrap: add any mesh peers for this topic that aren't yet in peerStates
            @Suppress("UNCHECKED_CAST")
            val mutableStates = peerStates as MutableMap<PeerId, PartialPeerState>
            router?.mesh?.get(topicId)?.forEach { peerHandler ->
                mutableStates.getOrPut(peerHandler.peerId) { PartialPeerState() }
            }

            sequence {
                for ((peerId, peerState) in mutableStates) {
                    var partialMessageBytes: ByteArray? = null
                    var partsMetadataBytes: ByteArray? = null
                    var newSentMetadata = peerState.ourSentMetadata

                    // Send actual parts if peer requests partial and has sent initial metadata
                    var newTheirKnownParts = peerState.theirKnownParts
                    if (peerRequestsPartial(peerId) && peerState.receivedInitialMetadata && parts != null) {
                        val missingForPeer = myBitmap and peerState.theirKnownParts.inv()
                        if (missingForPeer != 0) {
                            partialMessageBytes = encodePartialMessage(groupId, parts, missingForPeer)
                            newSentMetadata = newSentMetadata or myBitmap
                            // After sending, peer has all our parts (they can infer it from the message)
                            newTheirKnownParts = newTheirKnownParts or myBitmap
                        }
                    }

                    // Send partsMetadata if we haven't yet or we have new parts to announce
                    val newParts = myBitmap and newSentMetadata.inv()
                    if (myBitmap != 0 && (!peerState.sentInitialMetadata || newParts != 0)) {
                        newSentMetadata = newSentMetadata or myBitmap
                        partsMetadataBytes = byteArrayOf(newSentMetadata.toByte())
                    }

                    if (partialMessageBytes != null || partsMetadataBytes != null) {
                        yield(
                            peerId to PublishAction(
                                partialMessage = partialMessageBytes,
                                partsMetadata = partsMetadataBytes,
                                nextPeerState = peerState.copy(
                                    ourSentMetadata = newSentMetadata,
                                    sentInitialMetadata = true,
                                    theirKnownParts = newTheirKnownParts,
                                )
                            )
                        )
                    }
                }
            }
        }
    }

    private fun encodePartialMessage(groupId: ByteArray, parts: Array<ByteArray?>, missingForPeer: Int): ByteArray? {
        val included = synchronized(parts) {
            parts.indices.filter { i -> missingForPeer and (1 shl i) != 0 && parts[i] != null }
        }
        if (included.isEmpty()) return null

        val bitmap = included.fold(0) { acc, i -> acc or (1 shl i) }
        val buf = ByteBuffer.allocate(1 + included.size * PART_LEN + groupId.size)
        buf.put(bitmap.toByte())
        synchronized(parts) {
            for (i in included) {
                buf.put(parts[i]!!)
            }
        }
        buf.put(groupId)
        return buf.array()
    }

    override fun onIncomingRpc(
        from: PeerId,
        peerStates: Map<PeerId, PartialPeerState>,
        rpc: Rpc.PartialMessagesExtension,
        feedback: PartialMessagesPeerFeedback,
    ) {
        val topicId = rpc.topicID
        val groupId = rpc.groupID.toByteArray()
        val key = groupKey(topicId, groupId)

        // peerStates is the live MutableMap from PartialGroupStateStore - safe to cast and update
        @Suppress("UNCHECKED_CAST")
        val mutableStates = peerStates as MutableMap<PeerId, PartialPeerState>
        val currentState = mutableStates[from] ?: PartialPeerState()

        var theirKnownParts = currentState.theirKnownParts
        var receivedInitialMetadata = currentState.receivedInitialMetadata

        // Ingest partsMetadata
        if (rpc.hasPartsMetadata() && rpc.partsMetadata.size() > 0) {
            theirKnownParts = theirKnownParts or (rpc.partsMetadata.byteAt(0).toInt() and 0xFF)
            receivedInitialMetadata = true
        }

        // Ingest partialMessage bytes
        if (rpc.hasPartialMessage() && rpc.partialMessage.size() > 0) {
            val data = rpc.partialMessage.toByteArray()
            val incomingBitmap = data[0].toInt() and 0xFF
            theirKnownParts = theirKnownParts or incomingBitmap
            receivedInitialMetadata = true

            val parts = synchronized(groupParts) {
                groupParts.getOrPut(key) { arrayOfNulls(8) }
            }
            var complete = false
            synchronized(parts) {
                val dataEnd = data.size - groupId.size
                var offset = 1
                for (i in 0..7) {
                    if (incomingBitmap and (1 shl i) != 0) {
                        if (parts[i] == null && offset + PART_LEN <= dataEnd) {
                            parts[i] = data.copyOfRange(offset, offset + PART_LEN)
                        }
                        offset += PART_LEN
                    }
                }
                complete = parts.all { it != null }
            }
            if (complete) {
                checkAndLogComplete(key, groupId)
            }
        }

        mutableStates[from] = currentState.copy(
            theirKnownParts = theirKnownParts,
            receivedInitialMetadata = receivedInitialMetadata,
        )

        // React immediately: publish our state back to all peers (like Go does after every RPC)
        router?.publishPartial(topicId, groupId, buildPublishActionsFn(topicId, groupId))
    }

    override fun onEmitGossip(
        topic: Topic,
        groupId: ByteArray,
        gossipPeers: Collection<PeerId>,
        peerStates: Map<PeerId, PartialPeerState>,
        feedback: PartialMessagesPeerFeedback,
    ) {
        val g = gossip ?: return
        val myBitmap = getCollectedBitmap(topic, groupId)
        if (myBitmap == 0) return

        // peerStates is the live MutableMap; pre-populate with gossipPeers so decide() can iterate them.
        // Without this, peerStates is empty on first call and nothing is sent.
        @Suppress("UNCHECKED_CAST")
        val mutableStates = peerStates as MutableMap<PeerId, PartialPeerState>
        for (peer in gossipPeers) {
            mutableStates.getOrPut(peer) { PartialPeerState() }
        }

        // submit to event thread; runs after the heartbeat returns, by which time peerStates is populated
        g.publishPartial(topic, groupId, buildPublishActionsFn(topic, groupId))
    }
}
