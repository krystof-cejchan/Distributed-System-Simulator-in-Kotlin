package cz.krystofcejchan.entity

import cz.krystofcejchan.utils.logCt
import java.util.concurrent.ConcurrentLinkedQueue
import kotlin.math.abs
import kotlin.math.pow

private const val k = 32
private const val min = 0
private val max = (2.0.pow(k.toDouble()) - 1).toInt()

/**
 * network connecting Nodes as hash ring
 * singleton
 * @see Node
 */
object Network {
    private var head: Node? = null
    internal val nodes = HashMap<String, Node>()

    // Fronta žádostí o token
    internal val requestQueue: ConcurrentLinkedQueue<String> = ConcurrentLinkedQueue()


    private fun isInLegalRange(hashValue: Int) = hashValue in min..max

    private fun distance(hash1: Int, hash2: Int): Int {
        return if (hash1 == hash2) 0
        else if (hash1 < hash2) hash2 - hash1
        else (2.0.pow(k) + (hash2 - hash1)).toInt()
    }

    fun lookup(hashValue: Int): Node? {
        if (nodes.isEmpty() || !isInLegalRange(hashValue)) return null
        var temp = head ?: return null
        if (hashValue <= head!!.hash) {
            return temp
        } else {
            while (distance(temp.hash, hashValue) > distance(temp.next!!.hash, hashValue))
                temp = temp.next!!
            if (temp.hash == hashValue)
                return temp
            return temp.next
        }
    }

    /**
     * chord lookup
     */
    fun chordLookup(node: Node): Node? {
        if (nodes.isEmpty()) return null
        val visited = mutableSetOf<Node>()
        val head = this.head
        if (node.hash <= head!!.hash || node.hash > head.previous!!.hash) {
            return if (node.id == head.id) head else null
        }
        fun recursiveLookup(currNode: Node): Node? {
            visited.add(currNode)
            if (node.hash <= currNode.hash) {
                return if (node.id == currNode.id) currNode else null
            }
            val firstFromCurrFingerTable = currNode.fingerTable.first()
            if (node.hash <= firstFromCurrFingerTable.hash) {
                return if (node.id == firstFromCurrFingerTable.id) firstFromCurrFingerTable else null
            }
            val nextNode = getNextNodeFromFingerTable(currNode, node.hash)
            if (nextNode in visited) {
                return if (node.id == nextNode!!.id) nextNode else null
            }
            return recursiveLookup(nextNode!!)

        }

        return recursiveLookup(head)
    }

    fun buildFingerTables() {
        for (node in nodes.values) {
            for (i in 0 until k) {
                val value = node.hash + 2.0.pow(i)
                val temp = lookup(value.toInt()) ?: nodes.values.first()
                node.fingerTable.add(temp)
            }
        }
    }

    private fun getNextNodeFromFingerTable(currentNode: Node, hashValue: Int) =
        currentNode.fingerTable
            .filter { it.hash <= hashValue }
            .minByOrNull { abs(hashValue - it.hash) }


    // Registrace uzlu do sítě
    fun addNode(node: Node) {
        if (!isInLegalRange(node.hash)) return
        if (head == null) {
            node.previous = node
            node.next = node
            head = node
        } else {
            val temp = lookup(node.hash)
            node.next = temp
            node.previous = temp!!.previous
            node.previous!!.next = node
            node.next!!.previous = node

            if (node.hash < head!!.hash)
                head = node
        }
        buildFingerTables()
        nodes.putIfAbsent(node.id, node).also { node.start() }
    }

    fun removeNode(node: Node) {
        val temp = lookup(node.hash)
        if (nodes.size == 1) {
            head = null
            return
        }
        if (temp != null && temp.hash == node.hash) {
            temp.previous!!.next = temp.next
            temp.next!!.previous = temp.previous
            if (head!!.hash == node.hash) {
                head = temp.next
            }
        }
        nodes.getOrElse(node.id) { node }.let {
            nodes.remove(it.id)
            it.stop()
        }
        buildFingerTables()
    }

    // Vytvoření propojení mezi uzly
    fun connect(nodeId1: String, nodeId2: String) {
        if (nodes.containsKey(nodeId1) && nodes.containsKey(nodeId2)) {
            logCt("Propojeno $nodeId1 <-> $nodeId2")
        } else {
            logCt("Propojení selhalo: Jeden z uzlů neexistuje.")
        }
    }

    // Odeslání zprávy prostřednictvím sítě
    suspend fun sendMessage(message: Message) {
        val receiver = chordLookup(Node(message.receiverId))
        receiver?.receiveMessage(message) ?: logCt("Zpráva pro neexistující uzel: ${message.receiverId}")
    }

    fun stopAllNodes() = this.nodes.values.forEach { it.stop() }

    override fun toString(): String {
        return "Network(nodes=$nodes)"
    }


}
