package cz.krystofcejchan.entity

import cz.krystofcejchan.utils.logCt
import kotlin.math.abs
import kotlin.math.pow

private const val k = 32
private const val min = 0
private val max = (2.0.pow(k.toDouble()) - 1).toInt()

/**
 * hash ring
 */
class Network {
    private var head: Node? = null
    val nodes = mutableMapOf<String, Node>()

    private fun isInLegalRange(node: Node) = node.hash in min..max
    private fun distance(node1: Node, node2: Node): Int {
        val hash1 = node1.hash
        val hash2 = node2.hash
        return if (hash1 == hash2) {
            0
        } else if (hash1 < hash2) {
            hash2 - hash1
        } else (2.0.pow(k) + (hash2 - hash1)).toInt()
    }

    fun lookup(node: Node): Node? {
        if (nodes.isEmpty() || !isInLegalRange(node)) return null
        var temp = head ?: return null
        if (node.hash <= head!!.hash) {
            return temp
        } else {
            while (distance(temp, node) > distance(temp.next!!, node))
                temp = temp.next!!
            if (temp.hash == node.hash)
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
                val temp = lookup(node) ?: nodes.values.first()
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
        if (!isInLegalRange(node)) return
        if (head == null) {
            node.previous = node
            node.next = node
            head = node
        } else {
            val temp = lookup(node)
            node.next = temp
            node.previous = temp!!.previous
            node.previous!!.next = node
            node.next!!.previous = node

            if (node.hash < head!!.hash)
                head = node
        }
        nodes.putIfAbsent(node.id, node)
    }

    fun removeNode(node: Node) {
        var temp = lookup(node)
        if (nodes.size == 1) {
            temp = null
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
    }

    // Vytvoření propojení mezi uzly (pro tento jednoduchý příklad je vše propojeno)
    fun connect(nodeId1: String, nodeId2: String) {
        if (nodes.containsKey(nodeId1) && nodes.containsKey(nodeId2)) {
            logCt("Propojeno $nodeId1 <-> $nodeId2")
        } else {
            logCt("Propojení selhalo: Jeden z uzlů neexistuje.")
        }
    }

    // Odeslání zprávy prostřednictvím sítě
    suspend fun sendMessage(message: Message) {
        val receiver = nodes[message.receiverId]
        if (receiver != null) {
            receiver.receiveMessage(message)
        } else {
            logCt("Zpráva pro neexistující uzel: ${message.receiverId}")
        }
    }

    fun stopAllNodes() = this.nodes.values.forEach { it.stop() }

    override fun toString(): String {
        return "Network(nodes=$nodes)"
    }


}
