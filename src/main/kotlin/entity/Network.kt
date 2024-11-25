package cz.krystofcejchan.entity
// Network.kt

class Network {
    val nodes = mutableMapOf<String, Node>()

    // Registrace uzlu do sítě
    fun addNode(node: Node) {
        nodes[node.id] = node
    }

    // Vytvoření propojení mezi uzly (pro tento jednoduchý příklad je vše propojeno)
    fun connect(nodeId1: String, nodeId2: String) {
        if (nodes.containsKey(nodeId1) && nodes.containsKey(nodeId2)) {
            println("Propojeno $nodeId1 <-> $nodeId2")
        } else {
            println("Propojení selhalo: Jeden z uzlů neexistuje.")
        }
    }

    // Odeslání zprávy prostřednictvím sítě
    suspend fun sendMessage(message: Message) {
        val receiver = nodes[message.receiverId]
        if (receiver != null) {
            receiver.receiveMessage(message)
        } else {
            println("Zpráva pro neexistující uzel: ${message.receiverId}")
        }
    }

    fun stopAllNodes() = this.nodes.values.forEach { it.stop() }
    override fun toString(): String {
        return "Network(nodes=$nodes)"
    }


}
