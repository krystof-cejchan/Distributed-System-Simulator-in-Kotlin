import cz.krystofcejchan.entity.Network
import cz.krystofcejchan.entity.Node
import cz.krystofcejchan.entity.NodeState
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import kotlin.test.assertNotNull

private val network = Network

class DistributedSystemTest {
    companion object {
        @JvmStatic
        @BeforeAll
        fun initNetwork() {
            network.nodes.clear()
            val allNodeIds = setOf("A", "B", "C")

            // Vytvoření uzlů
            val nodes = allNodeIds.map { nodeId ->
                Node(nodeId)
            }

            // Registrace uzlů do sítě
            nodes.forEach { node ->
                network.addNode(node)
            }.also { network.buildFingerTables() }

            // Propojení uzlů
            allNodeIds.forEach { nodeId ->
                val otherNodes = allNodeIds.filter { it != nodeId }
                otherNodes.forEach { otherNodeId ->
                    network.connect(nodeId, otherNodeId)
                }
            }

            // Start uzlů
            nodes.forEach {
                it.start()
            }

            // Inicializace tokenu
            nodes.randomOrNull()?.let {
                it.hasToken = true
                it.enterCriticalSection()
            }

        }
    }

    @Test
    fun chord() {
        assertNotNull(network.chordLookup(Node("A")))
        assertNotNull(network.chordLookup(Node("B")))
        assertNotNull(network.chordLookup(Node("C")))
        network.addNode(Node("D"))
        network.addNode(Node("QZQ"))
        assertNotNull(network.chordLookup(Node("D")))
        assertNull(network.chordLookup(Node("E")))
    }

    @Test
    fun leader() {
        while (network.nodes.values.firstOrNull { it.state == NodeState.LEADER } == null) {
        }
        network.addNode(Node("D"))
        network.addNode(Node("QZQ"))
        network.addNode(Node("DE"))
        network.addNode(Node("QZQQ"))
        network.nodes.values.firstOrNull { it.state == NodeState.LEADER }?.let {
            network.removeNode(it)
        }
        while (network.nodes.values.firstOrNull { it.state == NodeState.LEADER } == null) {
        }
        assertNotNull(network.nodes.values.firstOrNull { it.state == NodeState.LEADER })
    }

    @Test
    fun token() {
        val haveHadToken = mutableSetOf<Node>()
        while (haveHadToken.size < network.nodes.size) {
            network.nodes.values.firstOrNull { it.hasToken }?.let { haveHadToken.add(it) }
        }
        network.nodes.values.forEach { assert(haveHadToken.contains(it)) }
    }

}