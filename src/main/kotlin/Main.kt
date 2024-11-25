/**
 * @author Kryštof Čejchan
 */
package cz.krystofcejchan

import cz.krystofcejchan.entity.Network
import cz.krystofcejchan.entity.Node
import cz.krystofcejchan.entity.NodeState
import cz.krystofcejchan.utils.Logger
import cz.krystofcejchan.utils.logCt
import kotlinx.coroutines.runBlocking
import kotlin.system.exitProcess

private val network = Network()

// Seznam všech uzlů
val allNodeIds = mutableListOf("A", "B", "C", "D", "E")
fun main(): Unit = runBlocking {

    // Vytvoření uzlů
    val nodes = allNodeIds.map { nodeId ->
        Node(nodeId, network, allNodeIds)
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
    nodes.forEach { node ->
        node.start()
    }

    // Inicializace tokenu - například uzel A drží token na začátku
    nodes.find { it.id == "A" }?.let { nodeA ->
        nodeA.hasToken = true
        nodeA.enterCriticalSection()
    }


    var input = ""
    while (!input.equals("exit", true)) {
        println(
            """
            print = print info about the network and its nodes
            exit = exit the application
            stop = stop a node
            election = start an election
            msg = send a message to a node   
            do = perform node's algorithm
            log = turn on/off console logging
            add = add node
            remove = remove node
        """
        )
        input = readln()
        handleInput(input)
    }

    network.stopAllNodes()
    logCt("Simulace dokončena.")
    exitProcess(0)
}

suspend fun handleInput(input: String) {
    when (input.lowercase()) {
        "print" -> println(network.toString())
        "stop" -> {
            println("enter the node id:")
            val nodeId = readln()
            network.nodes.getOrDefault(nodeId, null)?.stop()
        }

        "msg" -> {
            println("enter the node id:")
            val nodeId = readln()
            val node = network.nodes.getOrDefault(nodeId, null)
            if (node == null) {
                println("not found | aborting")
                return
            }
            println("enter the message:")
            val msg = readln()

            (network.nodes.values.firstOrNull { it.state == NodeState.LEADER } ?: network.nodes.values.random())
                .sendMessage(node.id, msg)
        }

        "do" -> {
            println("enter the node id:")
            val nodeId = readln()
            val node = network.nodes.getOrDefault(nodeId, null)
            if (node == null) {
                println("not found | aborting")
                return
            }
            node.performAlgorithms()
        }

        "log" -> {
            Logger.flip()
            println("Logging is set to ${Logger.loggingAllowed}")
        }

        "election" -> return //todo handle

        "add" -> {
            println("enter the node id:")
            val nodeId = readln()
            allNodeIds.add(nodeId)
            val newNode = Node(nodeId, network, allNodeIds)
            network.addNode(newNode)
        }

        "remove" -> {
            println("enter the node id:")
            val nodeId = readln()
            val newNode = Node(nodeId, network, allNodeIds)
            allNodeIds.remove(nodeId)
            network.removeNode(newNode)
        }

        else -> return
    }
}