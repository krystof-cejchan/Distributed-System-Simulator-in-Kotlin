/**
 * @author Kryštof Čejchan
 */
package cz.krystofcejchan

import cz.krystofcejchan.command_manager.CommandManager
import cz.krystofcejchan.command_manager.commands.Exit
import cz.krystofcejchan.entity.Network
import cz.krystofcejchan.entity.Node
import cz.krystofcejchan.utils.logCt
import kotlinx.coroutines.runBlocking
import kotlin.system.exitProcess

private val network = Network

// Seznam všech uzlů
private val allNodeIds = setOf("A", "B", "C")

fun main(): Unit = runBlocking {

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


    val cmdManager = CommandManager
    val exitCommandTrigger = Exit().commandTrigger()
    var input = ""
    while (!exitCommandTrigger.equals(input, ignoreCase = true)) {
        println(cmdManager.toString())
        input = readln()
        cmdManager.findCommandByTrigger(input)?.commandExecute() ?: "COMMAND $input WAS NOT FOUND"
    }

    network.stopAllNodes()
    logCt("Simulace dokončena.")
    exitProcess(0)
}
