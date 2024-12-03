package cz.krystofcejchan.command_manager.commands

import cz.krystofcejchan.command_manager.ICommand
import cz.krystofcejchan.entity.Node

internal class PerformAlgorithm : ICommand {
    override fun commandTrigger(): String = "do"


    override suspend fun commandExecute() {
        println("enter the node id:")
        val nodeId = readln()
        val node =  super.network.chordLookup(Node(nodeId))
        if (node == null) {
            println("not found | aborting")
            return
        }
        node.performAlgorithms()
    }

    override fun commandDesc(): String = "performs node's task"
}