package cz.krystofcejchan.command_manager.commands

import cz.krystofcejchan.command_manager.ICommand
import cz.krystofcejchan.entity.Node

internal class AddNode : ICommand {
    override fun commandTrigger(): String {
        return "add"
    }

    override suspend fun commandExecute() {
        println("enter the node id:")
        val nodeId = readln()
        super.network.addNode(Node(nodeId))
    }

    override fun commandDesc(): String {
        return "adds a node to the network"
    }
}