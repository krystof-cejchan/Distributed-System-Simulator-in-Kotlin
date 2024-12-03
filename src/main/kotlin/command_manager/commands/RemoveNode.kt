package cz.krystofcejchan.command_manager.commands

import cz.krystofcejchan.command_manager.ICommand
import cz.krystofcejchan.entity.Node

class RemoveNode : ICommand {
    override fun commandTrigger(): String = "remove"

    override suspend fun commandExecute() {
        println("enter the node id:")
        val nodeId = readln()
        val newNode = Node(nodeId)
        super.network.removeNode(newNode)
    }

    override fun commandDesc(): String = "removes a node from the network"
}