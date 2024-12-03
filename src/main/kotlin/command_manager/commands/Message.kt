package cz.krystofcejchan.command_manager.commands

import cz.krystofcejchan.command_manager.ICommand
import cz.krystofcejchan.entity.Node
import cz.krystofcejchan.entity.NodeState

class Message : ICommand {
    override fun commandTrigger(): String {
        return "send"
    }

    override suspend fun commandExecute() {
        println("enter the node id:")
        val nodeId = readln()
        val node = super.network.chordLookup(Node(nodeId))
        if (node == null) {
            println("not found | aborting")
            return
        }
        println("enter the message:")
        val msg = readln()

        (super.network.nodes.values.firstOrNull { it.state == NodeState.LEADER } ?: super.network.nodes.values.random())
            .sendMessage(node.id, msg)
    }

    override fun commandDesc(): String {
        return "sends a message to a node"
    }
}