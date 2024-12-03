package cz.krystofcejchan.command_manager.commands

import cz.krystofcejchan.command_manager.ICommand

class StopNode : ICommand {
    override fun commandTrigger(): String {
        return "stop"
    }

    override suspend fun commandExecute() {
        println("enter the node id:")
        val nodeId = readln()
        super.network.nodes.getOrDefault(nodeId, null)?.stop()
    }

    override fun commandDesc(): String {
        return "stops a node"
    }
}