package cz.krystofcejchan.command_manager.commands

import cz.krystofcejchan.command_manager.ICommand

class PrintInfo : ICommand {
    override fun commandTrigger(): String {
        return "print"
    }

    override suspend fun commandExecute() {
        println(super.network.toString())
    }

    override fun commandDesc(): String {
        return "prints info about the network"
    }
}