package cz.krystofcejchan.command_manager.commands

import cz.krystofcejchan.command_manager.ICommand

class Exit : ICommand {
    override fun commandTrigger(): String {
        return "exit"
    }

    override suspend fun commandExecute() {
        return
    }

    override fun commandDesc(): String {
        return "exits the application"
    }
}