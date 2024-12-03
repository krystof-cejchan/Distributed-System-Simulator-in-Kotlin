package cz.krystofcejchan.command_manager.commands

import cz.krystofcejchan.command_manager.ICommand
import cz.krystofcejchan.utils.Logger

internal class Log : ICommand {
    override fun commandTrigger(): String {
        return "log"
    }

    override suspend fun commandExecute() {
        Logger.flip()
        println("Logging is set to ${Logger.loggingAllowed}")
    }

    override fun commandDesc(): String {
        return "turns on/off printing logs"
    }
}