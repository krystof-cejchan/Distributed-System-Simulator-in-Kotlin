package cz.krystofcejchan.command_manager

import cz.krystofcejchan.command_manager.commands.*

object CommandManager {
    private val commands = mutableSetOf<ICommand>()

    init {
        commands.add(AddNode())
        commands.add(RemoveNode())
        commands.add(PerformAlgorithm())
        commands.add(Log())
        commands.add(Message())
        commands.add(PrintInfo())
        commands.add(StopNode())
        commands.add(Exit())
    }

    fun findCommandByTrigger(trigger: String): ICommand? {
        return commands.find { trigger.equals(it.commandTrigger(), true) }
    }

    override fun toString(): String {
        return commands.joinToString("\n") { "\t" + it.commandTrigger() + " -> " + it.commandDesc() }
    }
}