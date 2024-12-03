package cz.krystofcejchan.command_manager

import cz.krystofcejchan.entity.Network

interface ICommand {
    val network: Network
        get() = Network

    fun commandTrigger(): String
    suspend fun commandExecute()
    fun commandDesc(): String
}