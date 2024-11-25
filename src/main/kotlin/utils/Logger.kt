package cz.krystofcejchan.utils


class Logger {
    companion object {
        @JvmStatic
        internal var loggingAllowed = true

        @JvmStatic
        fun flip() {
            loggingAllowed = !loggingAllowed
        }
    }
}

fun logCt(message: Any) {
    if (Logger.loggingAllowed) {
        println(message.toString())
    }
}