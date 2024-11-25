package cz.krystofcejchan.entity

enum class MessageType {
    REQUEST,
    RESPONSE,
    REQUEST_TOKEN,
    TOKEN,
    REQUEST_VOTE,
    VOTE,
    HEARTBEAT
}

data class Message(
    val senderId: String,
    val receiverId: String,
    val content: String,
    val type: MessageType = MessageType.REQUEST,
    val term: Int = 0,
    val candidateId: String = "",
    val voteGranted: Boolean = false
)

