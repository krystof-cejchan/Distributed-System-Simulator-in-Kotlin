package cz.krystofcejchan.entity

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import java.util.*
import kotlin.random.Random

enum class NodeState {
    FOLLOWER,
    CANDIDATE,
    LEADER
}

class Node(
    val id: String,
    private val network: Network,
    private val allNodeIds: List<String>,
    private val task: Runnable = Runnable { println(id) }
) {
    private val messageChannel = Channel<Message>(Channel.UNLIMITED)
    private var isActive = true
    private val coroutineScope = CoroutineScope(Dispatchers.Default + Job())

    // Raft stavové proměnné
    var state: NodeState = NodeState.FOLLOWER
        get() = synchronized(this) { field }
    private var currentTerm: Int = 0
    private var votedFor: String? = null
    private var voteCount: Int = 0

    // Timeouty pro Raft
    private var electionTimeoutJob: Job? = null
    private val random = Random(System.currentTimeMillis())

    // Stav tokenu pro vzájemné vyloučení
    var hasToken: Boolean = false
    private var requestingCS: Boolean = false

    // Fronta žádostí o token
    private val requestQueue: LinkedList<String> = LinkedList()

    // Spuštění uzlu jako coroutine
    fun start() = coroutineScope.launch {
        resetElectionTimeout()
        while (isActive) {
            val message = messageChannel.receive()
            handleMessage(message)
        }
    }

    // Zastavení uzlu
    fun stop() {
        isActive = false
        electionTimeoutJob?.cancel()
        coroutineScope.cancel()
    }

    // Odeslání zprávy
    suspend fun sendMessage(
        receiverId: String,
        content: String,
        type: MessageType = MessageType.REQUEST,
        term: Int = currentTerm,
        candidateId: String = id,
        voteGranted: Boolean = false
    ) {
        val message = Message(
            senderId = id,
            receiverId = receiverId,
            content = content,
            type = type,
            term = term,
            candidateId = candidateId,
            voteGranted = voteGranted
        )
        network.sendMessage(message)
    }

    // Přijetí zprávy
    suspend fun receiveMessage(message: Message) {
        messageChannel.send(message)
    }

    // Resetování election timeoutu
    private fun resetElectionTimeout(timeout: Long = (15000 + random.nextInt(15000)).toLong()) {
        electionTimeoutJob?.cancel()
        electionTimeoutJob = coroutineScope.launch {
            delay(timeout)
            onElectionTimeout()
        }
    }

    // Zpracování timeoutu
    private fun onElectionTimeout() {
        coroutineScope.launch {
            state = NodeState.CANDIDATE
            currentTerm += 1
            votedFor = id
            voteCount = 1 // Hlas pro sebe
            println("Uzel $id se stává CANDIDATE v termínu $currentTerm")
            // Poslat RequestVote zprávy ostatním uzlům
            allNodeIds.filter { it != id }.forEach { nodeId ->
                sendMessage(nodeId, "Requesting vote", MessageType.REQUEST_VOTE, currentTerm, id)
            }
            resetElectionTimeout()
        }
    }

    // Zpracování přijatých zpráv
    private fun handleMessage(message: Message) {
        coroutineScope.launch {
            when (message.type) {
                MessageType.REQUEST_VOTE -> handleRequestVote(message)
                MessageType.VOTE -> handleVote(message)
                MessageType.HEARTBEAT -> handleAppendEntries(message)
                MessageType.REQUEST_TOKEN -> handleRequestToken(message)
                MessageType.TOKEN -> handleToken(message)
                else -> handleBasicMessage(message)
            }
        }
    }

    // Zpracování základních zpráv (REQUEST, RESPONSE)
    private fun handleBasicMessage(message: Message) {
        when (message.type) {
            MessageType.REQUEST -> {
                println("Uzel $id obdržel REQUEST od ${message.senderId}: ${message.content}")
                // Odpověď pouze na REQUEST typu
                if (message.type != MessageType.RESPONSE) {
                    coroutineScope.launch {
                        sendMessage(
                            message.senderId,
                            "Přijato: ${message.content}",
                            MessageType.RESPONSE
                        )
                    }
                }
            }

            MessageType.RESPONSE -> {
                println("Uzel $id obdržel RESPONSE od ${message.senderId}: ${message.content}")
            }

            else -> {
                // Ignorovat ostatní zprávy
            }
        }
    }

    // Zpracování RequestVote zprávy
    private suspend fun handleRequestVote(message: Message) {
        if (message.term < currentTerm) {
            // Odpovědět, že hlasování je neplatné
            sendMessage(message.senderId, "Reject vote", MessageType.VOTE, currentTerm, id, voteGranted = false)
            return
        }

        if (message.term > currentTerm) {
            currentTerm = message.term
            state = NodeState.FOLLOWER
            votedFor = null
        }

        // Přidat další podmínky, např. kontrola logu (zjednodušeně)
        if (votedFor == null || votedFor == message.candidateId) {
            votedFor = message.candidateId
            resetElectionTimeout()
            sendMessage(message.senderId, "Grant vote", MessageType.VOTE, currentTerm, id, voteGranted = true)
            println("Uzel $id hlasuje pro ${message.candidateId} v termínu ${message.term}")
        } else {
            sendMessage(message.senderId, "Reject vote", MessageType.VOTE, currentTerm, id, voteGranted = false)
        }
    }

    // Zpracování Vote zprávy
    private fun handleVote(message: Message) {
        if (state != NodeState.CANDIDATE || message.term != currentTerm) {
            return
        }

        if (message.voteGranted) {
            voteCount += 1
            println("Uzel $id získal hlas od ${message.senderId}. Celkem hlasů: $voteCount")
            if (voteCount > allNodeIds.size / 2) {
                becomeLeader()
            }
        }
    }

    // Zpracování AppendEntries zprávy (Heartbeat)
    private fun handleAppendEntries(message: Message) {
        if (message.term < currentTerm) {
            // Ignorovat starší termín
            return
        }

        resetElectionTimeout()

        if (message.term > currentTerm || state != NodeState.FOLLOWER) {
            currentTerm = message.term
            state = NodeState.FOLLOWER
            votedFor = null
            println("Uzel $id se stává FOLLOWER v termínu $currentTerm po obdržení AppendEntries od ${message.senderId}")
        }

        // V reálném Raft by zde byla odpověď, že AppendEntries byla přijata
    }

    // Přechod do stavu Leader
    private fun becomeLeader() {
        state = NodeState.LEADER
        println("Uzel $id se stal LEADER v termínu $currentTerm")
        // Zrušit election timeout, protože již není potřeba
        electionTimeoutJob?.cancel()
        // Start sending heartbeats
        sendHeartbeats()
    }

    // Posílání heartbeat zpráv
    private fun sendHeartbeats() {
        coroutineScope.launch {
            while (state == NodeState.LEADER && isActive) {
                allNodeIds.filter { it != id }.forEach { nodeId ->
                    sendMessage(nodeId, "Heartbeat", MessageType.HEARTBEAT, currentTerm, id)
                }
                delay(50) // Heartbeat interval (např. 50 ms)
            }
        }
    }

    // Zpracování REQUEST_TOKEN zprávy pro vzájemné vyloučení
    private fun handleRequestToken(message: Message) {
        println("Uzel $id obdržel ${MessageType.REQUEST_TOKEN.name} od ${message.senderId}")
        if (hasToken && !requestingCS) {
            // Předat token
            hasToken = false
            sendToken(message.senderId)
        } else {
            // Přidat do fronty
            requestQueue.add(message.senderId)
        }
    }

    // Zpracování TOKEN zprávy pro vzájemné vyloučení
    private fun handleToken(message: Message) {
        println("Uzel $id obdržel TOKEN od ${message.senderId}")
        hasToken = true
        enterCriticalSection()
    }

    // Odeslání tokenu
    private fun sendToken(receiverId: String) = coroutineScope.launch {
        sendMessage(receiverId, "Token", MessageType.TOKEN)
    }

    // Vstup do kritické sekce
    fun enterCriticalSection() = coroutineScope.launch {
        if (requestingCS && hasToken) {
            println("Uzel $id vstupuje do kritické sekce.")
            // Simulace práce v kritické sekci
            delay(2000)
            println("Uzel $id opouští kritickou sekci.")
            requestingCS = false
            // Předání tokenu dalšímu v frontě nebo náhodně
            if (requestQueue.isNotEmpty()) {
                val nextNode = requestQueue.poll()
                sendToken(nextNode)
            } else {
                // Pokud nikdo nečeká, drží token
                hasToken = true
            }
        }
    }

    // Požadavek na vstup do kritické sekce
    fun requestCriticalSection() = coroutineScope.launch {
        if (!requestingCS) {
            requestingCS = true
            if (hasToken) {
                // Můžeme okamžitě vstoupit
                enterCriticalSection()
            } else {
                // Požádat o token
                sendMessage(getNextNodeId(), "Requesting token", MessageType.REQUEST_TOKEN)
            }
        }
    }

    // Funkce pro získání ID následujícího uzlu v pořadí
    private fun getNextNodeId(): String {
        val currentIndex = allNodeIds.indexOf(id)
        val nextIndex = (currentIndex + 1) % allNodeIds.size
        return allNodeIds[nextIndex]
    }

    // Metoda pro vlastní algoritmus, kterou může uzel periodicky vykonávat
    fun performAlgorithms() = coroutineScope.launch {
        while (isActive) {
            requestCriticalSection()
            task.run()
            delay(timeMillis = 5000)
        }
    }


    override fun toString(): String {
        return "Node(id='$id', isActive=$isActive, state=$state)"
    }


}
