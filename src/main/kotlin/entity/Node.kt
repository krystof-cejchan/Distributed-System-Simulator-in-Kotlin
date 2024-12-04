package cz.krystofcejchan.entity

import cz.krystofcejchan.utils.logCt
import cz.krystofcejchan.utils.mmh3
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlin.random.Random
import kotlin.time.Duration.Companion.milliseconds

enum class NodeState {
    FOLLOWER,
    CANDIDATE,
    LEADER
}

private val HEARTBEAT_INTERVAL = 250.milliseconds
private const val MIN_ELECTION_TIMEOUT = 3000L // v milisekundách
private const val MAX_ELECTION_TIMEOUT = 8000L // v milisekundách

class Node(
    val id: String,
    private val task: Runnable = Runnable { println(id) },
    var previous: Node? = null,
    var next: Node? = null,
    val fingerTable: MutableSet<Node> = mutableSetOf(),
    val hash: Int = id.mmh3()
) {
    private val messageChannel = Channel<Message>(Channel.UNLIMITED)
    private var isActive = true
    private val coroutineScope = CoroutineScope(Dispatchers.Default + Job())

    // Raft stavové proměnné
    var state: NodeState = NodeState.FOLLOWER
    private var currentTerm: Int = 0
    private var votedFor: String? = null
    private var voteCount: Int = 0

    // Timeouty pro Raft
    private var electionTimeoutJob: Job? = null
    private var electionProcessJob: Job? = null
    private val random = Random(System.currentTimeMillis())

    // Stav tokenu pro vzájemné vyloučení
    var hasToken: Boolean = false
    private var requestingCS: Boolean = false
    private var tokenDeferred: CompletableDeferred<Unit>? = null


    // "zámek" pro zamezení spuštění algoritmu více než jednou
    private var isPerformingAlgorithm: Boolean = false

    // Job pro heartbeat
    private var heartbeatJob: Job? = null

    // Spuštění uzlu jako coroutine
    fun start() = coroutineScope.launch {
        resetElectionTimeout()
        while (this@Node.isActive) {
            val message = messageChannel.receive()
            handleMessage(message)
        }
    }

    // Zastavení uzlu
    fun stop() {
        isActive = false
        electionTimeoutJob?.cancel()
        electionProcessJob?.cancel()
        heartbeatJob?.cancel()
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
        Network.sendMessage(message)
    }

    // Přijetí zprávy
    suspend fun receiveMessage(message: Message) {
        messageChannel.send(message)
    }

    // Resetování election timeoutu
    private fun resetElectionTimeout() {
        electionTimeoutJob?.cancel()
        val timeout = random.nextLong(MIN_ELECTION_TIMEOUT, MAX_ELECTION_TIMEOUT)
        electionTimeoutJob = coroutineScope.launch {
            delay(timeout)
            onElectionTimeout()
        }
    }

    // Zpracování timeoutu
    private fun onElectionTimeout() {
        if (state == NodeState.FOLLOWER && this.isActive) {
            coroutineScope.launch {
                state = NodeState.CANDIDATE
                currentTerm += 1
                votedFor = id
                voteCount = 1 // Hlas pro sebe
                logCt("Uzel $id se stává CANDIDATE v termínu $currentTerm")
                // Poslat RequestVote zprávy ostatním uzlům
                allNodeIds(true).filter { it != id }.forEach { nodeId ->
                    sendMessage(nodeId as String, "Requesting vote", MessageType.REQUEST_VOTE, currentTerm, id)
                }
                // Zahájíme volby a čekáme na odpovědi
                startElectionProcess()
            }
        }
    }

    // Startování volebního procesu
    private fun startElectionProcess() {
        electionProcessJob?.cancel()
        electionProcessJob = coroutineScope.launch {
            // Čekáme na určitou dobu, než volby selžou
            delay((MAX_ELECTION_TIMEOUT + MAX_ELECTION_TIMEOUT * .2).toLong())
            if (state == NodeState.CANDIDATE) {
                // Pokud jsme stále kandidátem, znamená to, že volební proces selhal
                logCt("Uzel $id volby selhaly v termínu $currentTerm, opětovné zahájení voleb")
                state = NodeState.FOLLOWER
                votedFor = null
                resetElectionTimeout() // Opětovné nastavení election timeoutu
            }
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
                logCt("Uzel $id obdržel REQUEST od ${message.senderId}: ${message.content}")
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
                logCt("Uzel $id obdržel RESPONSE od ${message.senderId}: ${message.content}")
            }

            else -> {}
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

        if (votedFor == null || votedFor == message.candidateId) {
            votedFor = message.candidateId
            resetElectionTimeout()
            sendMessage(message.senderId, "Grant vote", MessageType.VOTE, currentTerm, id, voteGranted = true)
            logCt("Uzel $id hlasuje pro ${message.candidateId} v termínu ${message.term}")
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
            logCt("Uzel $id získal hlas od ${message.senderId}. Celkem hlasů: $voteCount")
            if (voteCount > allNodeIds(true).size / 2) {
                becomeLeader()
            }
        } else if (message.term > currentTerm) {
            // Pokud obdržíme vyšší termín, přejdeme do stavu FOLLOWER
            currentTerm = message.term
            state = NodeState.FOLLOWER
            votedFor = null
            electionProcessJob?.cancel()
            resetElectionTimeout()
        }
    }

    // Zpracování AppendEntries zprávy (Heartbeat)
    private fun handleAppendEntries(message: Message) {
        if (message.term < currentTerm) {
            return
        }

        resetElectionTimeout()

        if (message.term > currentTerm || state != NodeState.FOLLOWER) {
            currentTerm = message.term
            state = NodeState.FOLLOWER
            votedFor = null
            heartbeatJob?.cancel()
            logCt("Uzel $id se stává FOLLOWER v termínu $currentTerm po obdržení HeartBeatu od ${message.senderId}")
        } else {
            // Pokud jsme kandidát a obdržíme heartbeat s naším termínem, přejdeme do stavu FOLLOWER
            if (state == NodeState.CANDIDATE) {
                state = NodeState.FOLLOWER
                votedFor = null
                electionProcessJob?.cancel()
                logCt("Uzel $id se stává FOLLOWER v termínu $currentTerm po obdržení Heartbeatu od ${message.senderId}")
            }
        }
    }

    // Přechod do stavu Leader
    private fun becomeLeader() {
        if (state == NodeState.CANDIDATE) {
            state = NodeState.LEADER
            logCt("Uzel $id se stal LEADER v termínu $currentTerm")
            // Zrušit election timeout, protože již není potřeba
            electionTimeoutJob?.cancel()
            // Zrušit volbový proces, pokud je aktivní
            electionProcessJob?.cancel()
            // Zrušit předchozí heartbeat job, pokud existuje
            heartbeatJob?.cancel()
            // Start sending heartbeats
            heartbeatJob = sendHeartbeats()
        }
    }

    // Posílání heartbeat zpráv
    private fun sendHeartbeats(): Job {
        return coroutineScope.launch {
            while (state == NodeState.LEADER && this@Node.isActive) {
                allNodeIds().filter { it != id }.forEach { nodeId ->
                    sendMessage(nodeId as String, "Heartbeat", MessageType.HEARTBEAT, currentTerm, id)
                }
                delay(HEARTBEAT_INTERVAL) // Heartbeat interval
            }
        }
    }

    // Zpracování REQUEST_TOKEN zprávy pro vzájemné vyloučení
    private fun handleRequestToken(message: Message) {
        logCt("Uzel $id obdržel ${MessageType.REQUEST_TOKEN.name} od ${message.senderId}")
        if (hasToken && !isPerformingAlgorithm) {
            // Předat token
            hasToken = false
            sendToken(message.senderId)
        } else {
            // Přidat do fronty
            Network.requestQueue.add(message.senderId)
        }
    }

    // Zpracování TOKEN zprávy pro vzájemné vyloučení
    private fun handleToken(message: Message) {
        logCt("Node $id received TOKEN from ${message.senderId}")
        hasToken = true
        if (requestingCS) {
            tokenDeferred?.complete(Unit)
        } else {
            if (Network.requestQueue.isNotEmpty()) {
                sendToken(Network.requestQueue.poll())
            } else {
                sendToken(getNextNodeId())
            }
        }
    }

    // Odeslání tokenu
    private fun sendToken(receiverId: String) = coroutineScope.launch {
        hasToken = false
        isPerformingAlgorithm = false
        sendMessage(receiverId, "Token", MessageType.TOKEN)
    }

    // Vstup do kritické sekce
    fun enterCriticalSection() = coroutineScope.launch {
        if (requestingCS && hasToken) {
            logCt("Uzel $id vstupuje do kritické sekce.")
            while (isPerformingAlgorithm) {
                yield()
            }
            logCt("Uzel $id opouští kritickou sekci.")
            requestingCS = false
            // Předání tokenu dalšímu v frontě nebo náhodně
            if (Network.requestQueue.isNotEmpty()) {
                val nextNode = Network.requestQueue.poll()
                sendToken(nextNode)
            } else {
                sendToken(getNextNodeId())
            }
        } else if (hasToken && !isPerformingAlgorithm) {
            if (Network.requestQueue.isNotEmpty()) {
                sendToken(Network.requestQueue.poll())
            } else {
                sendToken(getNextNodeId())
            }
        }
    }

    // Požadavek na vstup do kritické sekce
    private fun requestCriticalSection() = coroutineScope.launch {
        if (!requestingCS) {
            requestingCS = true
            if (hasToken) {
                tokenDeferred?.complete(Unit)
            } else {
                sendMessage(getNextNodeId(), "Requesting token", MessageType.REQUEST_TOKEN)
            }
        }
    }

    // Funkce pro získání ID následujícího uzlu v pořadí
    private fun getNextNodeId(): String {
        val allIds = allNodeIds(true) as List<String>
        val currentIndex = allIds.indexOf(id)
        val nextIndex = (currentIndex + 1) % allIds.size
        return allIds[nextIndex]
    }

    // Metoda pro vlastní algoritmus, kterou může uzel periodicky vykonávat
    fun performAlgorithms() {
        isPerformingAlgorithm = true
        coroutineScope.launch {
            tokenDeferred = CompletableDeferred()
            requestCriticalSection()
            tokenDeferred?.await()
            if (hasToken) {
                for (n in 0 until 5) {
                    if (!this@Node.isActive) break
                    task.run()
                    delay(timeMillis = 1000)
                }
            }
            isPerformingAlgorithm = false
            requestingCS = false
            tokenDeferred = null
            if (Network.requestQueue.isNotEmpty())
                sendToken(Network.requestQueue.poll())
            else
                sendToken(getNextNodeId())

        }
    }

    private fun allNodeIds(filterActive: Boolean = false, idOnly: Boolean = true) =
        Network.nodes.values.filter { filterActive.not().or(it.isActive) }.map {
            if (idOnly)
                it.id
            else it
        }.toList()

    override fun toString(): String {
        return "Node(id='$id', isActive=$isActive, state=$state, term=$currentTerm, hasToken=$hasToken)"
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is Node) return false

        if (id != other.id) return false

        return true
    }

    override fun hashCode(): Int {
        return id.hashCode()
    }
}
