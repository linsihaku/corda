package net.corda.node.services.network

import net.corda.core.concurrent.CordaFuture
import net.corda.core.crypto.SecureHash
import net.corda.core.crypto.toStringShort
import net.corda.core.identity.AbstractParty
import net.corda.core.identity.CordaX500Name
import net.corda.core.identity.Party
import net.corda.core.identity.PartyAndCertificate
import net.corda.core.internal.*
import net.corda.core.internal.concurrent.map
import net.corda.core.internal.concurrent.openFuture
import net.corda.core.messaging.DataFeed
import net.corda.core.messaging.SingleMessageRecipient
import net.corda.core.node.NetworkParameters
import net.corda.core.node.NodeInfo
import net.corda.core.node.services.IdentityService
import net.corda.core.node.services.NetworkMapCache.MapChange
import net.corda.core.node.services.NotaryService
import net.corda.core.node.services.PartyInfo
import net.corda.core.schemas.NodeInfoSchemaV1
import net.corda.core.serialization.SerializationDefaults
import net.corda.core.serialization.SingletonSerializeAsToken
import net.corda.core.serialization.deserialize
import net.corda.core.serialization.serialize
import net.corda.core.utilities.NetworkHostAndPort
import net.corda.core.utilities.getOrThrow
import net.corda.core.utilities.loggerFor
import net.corda.core.utilities.toBase58String
import net.corda.node.services.api.NetworkCacheException
import net.corda.node.services.api.NetworkMapCacheInternal
import net.corda.node.services.api.NetworkMapCacheBaseInternal
import net.corda.node.services.config.NodeConfiguration
import net.corda.node.services.messaging.MessagingService
import net.corda.node.services.messaging.createMessage
import net.corda.node.services.messaging.sendRequest
import net.corda.node.services.network.NetworkMapService.FetchMapResponse
import net.corda.node.services.network.NetworkMapService.SubscribeResponse
import net.corda.node.utilities.*
import net.corda.node.utilities.AddOrRemove
import net.corda.node.utilities.bufferUntilDatabaseCommit
import net.corda.node.utilities.wrapWithDatabaseTransaction
import org.hibernate.Session
import rx.Observable
import rx.subjects.PublishSubject
import java.nio.file.Files
import java.security.PublicKey
import java.security.SignatureException
import java.util.*
import javax.annotation.concurrent.ThreadSafe
import kotlin.collections.HashMap

class NetworkMapCacheImpl(networkMapCacheBase: NetworkMapCacheBaseInternal, private val identityService: IdentityService) : NetworkMapCacheBaseInternal by networkMapCacheBase, NetworkMapCacheInternal {
    init {
        networkMapCacheBase.allNodes.forEach { it.legalIdentitiesAndCerts.forEach { identityService.verifyAndRegisterIdentity(it) } }
        networkMapCacheBase.changed.subscribe { mapChange ->
            // TODO how should we handle network map removal
            if (mapChange is MapChange.Added) {
                mapChange.node.legalIdentitiesAndCerts.forEach {
                    identityService.verifyAndRegisterIdentity(it)
                }
            }
        }
    }

    override fun getNodeByLegalIdentity(party: AbstractParty): NodeInfo? {
        val wellKnownParty = identityService.wellKnownPartyFromAnonymous(party)
        return wellKnownParty?.let {
            getNodesByLegalIdentityKey(it.owningKey).firstOrNull()
        }
    }
}

/**
 * Extremely simple in-memory cache of the network map.
 */
@ThreadSafe
open class PersistentNetworkMapCache(private val database: CordaPersistence,
                                     val configuration: NodeConfiguration,
                                     private var _networkParameters: NetworkParameters?) : SingletonSerializeAsToken(), NetworkMapCacheBaseInternal {
    companion object {
        val logger = loggerFor<PersistentNetworkMapCache>()
    }

    private var registeredForPush = false

    // TODO Cleanup registered and party nodes
    protected val registeredNodes: MutableMap<PublicKey, NodeInfo> = Collections.synchronizedMap(HashMap())
    protected val partyNodes: MutableList<NodeInfo> get() = registeredNodes.map { it.value }.toMutableList()
    private val _changed = PublishSubject.create<MapChange>()
    // We use assignment here so that multiple subscribers share the same wrapped Observable.
    override val changed: Observable<MapChange> = _changed.wrapWithDatabaseTransaction()
    private val changePublisher: rx.Observer<MapChange> get() = _changed.bufferUntilDatabaseCommit()

    private val _registrationFuture = openFuture<Void?>()
    override val nodeReady: CordaFuture<Void?> get() = _registrationFuture
    private var _loadDBSuccess: Boolean = false
    override val loadDBSuccess get() = _loadDBSuccess && networkParameters != null
    override val networkParameters: NetworkParameters? get() = _networkParameters

    // Hash of parameters currently used by the node.
    private var currentParametersHash: SecureHash? = null

    override val notaryIdentities: List<Party>
        get() {
            return networkParameters?.notaryIdentities ?: emptyList()
        }

    private val validatingNotaryIdentities: List<Party>
        get() {
            return networkParameters?.validatingNotaryIdentities ?: emptyList()
        }

    private val nodeInfoSerializer = NodeInfoWatcher(configuration.baseDirectory,
            configuration.additionalNodeInfoPollingFrequencyMsec)

    init {
        loadFromFiles()
        database.transaction { loadFromDB(session) }
    }

    private fun checkVersion() {
        _networkParameters?.let { check(configuration.minimumPlatformVersion <= it.minimumPlatformVersion) {
            "Node's minimumPlatformVersion is lower than network platform version"
        } }
    }

    private fun loadFromFiles() {
        logger.info("Loading network map from files..")
        nodeInfoSerializer.nodeInfoUpdates().subscribe { node -> addNode(node) }
    }

    override fun isValidatingNotary(party: Party): Boolean = isNotary(party) && party in validatingNotaryIdentities

    override fun getPartyInfo(party: Party): PartyInfo? {
        val nodes = database.transaction { queryByIdentityKey(session, party.owningKey) }
        if (nodes.size == 1 && nodes[0].isLegalIdentity(party)) {
            return PartyInfo.SingleNode(party, nodes[0].addresses)
        }
        for (node in nodes) {
            for (identity in node.legalIdentities) {
                if (identity == party) {
                    return PartyInfo.DistributedNode(party)
                }
            }
        }
        return null
    }

    override fun getNodeByLegalName(name: CordaX500Name): NodeInfo? = getNodesByLegalName(name).firstOrNull()
    override fun getNodesByLegalName(name: CordaX500Name): List<NodeInfo> = database.transaction { queryByLegalName(session, name) }
    override fun getNodesByLegalIdentityKey(identityKey: PublicKey): List<NodeInfo> =
            database.transaction { queryByIdentityKey(session, identityKey) }

    override fun getNodeByAddress(address: NetworkHostAndPort): NodeInfo? = database.transaction { queryByAddress(session, address) }

    override fun getPeerCertificateByLegalName(name: CordaX500Name): PartyAndCertificate? = database.transaction { queryIdentityByLegalName(session, name) }

    override fun track(): DataFeed<List<NodeInfo>, MapChange> {
        synchronized(_changed) {
            return DataFeed(partyNodes, _changed.bufferUntilSubscribed().wrapWithDatabaseTransaction())
        }
    }

    override fun addMapService(network: MessagingService, networkMapAddress: SingleMessageRecipient, subscribe: Boolean,
                               ifChangedSinceVer: Int?): CordaFuture<Unit> {
        if (subscribe && !registeredForPush) {
            // Add handler to the network, for updates received from the remote network map service.
            network.addMessageHandler(NetworkMapService.PUSH_TOPIC) { message, _ ->
                try {
                    val req = message.data.deserialize<NetworkMapService.Update>()
                    val ackMessage = network.createMessage(NetworkMapService.PUSH_ACK_TOPIC,
                            data = NetworkMapService.UpdateAcknowledge(req.mapVersion, network.myAddress).serialize().bytes)
                    network.send(ackMessage, req.replyTo)
                    processUpdatePush(req)
                } catch (e: NodeMapException) {
                    logger.warn("Failure during node map update due to bad update: ${e.javaClass.name}")
                } catch (e: Exception) {
                    logger.error("Exception processing update from network map service", e)
                }
            }
            registeredForPush = true
        }

        // Fetch the network map and register for updates at the same time
        val req = NetworkMapService.FetchMapRequest(subscribe, ifChangedSinceVer, network.myAddress)
        logger.info("Sending fetch request to network map")
        val future = network.sendRequest<FetchMapResponse>(NetworkMapService.FETCH_TOPIC, req, networkMapAddress).map { (networkMap, nodes) ->
            logger.info("Got parameters with hash: ${networkMap.parametersHash}")
            processParametersHash(network, networkMapAddress, networkMap.parametersHash)
            // We may not receive any nodes back, if the map hasn't changed since the version specified
            nodes?.forEach { processRegistration(it) } // TODO Process hashes, Patrick's PR.
            Unit
        }
        _registrationFuture.captureLater(future.map { null })

        return future
    }

    private fun processParametersHash(network: MessagingService, networkMapAddress: SingleMessageRecipient, parametersHash: SecureHash?) {
        logger.info("Processing parameters hash")
        if (networkParameters == null) {
            val request = NetworkMapService.FetchParametersRequest(network.myAddress)
            logger.info("Sending request to fetch network parameters")
            network.sendRequest<NetworkMapService.FetchParametersResponse>(NetworkMapService.PARAMETERS_TOPIC, request, networkMapAddress).map {
                (signedParameters) -> _networkParameters = signedParameters.verified()
                logger.info("Got NetworkParameters: $_networkParameters")
                // TODO Validate certificate chain
            }
            checkVersion()
        } else {
            if (currentParametersHash != parametersHash && currentParametersHash != null) {
                throw IllegalStateException("Node uses different NetworkParameters than the ones advertised by the network map")
            }
        }
    }

    override fun addNode(node: NodeInfo) {
        logger.info("Adding node with info: $node")
        synchronized(_changed) {
            registeredNodes[node.legalIdentities.first().owningKey]?.let {
                if (it.serial > node.serial) {
                    logger.info("Discarding older nodeInfo for ${node.legalIdentities.first().name}")
                    return
                }
            }
            val previousNode = registeredNodes.put(node.legalIdentities.first().owningKey, node) // TODO hack... we left the first one as special one
            if (previousNode == null) {
                logger.info("No previous node found")
                database.transaction {
                    updateInfoDB(node)
                    changePublisher.onNext(MapChange.Added(node))
                }
            } else if (previousNode != node) {
                logger.info("Previous node was found as: $previousNode")
                database.transaction {
                    updateInfoDB(node)
                    changePublisher.onNext(MapChange.Modified(node, previousNode))
                }
            } else {
                logger.info("Previous node was identical to incoming one - doing nothing")
            }
        }
        logger.info("Done adding node with info: $node")
    }

    override fun removeNode(node: NodeInfo) {
        logger.info("Removing node with info: $node")
        synchronized(_changed) {
            registeredNodes.remove(node.legalIdentities.first().owningKey)
            database.transaction {
                removeInfoDB(session, node)
                changePublisher.onNext(MapChange.Removed(node))
            }
        }
        logger.info("Done removing node with info: $node")
    }

    /**
     * Unsubscribes from updates from the given map service.
     * @param mapParty the network map service party to listen to updates from.
     */
    override fun deregisterForUpdates(network: MessagingService, mapParty: Party): CordaFuture<Unit> {
        // Fetch the network map and register for updates at the same time
        val req = NetworkMapService.SubscribeRequest(false, network.myAddress)
        // `network.getAddressOfParty(partyInfo)` is a work-around for MockNetwork and InMemoryMessaging to get rid of SingleMessageRecipient in NodeInfo.
        val address = getPartyInfo(mapParty)?.let { network.getAddressOfParty(it) } ?:
                throw IllegalArgumentException("Can't deregister for updates, don't know the party: $mapParty")
        val future = network.sendRequest<SubscribeResponse>(NetworkMapService.SUBSCRIPTION_TOPIC, req, address).map {
            if (it.confirmed) Unit else throw NetworkCacheException.DeregistrationFailed()
        }
        _registrationFuture.captureLater(future.map { null })
        return future
    }

    fun processUpdatePush(req: NetworkMapService.Update) {
        try {
            val reg = req.wireReg.verified()
            processRegistration(reg)
        } catch (e: SignatureException) {
            throw NodeMapException.InvalidSignature()
        }
    }

    override val allNodes: List<NodeInfo>
        get() = database.transaction {
            getAllInfos(session).map { it.toNodeInfo() }
        }

    private fun processRegistration(reg: NodeRegistration) {
        when (reg.type) {
            AddOrRemove.ADD -> addNode(reg.node)
            AddOrRemove.REMOVE -> removeNode(reg.node)
        }
    }

    @VisibleForTesting
    override fun runWithoutMapService() {
        _registrationFuture.set(null)
    }

    private fun getAllInfos(session: Session): List<NodeInfoSchemaV1.PersistentNodeInfo> {
        val criteria = session.criteriaBuilder.createQuery(NodeInfoSchemaV1.PersistentNodeInfo::class.java)
        criteria.select(criteria.from(NodeInfoSchemaV1.PersistentNodeInfo::class.java))
        return session.createQuery(criteria).resultList
    }

    /**
     * Load NetworkMap data from the database if present. Node can start without having NetworkMapService configured.
     */
    private fun loadFromDB(session: Session) {
        logger.info("Loading network map from database...")
        val result = getAllInfos(session)
        for (nodeInfo in result) {
            try {
                logger.info("Loaded node info: $nodeInfo")
                val node = nodeInfo.toNodeInfo()
                addNode(node)
                _loadDBSuccess = true // This is used in AbstractNode to indicate that node is ready.
            } catch (e: Exception) {
                logger.warn("Exception parsing network map from the database.", e)
            }
        }
        if (loadDBSuccess) {
            _registrationFuture.set(null) // Useful only if we don't have NetworkMapService configured so StateMachineManager can start.
        }
    }

    private fun updateInfoDB(nodeInfo: NodeInfo) {
        // TODO Temporary workaround to force isolated transaction (otherwise it causes race conditions when processing
        //  network map registration on network map node)
        database.dataSource.connection.use {
            val session = database.entityManagerFactory.withOptions().connection(it.apply {
                transactionIsolation = 1
            }).openSession()
            session.use {
                val tx = session.beginTransaction()
                // TODO For now the main legal identity is left in NodeInfo, this should be set comparision/come up with index for NodeInfo?
                val info = findByIdentityKey(session, nodeInfo.legalIdentitiesAndCerts.first().owningKey)
                val nodeInfoEntry = generateMappedObject(nodeInfo)
                if (info.isNotEmpty()) {
                    nodeInfoEntry.id = info[0].id
                }
                session.merge(nodeInfoEntry)
                tx.commit()
            }
        }
    }

    private fun removeInfoDB(session: Session, nodeInfo: NodeInfo) {
        val info = findByIdentityKey(session, nodeInfo.legalIdentitiesAndCerts.first().owningKey).single()
        session.remove(info)
    }

    private fun findByIdentityKey(session: Session, identityKey: PublicKey): List<NodeInfoSchemaV1.PersistentNodeInfo> {
        val query = session.createQuery(
                "SELECT n FROM ${NodeInfoSchemaV1.PersistentNodeInfo::class.java.name} n JOIN n.legalIdentitiesAndCerts l WHERE l.owningKeyHash = :owningKeyHash",
                NodeInfoSchemaV1.PersistentNodeInfo::class.java)
        query.setParameter("owningKeyHash", identityKey.toStringShort())
        return query.resultList
    }

    private fun queryByIdentityKey(session: Session, identityKey: PublicKey): List<NodeInfo> {
        val result = findByIdentityKey(session, identityKey)
        return result.map { it.toNodeInfo() }
    }

    private fun queryIdentityByLegalName(session: Session, name: CordaX500Name): PartyAndCertificate? {
        val query = session.createQuery(
                // We do the JOIN here to restrict results to those present in the network map
                "SELECT DISTINCT l FROM ${NodeInfoSchemaV1.PersistentNodeInfo::class.java.name} n JOIN n.legalIdentitiesAndCerts l WHERE l.name = :name",
                NodeInfoSchemaV1.DBPartyAndCertificate::class.java)
        query.setParameter("name", name.toString())
        val candidates = query.resultList.map { it.toLegalIdentityAndCert() }
        // The map is restricted to holding a single identity for any X.500 name, so firstOrNull() is correct here.
        return candidates.firstOrNull()
    }

    private fun queryByLegalName(session: Session, name: CordaX500Name): List<NodeInfo> {
        val query = session.createQuery(
                "SELECT n FROM ${NodeInfoSchemaV1.PersistentNodeInfo::class.java.name} n JOIN n.legalIdentitiesAndCerts l WHERE l.name = :name",
                NodeInfoSchemaV1.PersistentNodeInfo::class.java)
        query.setParameter("name", name.toString())
        val result = query.resultList
        return result.map { it.toNodeInfo() }
    }

    private fun queryByAddress(session: Session, hostAndPort: NetworkHostAndPort): NodeInfo? {
        val query = session.createQuery(
                "SELECT n FROM ${NodeInfoSchemaV1.PersistentNodeInfo::class.java.name} n JOIN n.addresses a WHERE a.pk.host = :host AND a.pk.port = :port",
                NodeInfoSchemaV1.PersistentNodeInfo::class.java)
        query.setParameter("host", hostAndPort.host)
        query.setParameter("port", hostAndPort.port)
        val result = query.resultList
        return if (result.isEmpty()) null
        else result.map { it.toNodeInfo() }.singleOrNull() ?: throw IllegalStateException("More than one node with the same host and port")
    }

    /** Object Relational Mapping support. */
    private fun generateMappedObject(nodeInfo: NodeInfo): NodeInfoSchemaV1.PersistentNodeInfo {
        return NodeInfoSchemaV1.PersistentNodeInfo(
                id = 0,
                addresses = nodeInfo.addresses.map { NodeInfoSchemaV1.DBHostAndPort.fromHostAndPort(it) },
                legalIdentitiesAndCerts = nodeInfo.legalIdentitiesAndCerts.mapIndexed { idx, elem ->
                    NodeInfoSchemaV1.DBPartyAndCertificate(elem, isMain = idx == 0)
                },
                platformVersion = nodeInfo.platformVersion,
                serial = nodeInfo.serial
        )
    }

    override fun clearNetworkMapCache() {
        database.transaction {
            val result = getAllInfos(session)
            for (nodeInfo in result) session.remove(nodeInfo)
        }
    }
}
