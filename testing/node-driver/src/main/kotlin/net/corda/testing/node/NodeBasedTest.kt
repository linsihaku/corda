package net.corda.testing.node

import net.corda.core.concurrent.CordaFuture
import net.corda.core.crypto.Crypto
import net.corda.core.crypto.DigitalSignature
import net.corda.core.crypto.SignedData
import net.corda.core.identity.CordaX500Name
import net.corda.core.identity.Party
import net.corda.core.internal.concurrent.*
import net.corda.core.internal.copyTo
import net.corda.core.internal.createDirectories
import net.corda.core.internal.div
import net.corda.core.node.NetworkParameters
import net.corda.core.serialization.serialize
import net.corda.core.utilities.NetworkHostAndPort
import net.corda.core.utilities.getOrThrow
import net.corda.core.utilities.loggerFor
import net.corda.node.internal.Node
import net.corda.node.internal.StartedNode
import net.corda.node.internal.cordapp.CordappLoader
import net.corda.node.services.config.*
import net.corda.node.utilities.ServiceIdentityGenerator
import net.corda.node.utilities.testParameters
import net.corda.nodeapi.User
import net.corda.nodeapi.config.toConfig
import net.corda.testing.*
import net.corda.testing.driver.addressMustNotBeBoundFuture
import net.corda.testing.node.MockServices.Companion.MOCK_VERSION_INFO
import org.apache.logging.log4j.Level
import org.junit.After
import org.junit.Before
import org.junit.Rule
import org.junit.rules.TemporaryFolder
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.util.concurrent.Executors
import kotlin.concurrent.thread

/**
 * Extend this class if you need to run nodes in a test. You could use the driver DSL but it's extremely slow for testing
 * purposes. Use the driver if you need to run the nodes in separate processes otherwise this class will suffice.
 */
// TODO Some of the logic here duplicates what's in the driver
abstract class NodeBasedTest(
        private val cordappPackages: List<String> = emptyList(),
        private val notaries: List<NotarySpec> = listOf(NotarySpec(DUMMY_NOTARY.name))
) : TestDependencyInjectionBase() {
    companion object {
        private val WHITESPACE = "\\s++".toRegex()
        val logger = loggerFor<NodeBasedTest>()
    }

    @Rule
    @JvmField
    val tempFolder: TemporaryFolder = TemporaryFolder()

    private val nodes = mutableListOf<StartedNode<Node>>()
    private var _networkMapNode: StartedNode<Node>? = null

    // TODO Get rid of this
    val networkMapNode: StartedNode<Node> get() = _networkMapNode ?: startNetworkMapNode()
    private lateinit var notaryIdentities: List<Party>
    private val validatingNotaryIdentities = mutableListOf<Party>()
    private lateinit var networkParameters: NetworkParameters

    @Before
    fun setup() {
        notaryIdentities = ServiceIdentityGenerator.generateNotaryIdentities(notaries, tempFolder.root.toPath())
        val validatingNotaries = notaries.filter { it.validating }.map { it.notaryName }
        validatingNotaryIdentities.addAll(notaryIdentities.filter { it.name in validatingNotaries })
        networkParameters = testParameters(notaryIdentities, validatingNotaryIdentities)
    }

    init {
        System.setProperty("consoleLogLevel", Level.DEBUG.name().toLowerCase())
    }

    private fun saveNetworkParameters(baseDirectory: Path) {
        val serializedParams = networkParameters.serialize()
        val digitalSignature = DigitalSignature.WithKey(DUMMY_MAP_KEY.public, Crypto.doSign(DUMMY_MAP_KEY.private, serializedParams.bytes))
        val signedParams = SignedData(serializedParams, digitalSignature)
        val paramsDir = baseDirectory / "network_parameters"
        paramsDir.createDirectories()
        signedParams.serialize().open().copyTo(paramsDir / "network-parameters-1", StandardCopyOption.REPLACE_EXISTING)
    }

    /**
     * Stops the network map node and all the nodes started by [startNode]. This is called automatically after each test
     * but can also be called manually within a test.
     */
    @After
    fun stopAllNodes() {
        val shutdownExecutor = Executors.newScheduledThreadPool(nodes.size)
        nodes.map { shutdownExecutor.fork(it::dispose) }.transpose().getOrThrow()
        // Wait until ports are released
        val portNotBoundChecks = nodes.flatMap {
            listOf(
                    it.internals.configuration.p2pAddress.let { addressMustNotBeBoundFuture(shutdownExecutor, it) },
                    it.internals.configuration.rpcAddress?.let { addressMustNotBeBoundFuture(shutdownExecutor, it) }
            )
        }.filterNotNull()
        nodes.clear()
        _networkMapNode = null
        portNotBoundChecks.transpose().getOrThrow()
    }

    /**
     *  Clear network map data from nodes' databases.
     */
    fun clearAllNodeInfoDb() {
        nodes.forEach { it.services.networkMapCache.clearNetworkMapCache() }
    }

    /**
     * You can use this method to start the network map node in a more customised manner. Otherwise it
     * will automatically be started with the default parameters.
     */
    fun startNetworkMapNode(legalName: CordaX500Name = DUMMY_MAP.name,
                            platformVersion: Int = 1,
                            rpcUsers: List<User> = emptyList(),
                            configOverrides: Map<String, Any> = emptyMap()): StartedNode<Node> {
        check(_networkMapNode == null || _networkMapNode!!.info.legalIdentitiesAndCerts.first().name == legalName)
        return startNodeInternal(legalName, platformVersion, rpcUsers, configOverrides).apply {
            _networkMapNode = this
        }
    }

    @JvmOverloads
    fun startNode(legalName: CordaX500Name,
                  platformVersion: Int = 1,
                  rpcUsers: List<User> = emptyList(),
                  configOverrides: Map<String, Any> = emptyMap(),
                  noNetworkMap: Boolean = false,
                  waitForConnection: Boolean = true): CordaFuture<StartedNode<Node>> {
        val networkMapConf = if (noNetworkMap) {
            // Nonexistent network map service address.
            mapOf(
                    "networkMapService" to mapOf(
                            "address" to "localhost:10000",
                            "legalName" to networkMapNode.info.legalIdentitiesAndCerts.first().name.toString()
                    )
            )
        } else {
            mapOf(
                    "networkMapService" to mapOf(
                            "address" to networkMapNode.internals.configuration.p2pAddress.toString(),
                            "legalName" to networkMapNode.info.legalIdentitiesAndCerts.first().name.toString()
                    )
            )
        }
        val node = startNodeInternal(
                legalName,
                platformVersion,
                rpcUsers,
                networkMapConf + configOverrides,
                noNetworkMap)
        return if (waitForConnection) node.internals.nodeReadyFuture.map { node } else doneFuture(node)
    }

    private fun startNotaryNode(name: CordaX500Name,
                        rpcUsers: List<User> = emptyList(),
                        validating: Boolean = true): CordaFuture<StartedNode<Node>> {
        if (name !in notaryIdentities.map { it.name }) logger.warn("Starting notary not provided in network parameters")
        return startNode(name, rpcUsers = rpcUsers, configOverrides = mapOf("notary" to mapOf("validating" to validating)))
    }

    private fun startNotaryCluster(notaryName: CordaX500Name, clusterSize: Int): CordaFuture<List<StartedNode<Node>>> {
        if (notaryName !in notaryIdentities.map { it.name }) logger.warn("Starting notary not provided in network parameters")
        fun notaryConfig(nodeAddress: NetworkHostAndPort, clusterAddress: NetworkHostAndPort? = null): Map<String, Any> {
            val clusterAddresses = if (clusterAddress != null) listOf(clusterAddress) else emptyList()
            val config = NotaryConfig(validating = true, raft = RaftConfig(nodeAddress = nodeAddress, clusterAddresses = clusterAddresses))
            return mapOf("notary" to config.toConfig().root().unwrapped())
        }

        val nodeAddresses = getFreeLocalPorts("localhost", clusterSize)

        val masterNodeFuture = startNode(
                CordaX500Name(organisation = "${notaryName.organisation}-0", locality = notaryName.locality, country = notaryName.country),
                configOverrides = notaryConfig(nodeAddresses[0]) + mapOf(
                        "database" to mapOf(
                                "serverNameTablePrefix" to if (clusterSize > 1) "${notaryName.organisation}0".replace(Regex("[^0-9A-Za-z]+"), "") else ""
                        )
                )
        )

        val remainingNodesFutures = (1 until clusterSize).map {
            startNode(
                    CordaX500Name(organisation = "${notaryName.organisation}-$it", locality = notaryName.locality, country = notaryName.country),
                    configOverrides = notaryConfig(nodeAddresses[it], nodeAddresses[0]) + mapOf(
                            "database" to mapOf(
                                    "serverNameTablePrefix" to "${notaryName.organisation}$it".replace(Regex("[^0-9A-Za-z]+"), "")
                            )
                    )
            )
        }

        return remainingNodesFutures.transpose().flatMap { remainingNodes ->
            masterNodeFuture.map { masterNode -> listOf(masterNode) + remainingNodes }
        }
    }

    protected fun baseDirectory(legalName: CordaX500Name) = tempFolder.root.toPath() / legalName.organisation.replace(WHITESPACE, "")

    private fun startNodeInternal(legalName: CordaX500Name,
                                  platformVersion: Int,
                                  rpcUsers: List<User>,
                                  configOverrides: Map<String, Any>,
                                  noNetworkMap: Boolean = false): StartedNode<Node> {
        val baseDirectory = baseDirectory(legalName).createDirectories()
        saveNetworkParameters(baseDirectory)
        val localPort = getFreeLocalPorts("localhost", 2)
        val p2pAddress = configOverrides["p2pAddress"] ?: localPort[0].toString()
        val config = ConfigHelper.loadConfig(
                baseDirectory = baseDirectory,
                allowMissingConfig = true,
                configOverrides = configOf(
                        "myLegalName" to legalName.toString(),
                        "p2pAddress" to p2pAddress,
                        "rpcAddress" to localPort[1].toString(),
                        "rpcUsers" to rpcUsers.map { it.toMap() },
                        "noNetworkMap" to noNetworkMap
                ) + configOverrides
        )

        val parsedConfig = config.parseAsNodeConfiguration()
        val node = Node(
                parsedConfig,
                MOCK_VERSION_INFO.copy(platformVersion = platformVersion),
                initialiseSerialization = false,
                cordappLoader = CordappLoader.createDefaultWithTestPackages(parsedConfig, cordappPackages)).start()
        nodes += node
        thread(name = legalName.organisation) {
            node.internals.run()
        }
        return node
    }
}
