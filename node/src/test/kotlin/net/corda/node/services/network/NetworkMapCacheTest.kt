package net.corda.node.services.network

import net.corda.core.node.services.NetworkMapCache
import net.corda.core.utilities.getOrThrow
import net.corda.testing.ALICE_NAME
import net.corda.testing.BOB_NAME
import net.corda.testing.chooseIdentity
import net.corda.testing.node.MockNetwork
import org.assertj.core.api.Assertions.assertThat
import org.junit.After
import org.junit.Before
import org.junit.Test
import java.math.BigInteger
import kotlin.test.assertEquals

class NetworkMapCacheTest {
    lateinit var mockNet: MockNetwork

    @Before
    fun setUp() {
        mockNet = MockNetwork()
    }

    @After
    fun teardown() {
        mockNet.stopNodes()
    }

    @Test
    fun registerWithNetwork() {
        mockNet.createNotaryNode()
        val aliceNode = mockNet.createPartyNode(ALICE_NAME)
        val future = aliceNode.services.networkMapCache.addMapService(aliceNode.network, mockNet.networkMapNode.network.myAddress, false, null)
        mockNet.runNetwork()
        future.getOrThrow()
    }

    @Test
    fun `key collision`() {
        val entropy = BigInteger.valueOf(24012017L)
        val aliceNode = mockNet.createNode(nodeFactory = MockNetwork.DefaultFactory, legalName = ALICE_NAME, entropyRoot = entropy)
        val alice = aliceNode.services.myInfo.chooseIdentity(ALICE_NAME)
        mockNet.runNetwork()

        // Node A currently knows only about itself, so this returns node A
        assertEquals(aliceNode.services.networkMapCache.getNodesByLegalIdentityKey(alice.owningKey).singleOrNull(), aliceNode.info)
        val bobNode = mockNet.createNode(nodeFactory = MockNetwork.DefaultFactory, legalName = BOB_NAME, entropyRoot = entropy)
        val bob = bobNode.services.myInfo.chooseIdentity(BOB_NAME)
        assertEquals(alice, bob)

        aliceNode.services.networkMapCache.addNode(bobNode.info)
        // The details of node B write over those for node A
        assertEquals(aliceNode.services.networkMapCache.getNodesByLegalIdentityKey(alice.owningKey).singleOrNull(), bobNode.info)
    }

    @Test
    fun `getNodeByLegalIdentity`() {
        val notaryNode = mockNet.createNotaryNode()
        val aliceNode = mockNet.createPartyNode(ALICE_NAME)
        val alice = aliceNode.services.myInfo.chooseIdentity(ALICE_NAME)
        val notaryCache: NetworkMapCache = notaryNode.services.networkMapCache
        val expected = aliceNode.info

        mockNet.runNetwork()
        val actual = notaryNode.database.transaction { notaryCache.getNodeByLegalIdentity(alice) }
        assertEquals(expected, actual)

        // TODO: Should have a test case with anonymous lookup
    }

    @Test
    fun `getPeerByLegalName`() {
        val notaryNode = mockNet.createNotaryNode()
        val aliceNode = mockNet.createPartyNode(ALICE_NAME)
        val notaryCache: NetworkMapCache = notaryNode.services.networkMapCache
        val expected = aliceNode.info.legalIdentities.single()

        mockNet.runNetwork()
        val actual = notaryNode.database.transaction { notaryCache.getPeerByLegalName(ALICE_NAME) }
        assertEquals(expected, actual)
    }

    @Test
    fun `remove node from cache`() {
        val notaryNode = mockNet.createNotaryNode()
        val aliceNode = mockNet.createPartyNode(ALICE_NAME)
        val notaryLegalIdentity = notaryNode.info.chooseIdentity()
        val alice = aliceNode.services.myInfo.chooseIdentity(ALICE_NAME)
        val notaryCache = notaryNode.services.networkMapCache as PersistentNetworkMapCache
        mockNet.runNetwork()
        notaryNode.database.transaction {
            assertThat(notaryCache.getNodeByLegalIdentity(alice) != null)
            notaryCache.removeNode(aliceNode.info)
            assertThat(notaryCache.getNodeByLegalIdentity(alice) == null)
            assertThat(notaryCache.getNodeByLegalIdentity(notaryLegalIdentity) != null)
            assertThat(notaryCache.getNodeByLegalName(ALICE_NAME) == null)
        }
    }
}
