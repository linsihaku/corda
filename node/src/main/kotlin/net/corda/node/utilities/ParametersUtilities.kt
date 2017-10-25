package net.corda.node.utilities

import net.corda.core.identity.CordaX500Name
import net.corda.core.identity.Party
import net.corda.core.node.NetworkParameters

sealed class NotaryNode {
    abstract val legalNotaryName: CordaX500Name
    abstract val validating: Boolean
    class Single(override val legalNotaryName: CordaX500Name, override val validating: Boolean = true) : NotaryNode()
    class Cluster(override val legalNotaryName: CordaX500Name,
                  val clusterSize: Int,
                  override val validating: Boolean = true,
                  val bft: Boolean = false,
                  val raft: Boolean = false,
                  // TODO This is only used for BFT
                  val exposeRaces: Boolean = false) : NotaryNode()
}

fun testParameters(notaries: List<Party>, validatingNotaries: List<Party>): NetworkParameters {
    val testClock = TestClock()
    return NetworkParameters(
            minimumPlatformVersion = 1,
            lastChanged = testClock.instant(),
            eventHorizon = testClock.instant(),
            maxMessageSize = 40000,
            maxTransactionSize = 40000,
            epoch = 1,
            notaryIdentities = notaries,
            validatingNotaryIdentities = validatingNotaries
    )
}