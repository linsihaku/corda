package net.corda.testing.node

import net.corda.core.identity.CordaX500Name
import net.corda.node.services.config.VerifierType
import net.corda.nodeapi.User

data class NotarySpec(
        val name: CordaX500Name,
        val validating: Boolean = true,
        val rpcUsers: List<User> = emptyList(),
        val verifierType: VerifierType = VerifierType.InMemory,
        val cluster: ClusterSpec? = null
) {
    init {
        if (cluster is ClusterSpec.BFTSMaRt) {
            require(!validating) { "Validating BFT-SMaRt notary not supported" }
        }
    }
}

sealed class ClusterSpec {
    abstract val clusterSize: Int

    data class Raft(override val clusterSize: Int) : ClusterSpec() {
        init { require(clusterSize > 0) }
    }
    data class BFTSMaRt(override val clusterSize: Int, val exposeRaces: Boolean = false) : ClusterSpec() {
        init { require(clusterSize > 0) }
    }
}