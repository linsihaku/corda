package net.corda.core.node

import net.corda.core.identity.Party
import net.corda.core.serialization.CordaSerializable
import java.time.Instant

/**
 * TODO
 */
@CordaSerializable
data class NetworkParameters(
        // TODO Add networkOperator CertPath?
        val minimumPlatformVersion: Int,
        val lastChanged: Instant,
        val eventHorizon: Instant,
        val maxMessageSize: Int, //In bytes. Now in AbstractNetworkMapService we have defined: MAX_SIZE_REGISTRATION_REQUEST_BYTES = 40000
        val maxTransactionSize: Int, // In bytes. We don't have it defined yet?
        val epoch: Int, // Network parameters are versioned by a single incrementing integer value, called an epoch.
        val notaryIdentities: List<Party>,
        // TODO I am not entirely convinced to this solution, as we may want in the future have some more information on notaries. Tags would be better.
        val validatingNotaryIdentities: List<Party> = emptyList()
)
