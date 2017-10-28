package net.corda.node.services

import net.corda.core.contracts.AlwaysAcceptAttachmentConstraint
import net.corda.core.contracts.ContractState
import net.corda.core.contracts.StateRef
import net.corda.core.crypto.CompositeKey
import net.corda.core.flows.NotaryError
import net.corda.core.flows.NotaryException
import net.corda.core.flows.NotaryFlow
import net.corda.core.identity.CordaX500Name
import net.corda.core.identity.Party
import net.corda.core.transactions.SignedTransaction
import net.corda.core.transactions.TransactionBuilder
import net.corda.core.utilities.Try
import net.corda.core.utilities.getOrThrow
import net.corda.node.internal.StartedNode
import net.corda.node.services.transactions.minClusterSize
import net.corda.node.services.transactions.minCorrectReplicas
import net.corda.testing.chooseIdentity
import net.corda.testing.contracts.DummyContract
import net.corda.testing.driver.NodeHandle
import net.corda.testing.driver.driver
import net.corda.testing.dummyCommand
import net.corda.testing.node.ClusterSpec
import net.corda.testing.node.NotarySpec
import org.junit.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class BFTNotaryServiceTests {
    /** Failure mode is the redundant replica gets stuck in startup, so we can't dispose it cleanly at the end. */
    @Test
    fun `all replicas start even if there is a new consensus during startup`() {
        driver(startNodesInProcess = true, notarySpecs = listOf(bftNotarySpec(minClusterSize(1), exposeRaces = true))) {
            val node = startNode().getOrThrow() as NodeHandle.InProcess
            notaries[0].nodeHandles.getOrThrow()
            val notaryIdentity = notaries[0].identity
            val f = node.node.run {
                val trivialTx = signInitialTransaction(notaryIdentity) {
                    addOutputState(DummyContract.SingleOwnerState(owner = info.chooseIdentity()), DummyContract.PROGRAM_ID, AlwaysAcceptAttachmentConstraint)
                }
                // Create a new consensus while the redundant replica is sleeping:
                services.startFlow(NotaryFlow.Client(trivialTx)).resultFuture
            }
            f.getOrThrow()
        }
    }

    @Test
    fun `detect double spend 1 faulty`() {
        detectDoubleSpend(1)
    }

    @Test
    fun `detect double spend 2 faulty`() {
        detectDoubleSpend(2)
    }

    private fun detectDoubleSpend(faultyReplicas: Int) {
        val clusterSize = minClusterSize(faultyReplicas)
        driver(startNodesInProcess = true, notarySpecs = listOf(bftNotarySpec(clusterSize))) {
            val node = startNode().getOrThrow() as NodeHandle.InProcess
            notaries[0].nodeHandles.getOrThrow()
            val notaryIdentity = notaries[0].identity
            node.node.run {
                val issueTx = signInitialTransaction(notaryIdentity) {
                    addOutputState(DummyContract.SingleOwnerState(owner = info.chooseIdentity()), DummyContract.PROGRAM_ID, AlwaysAcceptAttachmentConstraint)
                }
                database.transaction {
                    services.recordTransactions(issueTx)
                }
                val spendTxs = (1..10).map {
                    signInitialTransaction(notaryIdentity) {
                        addInputState(issueTx.tx.outRef<ContractState>(0))
                    }
                }
                assertEquals(spendTxs.size, spendTxs.map { it.id }.distinct().size)
                val flows = spendTxs.map { NotaryFlow.Client(it) }
                val stateMachines = flows.map { services.startFlow(it) }
                val results = stateMachines.map { Try.on { it.resultFuture.getOrThrow() } }
                val successfulIndex = results.mapIndexedNotNull { index, result ->
                    if (result is Try.Success) {
                        val signers = result.value.map { it.by }
                        assertEquals(minCorrectReplicas(clusterSize), signers.size)
                        signers.forEach {
                            assertTrue(it in (notaryIdentity.owningKey as CompositeKey).leafKeys)
                        }
                        index
                    } else {
                        null
                    }
                }.single()
                spendTxs.zip(results).forEach { (tx, result) ->
                    if (result is Try.Failure) {
                        val error = (result.exception as NotaryException).error as NotaryError.Conflict
                        assertEquals(tx.id, error.txId)
                        val (stateRef, consumingTx) = error.conflict.verified().stateHistory.entries.single()
                        assertEquals(StateRef(issueTx.id, 0), stateRef)
                        assertEquals(spendTxs[successfulIndex].id, consumingTx.id)
                        assertEquals(0, consumingTx.inputIndex)
                        assertEquals(info.chooseIdentity(), consumingTx.requestingParty)
                    }
                }
            }
        }
    }

    private fun bftNotarySpec(clusterSize: Int, exposeRaces: Boolean = false): NotarySpec {
        return NotarySpec(
                CordaX500Name("BFT", "Zurich", "CH"),
                validating = false,
                cluster = ClusterSpec.BFTSMaRt(clusterSize = clusterSize, exposeRaces = exposeRaces))
    }

    private fun StartedNode<*>.signInitialTransaction(notary: Party, block: TransactionBuilder.() -> Any?): SignedTransaction {
        return services.signInitialTransaction(
                TransactionBuilder(notary).apply {
                    addCommand(dummyCommand(services.myInfo.chooseIdentity().owningKey))
                    block()
                }
        )
    }
}
