package net.corda.bank

import net.corda.bank.api.BankOfCordaClientApi
import net.corda.bank.api.BankOfCordaWebApi.IssueRequestParams
import net.corda.core.utilities.getOrThrow
import net.corda.testing.BOC
import net.corda.testing.DUMMY_NOTARY
import net.corda.testing.driver.driver
import org.junit.Test
import kotlin.test.assertTrue

class BankOfCordaHttpAPITest {
    @Test
    fun `issuer flow via Http`() {
        driver(extraCordappPackagesToScan = listOf("net.corda.finance"), isDebug = true) {
            val bigCorpNodeFuture = startNode(providedName = BIGCORP_LEGAL_NAME)
            val nodeBankOfCordaFuture = startNode(providedName = BOC.name)
            val (nodeBankOfCorda) = listOf(nodeBankOfCordaFuture, bigCorpNodeFuture).map { it.getOrThrow() }
            val nodeBankOfCordaApiAddr = startWebserver(nodeBankOfCorda).getOrThrow().listenAddress
            assertTrue(BankOfCordaClientApi(nodeBankOfCordaApiAddr).requestWebIssue(IssueRequestParams(1000, "USD", BIGCORP_LEGAL_NAME, "1", BOC.name, DUMMY_NOTARY.name)))
        }
    }
}
