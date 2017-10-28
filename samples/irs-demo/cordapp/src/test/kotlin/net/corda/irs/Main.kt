package net.corda.irs

import net.corda.core.internal.concurrent.map
import net.corda.core.utilities.getOrThrow
import net.corda.testing.DUMMY_BANK_A
import net.corda.testing.DUMMY_BANK_B
import net.corda.testing.driver.driver

/**
 * This file is exclusively for being able to run your nodes through an IDE (as opposed to running deployNodes)
 * Do not use in a production environment.
 */
fun main(args: Array<String>) {
    driver(useTestClock = true, isDebug = true) {
        val (controller, nodeA, nodeB) = listOf(
                notaries[0].nodeHandles.map { it[0] },
                startNode(providedName = DUMMY_BANK_A.name),
                startNode(providedName = DUMMY_BANK_B.name))
                .map { it.getOrThrow() }

        startWebserver(controller)
        startWebserver(nodeA)
        startWebserver(nodeB)

        waitForAllNodesToFinish()
    }
}
