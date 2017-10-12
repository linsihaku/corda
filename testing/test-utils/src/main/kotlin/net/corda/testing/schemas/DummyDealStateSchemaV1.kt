package net.corda.testing.schemas

import net.corda.core.contracts.UniqueIdentifier
import net.corda.core.identity.AbstractParty
import net.corda.core.schemas.CommonSchemaV1
import net.corda.core.schemas.MappedSchema
import org.hibernate.annotations.Type
import javax.persistence.Entity
import javax.persistence.Table
import javax.persistence.Transient

/**
 * An object used to fully qualify the [DummyDealStateSchema] family name (i.e. independent of version).
 */
object DummyDealStateSchema

/**
 * First version of a cash contract ORM schema that maps all fields of the [DummyDealState] contract state as it stood
 * at the time of writing.
 */
object DummyDealStateSchemaV1 : MappedSchema(schemaFamily = DummyDealStateSchema.javaClass, version = 1, mappedTypes = listOf(PersistentDummyDealState::class.java)) {
    @Entity
    @Table(name = "dummy_deal_states")
    class PersistentDummyDealState(
            /** parent attributes */
            @Transient
            @Type(type = "party")
            val _participants: Set<AbstractParty>,

            @Transient
            val uid: UniqueIdentifier

    ) : CommonSchemaV1.LinearState(uid, _participants)
}
