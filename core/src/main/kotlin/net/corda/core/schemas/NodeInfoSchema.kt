package net.corda.core.schemas

import net.corda.core.crypto.toStringShort
import net.corda.core.identity.PartyAndCertificate
import net.corda.core.node.NodeInfo
import net.corda.core.serialization.SerializationDefaults
import net.corda.core.serialization.deserialize
import net.corda.core.serialization.serialize
import net.corda.core.utilities.MAX_HASH_HEX_SIZE
import net.corda.core.utilities.NetworkHostAndPort
import java.io.Serializable
import javax.persistence.*

object NodeInfoSchema

object NodeInfoSchemaV1 : MappedSchema(
        schemaFamily = NodeInfoSchema.javaClass,
        version = 1,
        mappedTypes = listOf(PersistentNodeInfo::class.java, DBPartyAndCertificate::class.java, DBHostAndPort::class.java)
) {
    @Entity
    @Table(name = "node_infos")
    class PersistentNodeInfo(
            @Id
            @GeneratedValue
            @Column(name = "node_info_id")
            var id: Int,

            @Column(name = "addresses")
            @OneToMany(cascade = arrayOf(CascadeType.ALL), orphanRemoval = true)
            val addresses: List<NodeInfoSchemaV1.DBHostAndPort>,

            @Column(name = "legal_identities_certs")
            @ManyToMany(cascade = arrayOf(CascadeType.ALL))
            @JoinTable(name = "link_nodeinfo_party",
                    joinColumns = arrayOf(JoinColumn(name = "node_info_id")),
                    inverseJoinColumns = arrayOf(JoinColumn(name = "party_name")))
            val legalIdentitiesAndCerts: List<DBPartyAndCertificate>,

            @Column(name = "platform_version")
            val platformVersion: Int,

            /**
             * serial is an increasing value which represents the version of [NodeInfo].
             * Not expected to be sequential, but later versions of the registration must have higher values
             * Similar to the serial number on DNS records.
             */
            @Column(name = "serial")
            val serial: Long
    ) {
        fun toNodeInfo(): NodeInfo {
            return NodeInfo(
                    this.addresses.map { it.toHostAndPort() },
                    (this.legalIdentitiesAndCerts.filter { it.isMain } + this.legalIdentitiesAndCerts.filter { !it.isMain }).map { it.toLegalIdentityAndCert() },
                    this.platformVersion,
                    this.serial
            )
        }
    }

    @Embeddable
    data class PKHostAndPort(
            val host: String? = null,
            val port: Int? = null
    ) : Serializable

    @Entity
    data class DBHostAndPort(
            @EmbeddedId
            private val pk: PKHostAndPort
    ) {
        companion object {
            fun fromHostAndPort(hostAndPort: NetworkHostAndPort) = DBHostAndPort(
                    PKHostAndPort(hostAndPort.host, hostAndPort.port)
            )
        }

        fun toHostAndPort(): NetworkHostAndPort {
            return NetworkHostAndPort(this.pk.host!!, this.pk.port!!)
        }
    }

    /**
     *  PartyAndCertificate entity (to be replaced by referencing final Identity Schema).
     */
    @Entity
    @Table(name = "node_info_party_cert")
    data class DBPartyAndCertificate(
            @Id
            @Column(name = "party_name", nullable = false)
            val name: String,

            @Lob
            @Column(name = "owning_key_hash", length = MAX_HASH_HEX_SIZE)
            val owningKeyHash: String,

            @Lob
            @Column(name = "party_cert_binary")
            val partyCertBinary: ByteArray,


            val isMain: Boolean,

            @ManyToMany(mappedBy = "legalIdentitiesAndCerts", cascade = arrayOf(CascadeType.ALL)) // ManyToMany because of distributed services.
            private val persistentNodeInfos: Set<PersistentNodeInfo> = emptySet()
    ) {
        constructor(partyAndCert: PartyAndCertificate, isMain: Boolean = false)
                : this(partyAndCert.name.toString(),
                       partyAndCert.party.owningKey.toStringShort(),
                       partyAndCert.serialize(context = SerializationDefaults.STORAGE_CONTEXT).bytes, isMain)

        fun toLegalIdentityAndCert(): PartyAndCertificate {
            return partyCertBinary.deserialize()
        }
    }
}
