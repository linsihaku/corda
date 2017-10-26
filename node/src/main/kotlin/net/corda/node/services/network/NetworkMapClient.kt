package net.corda.node.services.network

import com.fasterxml.jackson.databind.ObjectMapper
import net.corda.core.crypto.SecureHash
import net.corda.core.crypto.SignedData
import net.corda.core.node.NodeInfo
import net.corda.core.serialization.deserialize
import net.corda.core.serialization.serialize
import net.corda.core.utilities.loggerFor
import net.corda.core.utilities.minutes
import net.corda.core.utilities.seconds
import rx.Observable
import rx.subjects.BehaviorSubject
import java.net.HttpURLConnection
import java.net.URL
import java.util.concurrent.TimeUnit

interface NetworkMapClient {
    companion object {
        val defaultNetworkMapTimeout: Long = 1.minutes.toMillis()
        val logger = loggerFor<NetworkMapClient>()
    }
    /**
     *  Publish node info to network map service.
     */
    fun publish(signedNodeInfo: SignedData<NodeInfo>)

    /**
     *  Retrieve [NetworkMap] from the network map service containing list of node info hashes and network parameter hash.
     */
    // TODO: Use NetworkMap object when available.
    fun getNetworkMap(): Pair<List<SecureHash>, Long>

    val networkMapObservable: Observable<List<SecureHash>>
        get() {
            val timeoutSubject = BehaviorSubject.create(1.seconds.toMillis())
            return timeoutSubject.switchMap {
                Observable.interval(it, it, TimeUnit.SECONDS)
            }.map {
                val (networkMap, timeout) = getNetworkMap()
                timeoutSubject.onNext(timeout)
                networkMap
            }.doOnError { logger.warn("Error encountered when updating network map.", it) }
        }

    /**
     *  Retrieve [NodeInfo] from network map service using the node info hash.
     */
    fun getNodeInfo(nodeInfoHash: SecureHash): NodeInfo?

    fun myHostName(): String

    // TODO: Implement getNetworkParameter when its available.
    //fun getNetworkParameter(networkParameterHash: SecureHash): NetworkParameter
}

class HTTPNetworkMapClient(private val networkMapUrl: URL) : NetworkMapClient {
    override fun publish(signedNodeInfo: SignedData<NodeInfo>) {
        val publishURL = URL("$networkMapUrl/publish")
        val conn = publishURL.openConnection() as HttpURLConnection
        conn.doOutput = true
        conn.requestMethod = "POST"
        conn.setRequestProperty("Content-Type", "application/octet-stream")
        conn.outputStream.write(signedNodeInfo.serialize().bytes)
        when (conn.responseCode) {
            HttpURLConnection.HTTP_OK -> return
            HttpURLConnection.HTTP_UNAUTHORIZED -> throw IllegalArgumentException(conn.errorStream.bufferedReader().readLine())
            else -> throw IllegalArgumentException("Unexpected response code ${conn.responseCode}, response error message: '${conn.errorStream.bufferedReader().readLines()}'")
        }
    }

    override fun getNetworkMap(): Pair<List<SecureHash>, Long> {
        val conn = networkMapUrl.openConnection() as HttpURLConnection
        val networkMap = when (conn.responseCode) {
            HttpURLConnection.HTTP_OK -> {
                val response = conn.inputStream.bufferedReader().use { it.readLine() }
                ObjectMapper().readValue(response, List::class.java).map { SecureHash.parse(it.toString()) }
            }
            else -> throw IllegalArgumentException("Unexpected response code ${conn.responseCode}, response error message: '${conn.errorStream.bufferedReader().readLines()}'")
        }
        val timeout = conn.headerFields["Cache-Control"]?.find { it.startsWith("max-age=") }?.removePrefix("max-age=")?.toLong()
        return Pair(networkMap, timeout ?: NetworkMapClient.defaultNetworkMapTimeout)
    }

    override fun getNodeInfo(nodeInfoHash: SecureHash): NodeInfo? {
        val nodeInfoURL = URL("$networkMapUrl/$nodeInfoHash")
        val conn = nodeInfoURL.openConnection() as HttpURLConnection

        return when (conn.responseCode) {
            HttpURLConnection.HTTP_OK -> conn.inputStream.readBytes().deserialize()
            HttpURLConnection.HTTP_NOT_FOUND -> null
            else -> throw IllegalArgumentException("Unexpected response code ${conn.responseCode}, response error message: '${conn.errorStream.bufferedReader().readLines()}'")
        }
    }

    override fun myHostName(): String {
        return ""
    }
}