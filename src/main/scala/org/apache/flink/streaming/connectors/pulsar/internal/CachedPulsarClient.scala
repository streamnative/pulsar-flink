/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.streaming.connectors.pulsar.internal

import java.{util => ju}
import java.util.concurrent.{ConcurrentMap, ExecutionException}

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import org.apache.pulsar.shade.com.google.common.cache.{CacheBuilder, CacheLoader, LoadingCache, RemovalListener, RemovalNotification}
import org.apache.pulsar.shade.com.google.common.util.concurrent.{ExecutionError, UncheckedExecutionException}

object CachedPulsarClient extends Logging {

  private type Client = org.apache.pulsar.client.api.PulsarClient

  private var cacheSize = 5

  def setCacheSize(size: Int): Unit = {
    cacheSize = size
  }

  def getCacheSize(): Int = cacheSize

  private val cacheLoader = new CacheLoader[Seq[(String, Object)], Client] {
    override def load(config: Seq[(String, Object)]): Client = {
      val configMap = config.map(x => x._1 -> x._2).toMap.asJava
      createPulsarClient(configMap)
    }
  }

  private val removalListener = new RemovalListener[Seq[(String, Object)], Client]() {
    override def onRemoval(
        notification: RemovalNotification[Seq[(String, Object)], Client]): Unit = {
      val paramsSeq: Seq[(String, Object)] = notification.getKey
      val client: Client = notification.getValue
      logDebug(
        s"Evicting pulsar producer $client params: $paramsSeq, due to ${notification.getCause}")
      close(paramsSeq, client)
    }
  }

  private lazy val guavaCache: LoadingCache[Seq[(String, Object)], Client] =
    CacheBuilder
      .newBuilder()
      .maximumSize(cacheSize)
      .removalListener(removalListener)
      .build[Seq[(String, Object)], Client](cacheLoader)

  private def createPulsarClient(pulsarConf: ju.Map[String, Object]): Client = {
    val pulsarServiceUrl =
      pulsarConf.get(PulsarOptions.SERVICE_URL_OPTION_KEY).asInstanceOf[String]
    val clientConf = new PulsarConfigUpdater(
      "pulsarClientCache",
      pulsarConf.asScala.toMap,
      PulsarOptions.FILTERED_KEYS
    ).rebuild()
    logInfo(s"Client Conf = $clientConf")
    try {
      val pulsarClient: Client = org.apache.pulsar.client.api.PulsarClient
        .builder()
        .serviceUrl(pulsarServiceUrl)
        .loadConf(clientConf)
        .build()
      logDebug(
        s"Created a new instance of PulsarClient for serviceUrl = $pulsarServiceUrl,"
          + s" clientConf = $clientConf.")
      pulsarClient
    } catch {
      case e: Throwable =>
        logError(
          s"Failed to create PulsarClient to serviceUrl $pulsarServiceUrl"
            + s" using client conf $clientConf",
          e)
        throw e
    }
  }

  /**
   * Get a cached PulsarProducer for a given configuration. If matching PulsarProducer doesn't
   * exist, a new PulsarProducer will be created. PulsarProducer is thread safe, it is best to keep
   * one instance per specified pulsarParams.
   */
  def getOrCreate(pulsarParams: ju.Map[String, Object]): Client = {
    val paramsSeq: Seq[(String, Object)] = paramsToSeq(pulsarParams)
    try {
      guavaCache.get(paramsSeq)
    } catch {
      case e @ (_: ExecutionException | _: UncheckedExecutionException | _: ExecutionError)
          if e.getCause != null =>
        throw e.getCause
    }
  }

  private def paramsToSeq(pulsarParams: ju.Map[String, Object]): Seq[(String, Object)] = {
    val paramsSeq: Seq[(String, Object)] = pulsarParams.asScala.toSeq.sortBy(x => x._1)
    paramsSeq
  }

  /** For explicitly closing pulsar producer */
  def close(pulsarParams: ju.Map[String, Object]): Unit = {
    val paramsSeq = paramsToSeq(pulsarParams)
    guavaCache.invalidate(paramsSeq)
  }

  /** Auto close on cache evict */
  private def close(paramsSeq: Seq[(String, Object)], client: Client): Unit = {
    try {
      logInfo(s"Closing the Pulsar Client with params: ${paramsSeq.mkString("\n")}.")
      client.close()
    } catch {
      case NonFatal(e) => logWarning("Error while closing pulsar producer.", e)
    }
  }

  def clear(): Unit = {
    logInfo("Cleaning up guava cache.")
    guavaCache.invalidateAll()
  }

  // Intended for testing purpose only.
  private def getAsMap: ConcurrentMap[Seq[(String, Object)], Client] = guavaCache.asMap()
}
