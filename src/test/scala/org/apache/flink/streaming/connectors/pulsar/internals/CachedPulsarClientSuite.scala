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
package org.apache.flink.streaming.connectors.pulsar.internals

import java.util.concurrent.ConcurrentMap
import java.{util => ju}

import org.apache.flink.streaming.connectors.pulsar.internal.CachedPulsarClient
import org.apache.pulsar.client.api.PulsarClient
import org.scalatest.PrivateMethodTester

class CachedPulsarClientSuite extends PulsarFunSuite with PrivateMethodTester with PulsarTest {

  import org.apache.flink.streaming.connectors.pulsar.internal.PulsarOptions._

  type KP = PulsarClient

  protected override def beforeEach(): Unit = {
    super.beforeEach()
    CachedPulsarClient.clear()
  }

  test("Should return the cached instance on calling getOrCreate with same params.") {
    val pulsarParams = new ju.HashMap[String, Object]()
    // Here only host should be resolvable, it does not need a running instance of pulsar server.
    pulsarParams.put(SERVICE_URL_OPTION_KEY, "pulsar://127.0.0.1:6650")
    pulsarParams.put("concurrentLookupRequest", "10000")
    val producer = CachedPulsarClient.getOrCreate(pulsarParams)
    val producer2 = CachedPulsarClient.getOrCreate(pulsarParams)
    assert(producer == producer2)

    val cacheMap = PrivateMethod[ConcurrentMap[Seq[(String, Object)], KP]]('getAsMap)
    val map = CachedPulsarClient.invokePrivate(cacheMap())
    assert(map.size == 1)
  }

  test("Should close the correct pulsar producer for the given pulsarPrams.") {
    val pulsarParams = new ju.HashMap[String, Object]()
    pulsarParams.put(SERVICE_URL_OPTION_KEY, "pulsar://127.0.0.1:6650")
    pulsarParams.put("concurrentLookupRequest", "10000")
    val producer: KP = CachedPulsarClient.getOrCreate(pulsarParams)
    pulsarParams.put("concurrentLookupRequest", "20000")
    val producer2: KP = CachedPulsarClient.getOrCreate(pulsarParams)
    // With updated conf, a new producer instance should be created.
    assert(producer != producer2)

    val cacheMap = PrivateMethod[ConcurrentMap[Seq[(String, Object)], KP]]('getAsMap)
    val map = CachedPulsarClient.invokePrivate(cacheMap())
    assert(map.size == 2)

    CachedPulsarClient.close(pulsarParams)
    val map2 = CachedPulsarClient.invokePrivate(cacheMap())
    assert(map2.size == 1)
    import scala.collection.JavaConverters._
    val (seq: Seq[(String, Object)], _producer: KP) = map2.asScala.toArray.apply(0)
    assert(_producer == producer)
  }
}
