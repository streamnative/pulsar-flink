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
package org.apache.flink.connectors.pulsar.common

import java.{util => ju}

import scala.collection.JavaConverters._

/**
 * Class to conveniently update pulsar config params, while logging the changes.
 */
private[pulsar] case class PulsarConfigUpdater(
    module: String,
    pulsarParams: Map[String, Object],
    blacklistedKeys: Set[String] = Set())
    extends Logging {

  private val map = new ju.HashMap[String, Object](pulsarParams.asJava)

  def set(key: String, value: Object): this.type = {
    set(key, value, map)
  }

  def set(key: String, value: Object, map: ju.Map[String, Object]): this.type = {
    if (blacklistedKeys.contains(key)) {
      logInfo(s"$module: Skip $key")
    } else {
      map.put(key, value)
      logInfo(s"$module: Set $key to $value, earlier value: ${pulsarParams.getOrElse(key, "")}")
    }
    this
  }

  def setIfUnset(key: String, value: Object): this.type = {
    setIfUnset(key, value, map)
  }

  def setIfUnset(key: String, value: Object, map: ju.Map[String, Object]): this.type = {
    if (blacklistedKeys.contains(key)) {
      logInfo(s"$module: Skip $key")
    } else {
      if (!map.containsKey(key)) {
        map.put(key, value)
        logDebug(s"$module: Set $key to $value")
      }
    }
    this
  }

  def setAuthenticationConfigIfNeeded(): this.type = {
    // FIXME: not implemented yet
    this
  }

  def build(): ju.Map[String, Object] = map

  def rebuild(): ju.Map[String, Object] = {
    val map = new ju.HashMap[String, Object]()
    pulsarParams map {
      case (k, v) =>
        set(k, v, map)
    }
    map
  }

}
