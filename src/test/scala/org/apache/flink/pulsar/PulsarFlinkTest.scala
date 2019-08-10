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
package org.apache.flink.pulsar

import scala.collection.JavaConverters._
import org.scalatest.time.SpanSugar._

import org.apache.flink.api.common.JobID
import org.apache.flink.client.program.ClusterClient
import org.apache.flink.configuration.{AkkaOptions, ConfigConstants, Configuration, TaskManagerOptions}
import org.apache.flink.metrics.jmx.JMXReporter
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration
import org.apache.flink.test.util.MiniClusterWithClientResource

trait PulsarFlinkTest extends PulsarTest {
  self: PulsarFunSuite =>

  val streamingTimeout = 60.seconds

  var flinkClient: ClusterClient[_] = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    val fConfig = new Configuration()
    fConfig.setString(AkkaOptions.WATCH_HEARTBEAT_PAUSE, "5 s")
    fConfig.setString(AkkaOptions.WATCH_HEARTBEAT_INTERVAL, "1 s")
    fConfig.setString(TaskManagerOptions.MANAGED_MEMORY_SIZE, "16m")
    fConfig.setString(ConfigConstants.RESTART_STRATEGY_FIXED_DELAY_DELAY, "0 s")
    fConfig.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "my_reporter." +
      ConfigConstants.METRICS_REPORTER_CLASS_SUFFIX, classOf[JMXReporter].getName)

    val flink = new MiniClusterWithClientResource(
      new MiniClusterResourceConfiguration.Builder()
        .setConfiguration(fConfig)
        .setNumberTaskManagers(1)
        .setNumberSlotsPerTaskManager(8).build())

    flinkClient = flink.getClusterClient
    waitUntilNoJobIsRunning(flinkClient)
  }


  def waitUntilJobIsRunning(client: ClusterClient[_]): Unit = {
    while (getRunningJobs(client).isEmpty) {
      Thread.sleep(50)
    }
  }

  def waitUntilNoJobIsRunning(client: ClusterClient[_]): Unit = {
    while (!getRunningJobs(client).isEmpty) {
      Thread.sleep(50)
    }
  }

  def getRunningJobs(client: ClusterClient[_]): Seq[JobID] = {
    val statusMessages= client.listJobs.get
    statusMessages.asScala.filter(!_.getJobState.isGloballyTerminalState).map(_.getJobId).toSeq
  }

}
