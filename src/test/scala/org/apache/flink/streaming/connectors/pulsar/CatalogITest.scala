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
package org.apache.flink.streaming.connectors.pulsar

import java.{util => ju}
import java.io.File
import java.net.URL

import scala.collection.JavaConverters._

import com.google.common.collect.Sets
import org.apache.commons.cli.Options
import org.apache.flink.client.cli.DefaultCLI
import org.apache.flink.configuration.Configuration
import org.apache.flink.pulsar.{PulsarFlinkTest, PulsarFunSuite, Utils}
import org.apache.flink.table.catalog.pulsar.PulsarCatalog
import org.apache.flink.table.client.config.Environment
import org.apache.flink.table.client.gateway.SessionContext
import org.apache.flink.table.client.gateway.local.ExecutionContext
import org.apache.flink.util.FileUtils
import org.apache.pulsar.client.admin.PulsarAdmin
import org.apache.pulsar.common.policies.data.TenantInfo

class CatalogITest extends PulsarFunSuite with PulsarFlinkTest {

  val CATALOGS_ENVIRONMENT_FILE = "test-sql-client-pulsar-catalog.yaml"

  test("test catalogs") {
    val inmemoryCatalog = "inmemorycatalog"
    val pulsarCatalog1 = "pulsarcatalog1"
    val pulsarCatalog2 = "pulsarcatalog2"

    val context = createExecutionContext(CATALOGS_ENVIRONMENT_FILE)
    val tableEnv = context.createEnvironmentInstance.getTableEnvironment()

    assert(tableEnv.getCurrentCatalog === inmemoryCatalog)
    assert(tableEnv.getCurrentDatabase === "mydatabase")

    var catalog = tableEnv.getCatalog(pulsarCatalog1).orElse(null)
    assert(catalog != null)
    assert(catalog.isInstanceOf[PulsarCatalog])
    tableEnv.useCatalog(pulsarCatalog1)
    assert(tableEnv.getCurrentDatabase === "public/default")

    catalog = tableEnv.getCatalog(pulsarCatalog2).orElse(null)
    assert(catalog != null)
    assert(catalog.isInstanceOf[PulsarCatalog])
    tableEnv.useCatalog(pulsarCatalog2)
    assert(tableEnv.getCurrentDatabase === "tn/ns")
  }


  test("test databases") {
    val pulsarCatalog1 = "pulsarcatalog1"
    val namespaces = "tn1/ns1" :: "tn1/ns2" :: Nil
    val topics = "tp1" :: "tp2" :: Nil
    val topicsFullName = topics.map(a => s"tn1/ns1/$a")
    val partitionedTopics = "ptp1" :: "ptp2" :: Nil
    val partitionedTopicsFullName = partitionedTopics.map(a => s"tn1/ns1/$a")

    val context = createExecutionContext(CATALOGS_ENVIRONMENT_FILE)
    val tableEnv = context.createEnvironmentInstance.getTableEnvironment()

    tableEnv.useCatalog(pulsarCatalog1)
    assert(tableEnv.getCurrentDatabase === "public/default")

    Utils.tryWithResource(PulsarAdmin.builder().serviceHttpUrl(adminUrl).build()) { admin =>
      admin.tenants().createTenant("tn1",
        new TenantInfo(Sets.newHashSet(), Sets.newHashSet("standalone")))
      namespaces.foreach { ns =>
        admin.namespaces().createNamespace(ns)
      }
      topicsFullName.foreach { tp =>
        admin.topics().createNonPartitionedTopic(tp)
      }
      partitionedTopicsFullName.foreach { tp =>
        admin.topics().createPartitionedTopic(tp, 5)
      }

      assert(namespaces.toSet.subsetOf(tableEnv.listDatabases().toSet))

      tableEnv.useDatabase("tn1/ns1")
      assert(tableEnv.listTables().toSet == (topics ++ partitionedTopics).toSet)

      topicsFullName.foreach { tp =>
        admin.topics().delete(tp)
      }
      partitionedTopicsFullName.foreach { tp =>
        admin.topics().deletePartitionedTopic(tp)
      }
      namespaces.foreach { ns =>
        admin.namespaces().deleteNamespace(ns)
      }
    }
  }

  test("test tables") {


  }

  private def createExecutionContext[T](file: String): ExecutionContext[T] = {
    val replaceVars = new ju.HashMap[String, String]
    replaceVars.put("$VAR_EXECUTION_TYPE", "streaming")
    replaceVars.put("$VAR_RESULT_MODE", "changelog")
    replaceVars.put("$VAR_UPDATE_MODE", "update-mode: append")
    replaceVars.put("$VAR_MAX_ROWS", "100")
    replaceVars.put("$VAR_SERVICEURL", serviceUrl)
    replaceVars.put("$VAR_ADMINURL", adminUrl)
    val env = EnvironmentFileUtil.parseModified(file, replaceVars)

    val session = new SessionContext("test-session", new Environment)
    val flinkConfig = new Configuration()
    new ExecutionContext[T](
      env,
      session,
      ju.Collections.emptyList[URL],
      flinkConfig,
      new Options(),
      ju.Collections.singletonList(new DefaultCLI(flinkConfig)))
  }
}

object EnvironmentFileUtil {

  def parseUnmodified(fileName: String): Environment = {
    val url = getClass.getClassLoader.getResource(fileName)
    ju.Objects.requireNonNull(url)
    Environment.parse(url)
  }

  def parseModified(fileName: String, replaceVars: ju.Map[String, String]): Environment = {
    val url = getClass.getClassLoader.getResource(fileName)
    ju.Objects.requireNonNull(url)
    var schema = FileUtils.readFileUtf8(new File(url.getFile))
    import scala.collection.JavaConversions._
    for (replaceVar <- replaceVars.entrySet) {
      schema = schema.replace(replaceVar.getKey, replaceVar.getValue)
    }
    Environment.parse(schema)
  }
}
