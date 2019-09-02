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
import java.util.concurrent.atomic.AtomicInteger

import com.google.common.collect.Sets
import org.apache.commons.cli.Options
import org.apache.flink.client.cli.DefaultCLI
import org.apache.flink.configuration.Configuration
import org.apache.flink.pulsar.PulsarFunSuite
import org.apache.flink.streaming.connectors.pulsar.internal.Utils
import org.apache.flink.streaming.connectors.pulsar.internals.{PulsarFlinkTest, PulsarFunSuite}
import org.apache.flink.streaming.connectors.pulsar.testutils.FailingIdentityMapper
import org.apache.flink.table.api.internal.TableImpl
import org.apache.flink.table.api.java.StreamTableEnvironment
import org.apache.flink.table.catalog.pulsar.PulsarCatalog
import org.apache.flink.table.client.config.Environment
import org.apache.flink.table.client.gateway.SessionContext
import org.apache.flink.table.client.gateway.local.ExecutionContext
import org.apache.flink.table.runtime.utils.StreamITCase
import org.apache.flink.types.Row
import org.apache.flink.util.FileUtils
import org.apache.pulsar.client.admin.PulsarAdmin
import org.apache.pulsar.common.naming.TopicName
import org.apache.pulsar.common.policies.data.TenantInfo
import org.apache.pulsar.common.schema.SchemaType

class CatalogITest extends PulsarFunSuite with PulsarFlinkTest {

  val CATALOGS_ENVIRONMENT_FILE = "test-sql-client-pulsar-catalog.yaml"
  val CATALOGS_ENVIRONMENT_FILE_START = "test-sql-client-pulsar-start-catalog.yaml"

  override def beforeEach(): Unit = {
    super.beforeEach()
    StreamITCase.testResults.clear()
    FailingIdentityMapper.failedBefore = false
  }

  test("test catalogs") {
    val inmemoryCatalog = "inmemorycatalog"
    val pulsarCatalog1 = "pulsarcatalog1"
    val pulsarCatalog2 = "pulsarcatalog2"

    val context = createExecutionContext(streamingConfs, CATALOGS_ENVIRONMENT_FILE)
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

    val context = createExecutionContext(streamingConfs, CATALOGS_ENVIRONMENT_FILE)
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

  test("test tables read - start from latest by default") {
    import org.apache.flink.streaming.connectors.pulsar.internals.SchemaData._

    val pulsarCatalog1 = "pulsarcatalog1"

    val tableName = newTopic()

    sendTypedMessages[Int](tableName, SchemaType.INT32, int32Seq.toArray, None)

    val context = createExecutionContext(streamingConfs, CATALOGS_ENVIRONMENT_FILE)
    val tableEnv = context.createEnvironmentInstance.getTableEnvironment()

    tableEnv.useCatalog(pulsarCatalog1)

    val t = tableEnv.scan(TopicName.get(tableName).getLocalName).select("value")

    val tpe = t.getSchema.toRowType

    val stream = t.asInstanceOf[TableImpl].getTableEnvironment.asInstanceOf[StreamTableEnvironment].toAppendStream(t, tpe)
    stream.map(new FailingIdentityMapper[Row](int32Seq.length))

    stream.addSink(new StreamITCase.StringSink[Row]).setParallelism(1)

    val runner = new Thread("runner") {
      override def run(): Unit = {
        try {
          tableEnv.execute("read from latest")
        } catch {
          case t: Throwable => // do nothing
        }
      }
    }
    runner.start()

    // wait a little while for flink job start and send messages
    Thread.sleep(2000)
    sendTypedMessages[Int](tableName, SchemaType.INT32, int32Seq.toArray, None)

    Thread.sleep(2000)
    assert(StreamITCase.testResults === int32Seq.init.map(_.toString))
  }

  test("test tables read - start from earliest by conf") {
    import org.apache.flink.streaming.connectors.pulsar.internals.SchemaData._

    val pulsarCatalog1 = "pulsarcatalog1"

    val tableName = newTopic()

    sendTypedMessages[Int](tableName, SchemaType.INT32, int32Seq.toArray, None)

    val conf = streamingConfs()
    conf.put("$VAR_STARTING", "earliest")

    val context = createExecutionContext(conf, CATALOGS_ENVIRONMENT_FILE_START)
    val tableEnv = context.createEnvironmentInstance.getTableEnvironment()

    tableEnv.useCatalog(pulsarCatalog1)

    val t = tableEnv.scan(TopicName.get(tableName).getLocalName).select("value")

    val tpe = t.getSchema.toRowType

    val stream = t.asInstanceOf[TableImpl].getTableEnvironment.asInstanceOf[StreamTableEnvironment].toAppendStream(t, tpe)
    stream.map(new FailingIdentityMapper[Row](int32Seq.length))

    stream.addSink(new StreamITCase.StringSink[Row]).setParallelism(1)

    val runner = new Thread("runner") {
      override def run(): Unit = {
        try {
          tableEnv.execute("read from ealiest")
        } catch {
          case t: Throwable => // do nothing
        }
      }
    }
    runner.start()

    Thread.sleep(2000)
    assert(StreamITCase.testResults === int32Seq.init.map(_.toString))
  }

  test("test table sink") {
    import org.apache.flink.streaming.connectors.pulsar.internals.SchemaData._

    val tp = newTopic()
    val tableName = TopicName.get(tp).getLocalName

    sendTypedMessages[Int](tp, SchemaType.INT32, int32Seq.toArray, None)

    val conf = streamingConfs()
    conf.put("$VAR_STARTING", "earliest")
    val context = createExecutionContext(conf, CATALOGS_ENVIRONMENT_FILE_START)
    val tableEnv = context.createEnvironmentInstance().getTableEnvironment

    tableEnv.useCatalog("pulsarcatalog1")

    val sinkDDL = "create table tableSink(v int)"
    val insertQ = s"INSERT INTO tableSink SELECT * FROM `$tableName`"

    tableEnv.sqlUpdate(sinkDDL)
    tableEnv.sqlUpdate(insertQ)

    val runner = new Thread("write to table") {
      override def run(): Unit = {
        try {
          tableEnv.execute("write to table")
        } catch {
          case t: Throwable => // do nothing
        }
      }
    }
    runner.start()

    val conf1 = streamingConfs()
    conf1.put("$VAR_STARTING", "earliest")
    val context1 = createExecutionContext(conf1, CATALOGS_ENVIRONMENT_FILE_START)
    val tableEnv1 = context1.createEnvironmentInstance().getTableEnvironment

    tableEnv1.useCatalog("pulsarcatalog1")

    val t = tableEnv1.scan("tableSink").select("v")

    val tpe = t.getSchema.toRowType

    val stream = t.asInstanceOf[TableImpl].getTableEnvironment.asInstanceOf[StreamTableEnvironment].toAppendStream(t, tpe)
    stream.map(new FailingIdentityMapper[Row](int32Seq.length))

    stream.addSink(new StreamITCase.StringSink[Row]).setParallelism(1)

    val reader = new Thread("runner") {
      override def run(): Unit = {
        try {
          tableEnv1.execute("read from ealiest")
        } catch {
          case t: Throwable => // do nothing
        }
      }
    }
    reader.start()

    reader.join()
    assert(StreamITCase.testResults === int32Seq.init.map(_.toString))
  }

  private val topicId = new AtomicInteger(0)

  private def newTopic(): String = TopicName.get(s"topic-${topicId.getAndIncrement()}").toString

  private def createExecutionContext[T](replaceVars: ju.HashMap[String, String], file: String): ExecutionContext[T] = {
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

  private def streamingConfs(): ju.HashMap[String, String]  = {
    val replaceVars = new ju.HashMap[String, String]
    replaceVars.put("$VAR_EXECUTION_TYPE", "streaming")
    replaceVars.put("$VAR_RESULT_MODE", "changelog")
    replaceVars.put("$VAR_UPDATE_MODE", "update-mode: append")
    replaceVars.put("$VAR_MAX_ROWS", "100")
    replaceVars.put("$VAR_SERVICEURL", serviceUrl)
    replaceVars.put("$VAR_ADMINURL", adminUrl)
    replaceVars
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
