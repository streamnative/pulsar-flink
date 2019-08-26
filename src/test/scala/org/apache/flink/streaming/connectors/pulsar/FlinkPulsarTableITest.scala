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

import java.util.Properties
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.pulsar.{PulsarFlinkTest, PulsarFunSuite}
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.pulsar.testutils.FailingIdentityMapper
import org.apache.flink.table.descriptors.Pulsar
import org.apache.flink.table.runtime.utils.StreamITCase
import org.apache.flink.test.util.TestUtils
import org.apache.flink.types.Row

import org.apache.pulsar.common.naming.TopicName
import org.apache.pulsar.common.schema.SchemaType

class FlinkPulsarTableITest extends PulsarFunSuite with PulsarFlinkTest {
  import org.apache.flink.pulsar.SchemaData._
  import org.apache.flink.pulsar.PulsarOptions._
  import org.apache.flink.table.api._
  import org.apache.flink.table.api.scala._

  override def beforeEach(): Unit = {
    super.beforeEach()
    StreamITCase.testResults.clear()
    FailingIdentityMapper.failedBefore = false
  }

  test("basic functioning") {

    val see = StreamExecutionEnvironment.getExecutionEnvironment
    see.setParallelism(1)
    val tEnv = StreamTableEnvironment.create(see)


    val table = newTopic()

    sendTypedMessages[Boolean](table, SchemaType.BOOLEAN, booleanSeq :+ true, None)

    val props = sourceProperties()
    props.setProperty(TOPIC_SINGLE, table)

    tEnv
      .connect(new Pulsar().properties(props))
      .inAppendMode()
      .registerTableSource(table)

    val t: Table = tEnv.scan(table).select("value")
    t.printSchema()
    implicit val ti = t.getSchema.toRowType
    val as = t.toAppendStream[Row]
    as.map(new FailingIdentityMapper[Row](booleanSeq.length + 1))

    as.addSink(new StreamITCase.StringSink[Row]).setParallelism(1)

    intercept[Throwable] {
      see.execute()
    }

    assert(StreamITCase.testResults == booleanSeq.map(_.toString))
  }

  test("write then read") {
    val tp = newTopic()

    val see = StreamExecutionEnvironment.getExecutionEnvironment
    see.getConfig.disableSysoutLogging()
    see.setParallelism(1)

    val ds = see.fromCollection(fooSeq)(TypeInformation.of(classOf[Foo]))
    val sinkProp = sinkProperties()
    sinkProp.setProperty(TOPIC_SINGLE, tp)

    ds.addSink(new FlinkPulsarSink[Foo](sinkProp, new TopicKeyExtractor[Foo] {
      override def serializeKey(element: Foo): Array[Byte] = null
      override def getTopic(element: Foo): String = null
    }))
    see.execute("write first")

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.getConfig.disableSysoutLogging()
    env.setParallelism(1)

    val sourceProps = sourceProperties()
    sourceProps.setProperty(TOPIC_SINGLE, tp)

    val tEnv = StreamTableEnvironment.create(env)

    tEnv
      .connect(new Pulsar().properties(sourceProps))
      .inAppendMode()
      .registerTableSource(tp)

    val t0 = tEnv.scan(tp)
    val t = t0.select("i, f, bar")
    implicit val ti = t.getSchema.toRowType
    val as = t.toAppendStream[Row]
    as.map(new FailingIdentityMapper[Row](fooSeq.length))

    as.addSink(new StreamITCase.StringSink[Row]).setParallelism(1)


    intercept[Throwable] {
      TestUtils.tryExecute(env.getJavaEnv, "count elements from topics")
    }
    assert(StreamITCase.testResults == fooSeq.init.map(_.toString))
  }

  test("test struct types in avro") {

    val see = StreamExecutionEnvironment.getExecutionEnvironment
    see.setParallelism(1)
    val tEnv = StreamTableEnvironment.create(see)

    val table = newTopic()

    sendTypedMessages[Foo](table, SchemaType.AVRO, fooSeq, None)

    val props = sourceProperties()
    props.setProperty(TOPIC_SINGLE, table)

    tEnv
      .connect(new Pulsar().properties(props))
      .inAppendMode()
      .registerTableSource(table)

    val t: Table = tEnv.scan(table).select("i, f, bar")
    t.printSchema()
    implicit val ti = t.getSchema.toRowType
    val as = t.toAppendStream[Row]
    as.map(new FailingIdentityMapper[Row](fooSeq.length))

    as.addSink(new StreamITCase.StringSink[Row]).setParallelism(1)

    intercept[Throwable] {
      see.execute()
    }

    assert(StreamITCase.testResults == fooSeq.init.map(_.toString))
  }

  def sourceProperties(): Properties = {
    val prop = new Properties()
    prop.setProperty(SERVICE_URL_OPTION_KEY, serviceUrl)
    prop.setProperty(ADMIN_URL_OPTION_KEY, adminUrl)
    prop.setProperty(PARTITION_DISCOVERY_INTERVAL_MS, "5000")
    prop.setProperty(STARTING_OFFSETS_OPTION_KEY, "earliest")
    prop
  }

  def sinkProperties(): Properties = {
    val prop = new Properties()
    prop.setProperty(SERVICE_URL_OPTION_KEY, serviceUrl)
    prop.setProperty(ADMIN_URL_OPTION_KEY, adminUrl)
    prop.setProperty(FLUSH_ON_CHECKPOINT, "true")
    prop.setProperty(FAIL_ON_WRITE, "true")
    prop
  }

  private val topicId = new AtomicInteger(0)

  private def newTopic(): String = TopicName.get(s"topic-${topicId.getAndIncrement()}").toString

}

class StringSink[T] extends RichSinkFunction[T]() {
  val testResults = mutable.MutableList.empty[String]
  override def invoke(value: T) {
    testResults.synchronized {
      testResults += value.toString
    }
  }
}
