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

import java.util.{Properties, Random, UUID}
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}

import org.apache.flink.api.common.JobExecutionResult
import org.apache.flink.streaming.api.environment.{StreamExecutionEnvironment => JavaEnv}

import scala.collection.mutable
import scala.collection.JavaConverters._
import org.apache.flink.api.common.functions.{FlatMapFunction, MapFunction, RichFlatMapFunction, RichFunction}
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeHint, TypeInformation}
import org.apache.flink.api.dag.Transformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.client.JobCancellationException
import org.apache.flink.runtime.jobgraph.JobStatus
import org.apache.flink.shaded.guava18.com.google.common.collect.Iterables
import org.apache.flink.streaming.api.functions.sink.DiscardingSink
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.graph.{StreamGraph, StreamingJobGraphGenerator}
import org.apache.flink.streaming.api.operators.StreamSink
import org.apache.flink.streaming.connectors.pulsar.internal.{CachedPulsarClient, JsonUtils, SourceSinkUtils}
import org.apache.flink.streaming.connectors.pulsar.internals.{PulsarFlinkTest, PulsarFunSuite}
import org.apache.flink.streaming.connectors.pulsar.testutils.{FailingIdentityMapper, ValidatingExactlyOnceSink}
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness
import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.types.DataType
import org.apache.flink.table.types.utils.LegacyTypeInfoDataTypeConverter
import org.apache.flink.test.util.{SuccessException, TestUtils}
import org.apache.flink.types.Row
import org.apache.flink.util.{Collector, ExceptionUtils}
import org.apache.pulsar.client.api.{MessageId, PulsarClient, Schema}
import org.apache.pulsar.common.naming.TopicName
import org.apache.pulsar.common.schema.SchemaType

class FlinkPulsarITest extends PulsarFunSuite with PulsarFlinkTest {

  import org.apache.flink.streaming.connectors.pulsar.internal.PulsarOptions._

  test("run failed on wrong service url") {
    val props = sourceProperties()
    props.setProperty(SERVICE_URL_OPTION_KEY, "dummy")
    props.setProperty(TOPIC_SINGLE, newTopic())

    val see = StreamExecutionEnvironment.getExecutionEnvironment
    see.getConfig.disableSysoutLogging()

    // Sink FlinkPulsarSource is ResultTypeQueryable, we don't need to provide an extra TypeInfo
    val stream = see.addSource(new FlinkPulsarSource(props))(null)
    stream.print()
    val ex = intercept[Throwable] {
      see.execute("wrong service url test")
    }
    assert(causedBySpecificException[IllegalArgumentException](
      ex, "authority component is missing"))
  }

  test("case sensitive reader conf") {

    val tp = newTopic()
    val messages = (0 until 50)
    sendTypedMessages[Int](tp, SchemaType.INT32, messages, None)

    val sourceProps = sourceProperties()
    sourceProps.setProperty("pulsar.reader.receiverQueueSize", "1000000")
    sourceProps.setProperty(TOPIC_SINGLE, tp)

    val see = StreamExecutionEnvironment.getExecutionEnvironment
    see.getConfig.disableSysoutLogging()

    val source = new FlinkPulsarSource(sourceProps)

    val stream = see.addSource(source)(null)
    stream.addSink(new DiscardingSink[Row]())

    // launch a consumer asynchronously
    val jobError = new AtomicReference[Throwable]()

    val jobGraph = StreamingJobGraphGenerator.createJobGraph(see.getStreamGraph)
    val jobId = jobGraph.getJobID

    val jobRunner = new Runnable {
      override def run(): Unit = {
        try {
          flinkClient.setDetached(false)
          flinkClient.submitJob(jobGraph, getClass.getClassLoader)
        } catch {
          case e: Throwable =>
            jobError.set(e)
        }
      }
    }

    val runnerThread = new Thread(jobRunner, "program runner thread")
    runnerThread.start()

    Thread.sleep(2000)
    val failureCause = jobError.get()

    if (failureCause != null) {
      failureCause.printStackTrace()
      fail("Test failed prematurely with: " + failureCause.getMessage)
    }

    // cancel
    flinkClient.cancel(jobId)

    // wait for the program to be done and validate that we failed with the right exception
    runnerThread.join()

    assert(flinkClient.getJobStatus(jobId).get().toString == JobStatus.CANCELED.toString)
  }

  test("case sensitive producer conf") {
    val numTopic = 5
    val numElements = 20

    val see = StreamExecutionEnvironment.getExecutionEnvironment
    see.getConfig.disableSysoutLogging()
    see.setParallelism(1)

    implicit val tpe = intRowWithTopicTypeInfo()

    val topics = (0 until numTopic).map(_ => newTopic())
    val stream = see.addSource(new MutiTopicSource(topics, numElements))

    val sinkProp = sinkProperties()
    sinkProp.setProperty("pulsar.producer.blockIfQueueFull", "true")
    sinkProp.setProperty("pulsar.producer.maxPendingMessages", "100000")
    sinkProp.setProperty("pulsar.producer.maxPendingMessagesAcrossPartitions", "5000000")
    sinkProp.setProperty("pulsar.producer.sendTimeoutMs", "30000")
    produceIntoPulsar(stream, intRowWithTopicType(), sinkProp)
    see.execute("write with topics")
  }

  test("assure client cache parameters passed to tasks") {
    val numTopic = 5
    val numElements = 20

    val see = StreamExecutionEnvironment.getExecutionEnvironment
    see.getConfig.disableSysoutLogging()
    see.setParallelism(3)

    implicit val tpe = intRowWithTopicTypeInfo()

    val topics = (0 until numTopic).map(_ => newTopic())
    val stream = see.addSource(new MutiTopicSource(topics, numElements))

    val sinkProp = sinkProperties()
    sinkProp.setProperty(FLUSH_ON_CHECKPOINT, "true")
    sinkProp.setProperty(CLIENT_CACHE_SIZE, "7")
    stream.addSink(new AssertSink(7, intRowWithTopicType(), sinkProp))
    see.execute("write with topics")
  }

  test("produce consume multiple topics") {
    val numTopic = 5
    val numElements = 20

    val see = StreamExecutionEnvironment.getExecutionEnvironment
    see.getConfig.disableSysoutLogging()
    see.setParallelism(1)

    implicit val tpe = intRowWithTopicTypeInfo()

    val topics = (0 until numTopic).map(_ => newTopic())
    val stream = see.addSource(new MutiTopicSource(topics, numElements))

    val sinkProp = sinkProperties()
    produceIntoPulsar(stream, intRowWithTopicType(), sinkProp)
    see.execute("write with topics")

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.getConfig.disableSysoutLogging()

    val sourceProps = sourceProperties()
    sourceProps.setProperty(TOPIC_MULTI, topics.mkString(","))
    val stream1 = env.addSource(new FlinkPulsarSource(sourceProps))

    stream1.flatMap(
      new CountMessageNumberFM(numElements))(new TypeHint[Int](){}.getTypeInfo)
      .setParallelism(1)

    TestUtils.tryExecute(env.getJavaEnv, "count elements from topics")
  }

  test("commit offsets to pulsar") {
    val topics = (0 until 3).map(_ => newTopic())
    val messages = (0 until 50)
    val expectedIds = topics.map { tp =>
      val lid = sendTypedMessages[Int](tp, SchemaType.INT32, messages, None).last
      tp -> lid
    }.toMap

    val see = StreamExecutionEnvironment.getExecutionEnvironment
    see.getConfig.disableSysoutLogging()
    see.setParallelism(3)
    see.enableCheckpointing(200)

    val subName = UUID.randomUUID().toString

    val sourceProps = sourceProperties()
    sourceProps.setProperty(TOPIC_MULTI, topics.mkString(","))

    // Sink FlinkPulsarSource is ResultTypeQueryable, we don't need to provide an extra TypeInfo
    val stream = see.addSource(new FlinkPulsarSourceSub(sourceProps, subName))(null)

    stream.addSink(new DiscardingSink[Row]())

    val errorRef = new AtomicReference[Throwable]
    val runner = new Thread("runner") {
      override def run(): Unit = {
        try {
          see.execute
        } catch {
          case t: Throwable =>
            if (!t.isInstanceOf[JobCancellationException]) errorRef.set(t)
        }
      }
    }
    runner.start()

    Thread.sleep(3000)

    val deadline = 30000000000L + System.nanoTime

    var gotLast = false
    do {
      val ids = getCommittedOffsets(topics.toSet, subName)
      if (roughEquals(ids, expectedIds)) {
        gotLast = true
      } else {
        Thread.sleep(100)
      }
    } while (System.nanoTime() < deadline && !gotLast)

    // cancel the job & wait for the job to finish
    flinkClient.cancel(Iterables.getOnlyElement(getRunningJobs(flinkClient).asJava))
    runner.join()

    val t = errorRef.get
    if (t != null) throw new RuntimeException("Job failed with an exception", t)

    assert(gotLast)
  }

  test("start from earliest") {
    val topics = (0 until 3).map(_ => newTopic())
    val messages = (0 until 50)
    val expectedData = topics.map { tp =>
      val lid = sendTypedMessages[Int](tp, SchemaType.INT32, messages, None).last
      tp -> messages.toSet
    }.toMap

    val see = StreamExecutionEnvironment.getExecutionEnvironment
    see.getConfig.disableSysoutLogging()
    see.setParallelism(3)

    val sourceProps = sourceProperties()
    sourceProps.setProperty(TOPIC_MULTI, topics.mkString(","))

    val source = new FlinkPulsarSource(sourceProps)
    val stream = see.addSource(source)(null)

    // we pass in an arbitrary TypeInfo since it collects nothing
    val x = stream.flatMap(new CheckAllMessageIdExist(expectedData, 150))(intRowTypeInfo())
    x.setParallelism(1)

    TestUtils.tryExecute(see.getJavaEnv, "start from earliest")
  }

  test("start from latest") {
    val topics = (0 until 3).map(_ => newTopic())
    val messages = (0 until 50)
    val newMessages = (50 until 60)
    topics.map { tp =>
      val lid = sendTypedMessages[Int](tp, SchemaType.INT32, messages, None).last
      tp -> messages.toSet
    }

    val see = StreamExecutionEnvironment.getExecutionEnvironment
    see.getConfig.disableSysoutLogging()
    see.setParallelism(3)

    val sourceProps = sourceProperties()
    sourceProps.setProperty(TOPIC_MULTI, topics.mkString(","))
    sourceProps.setProperty(STARTING_OFFSETS_OPTION_KEY, "latest")

    val source = new FlinkPulsarSource(sourceProps)

    val stream = see.addSource(source)(null)

    val expectedData = topics.map { tp =>
      tp -> newMessages.toSet
    }.toMap

    // we pass in an arbitrary TypeInfo since it collects nothing
    val x = stream.flatMap(new CheckAllMessageIdExist(expectedData, 30))(intRowTypeInfo())
    x.setParallelism(1)
    x.addSink(new DiscardingSink[Row])

    val jobGraph = StreamingJobGraphGenerator.createJobGraph(see.getStreamGraph)
    val consumeJobId = jobGraph.getJobID

    val error = new AtomicReference[Throwable]()
    val consumeThread = new Thread(new Runnable() {
      override def run(): Unit = {
        try {
          flinkClient.setDetached(false)
          flinkClient.submitJob(jobGraph, classOf[FlinkPulsarITest].getClassLoader)
        } catch {
          case t: Throwable =>
            if (!ExceptionUtils.findThrowable(t, classOf[JobCancellationException]).isPresent) error.set(t)
        }
      }
    })
    consumeThread.start()
    waitUntilJobIsRunning(flinkClient)

    Thread.sleep(3000)

    val extraProduceThread = new Thread(new Runnable {
      override def run(): Unit = {
        topics.foreach { tp =>
          sendTypedMessages[Int](tp, SchemaType.INT32, newMessages, None)
        }
      }
    })
    extraProduceThread.start()

    consumeThread.join()
    // check whether the consuming thread threw any test errors;
    // test will fail here if the consume job had incorrectly read any records other than the extra records
    val consumerError = error.get
    assert(causedBySpecificException[SuccessException](consumerError))
  }

  test("start from specific") {
    val topic = newTopic()
    val mids = sendTypedMessages[Int](
      topic,
      SchemaType.INT32,
      Array(
        //  0,   1,   2, 3, 4, 5,  6,  7,  8
           -20, -21, -22, 1, 2, 3, 10, 11, 12),
      None)

    val s1 = JsonUtils.topicOffsets(Map(topic -> mids(3)))

    val expectedData = Map(topic -> Set(2, 3, 10, 11, 12))

    val see = StreamExecutionEnvironment.getExecutionEnvironment
    see.getConfig.disableSysoutLogging()
    see.setParallelism(1)

    val sourceProps = sourceProperties()
    sourceProps.setProperty(TOPIC_SINGLE, topic)
    sourceProps.setProperty(STARTING_OFFSETS_OPTION_KEY, s1)

    val source = new FlinkPulsarSource(sourceProps)
    val stream = see.addSource(source)(null)

    val x = stream.flatMap(new CheckAllMessageIdExist(expectedData, 5))(intRowTypeInfo())
    x.setParallelism(1)

    TestUtils.tryExecute(see.getJavaEnv, "start from specific")
  }

  test("one to one exactly once") {

    val topic = newTopic()
    val parallelism = 5
    val numElementsPerPartition = 1000
    val totalElements = parallelism * numElementsPerPartition
    val failAfterElements = numElementsPerPartition / 3

    val allTopicNames = (0 until parallelism).map(i => s"$topic-partition-$i")

    createTopic(topic, parallelism)

    generateRandomizedIntegerSequence(
      StreamExecutionEnvironment.getExecutionEnvironment,
      topic, parallelism, numElementsPerPartition, true)

    // run the topology that fails and recovers

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(500)
    env.setParallelism(parallelism)
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 0))
    env.getConfig.disableSysoutLogging()

    val sourceProps = sourceProperties()
    sourceProps.setProperty(TOPIC_MULTI, allTopicNames.mkString(","))

    val source = new FlinkPulsarSource(sourceProps)
    implicit val tpe = source.getProducedType
    env.addSource(source)
      .map(new PartitionValidatorMapper(parallelism, 1))
      .map(new FailingIdentityMapper[Row](failAfterElements))
      .addSink(new ValidatingExactlyOnceSink(totalElements)).setParallelism(1)

    FailingIdentityMapper.failedBefore = false
    TestUtils.tryExecute(env.getJavaEnv, "One-to-one exactly once test")
  }

  test("one source multiple topics") {

    val topic = newTopic()
    val numPartitions = 5
    val numElementsPerPartition = 1000
    val totalElements = numPartitions * numElementsPerPartition
    val failAfterElements = numElementsPerPartition / 3

    val allTopicNames = (0 until numPartitions).map(i => s"$topic-partition-$i")

    createTopic(topic, numPartitions)

    generateRandomizedIntegerSequence(
      StreamExecutionEnvironment.getExecutionEnvironment,
      topic, numPartitions, numElementsPerPartition, true)

    val parallelism = 2

    // run the topology that fails and recovers

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(500)
    env.setParallelism(parallelism)
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 0))
    env.getConfig.disableSysoutLogging()

    val sourceProps = sourceProperties()
    sourceProps.setProperty(TOPIC_SINGLE, topic)

    val source = new FlinkPulsarSource(sourceProps)
    implicit val tpe = source.getProducedType
    env.addSource(source)
      .map(new PartitionValidatorMapper(numPartitions, 3))
      .map(new FailingIdentityMapper[Row](failAfterElements))
      .addSink(new ValidatingExactlyOnceSink(totalElements)).setParallelism(1)

    FailingIdentityMapper.failedBefore = false
    TestUtils.tryExecute(env.getJavaEnv, "One-source-multi-partitions exactly once test")
  }

  test("source task number > partition number") {

    val topic = newTopic()
    val numPartitions = 5
    val numElementsPerPartition = 1000
    val totalElements = numPartitions * numElementsPerPartition
    val failAfterElements = numElementsPerPartition / 3

    val allTopicNames = (0 until numPartitions).map(i => s"$topic-partition-$i")

    createTopic(topic, numPartitions)

    generateRandomizedIntegerSequence(
      StreamExecutionEnvironment.getExecutionEnvironment,
      topic, numPartitions, numElementsPerPartition, true)

    val parallelism = 8

    // run the topology that fails and recovers

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(500)
    env.setParallelism(parallelism)
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 0))
    env.getConfig.disableSysoutLogging()
    env.setBufferTimeout(0)

    val sourceProps = sourceProperties()
    sourceProps.setProperty(TOPIC_MULTI, allTopicNames.mkString(","))

    val source = new FlinkPulsarSource(sourceProps)
    implicit val tpe = source.getProducedType
    env.addSource(source)
      .map(new PartitionValidatorMapper(numPartitions, 1))
      .map(new FailingIdentityMapper[Row](failAfterElements))
      .addSink(new ValidatingExactlyOnceSink(totalElements)).setParallelism(1)

    FailingIdentityMapper.failedBefore = false
    TestUtils.tryExecute(env.getJavaEnv, "source task number > partition number")
  }

  test("canceling on full input") {
    val tp = newTopic()
    val parallelism = 3
    createTopic(tp, parallelism)

    val generator = new InfiniteStringsGenerator(tp)
    generator.start()

    // launch a consumer asynchronously
    val jobError = new AtomicReference[Throwable]()

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(parallelism)
    env.enableCheckpointing(100)
    env.getConfig.disableSysoutLogging()

    val prop = sourceProperties()
    prop.setProperty(TOPIC_SINGLE, tp)
    val source = new FlinkPulsarSource(prop)


    env.addSource(source)(null).addSink(new DiscardingSink[Row]())

    val jobGraph = StreamingJobGraphGenerator.createJobGraph(env.getStreamGraph)
    val jobId = jobGraph.getJobID

    val jobRunner = new Runnable {
      override def run(): Unit = {
        try {
          flinkClient.setDetached(false)
          flinkClient.submitJob(jobGraph, getClass.getClassLoader)
        } catch {
          case e: Throwable =>
            jobError.set(e)
        }
      }
    }

    val runnerThread = new Thread(jobRunner, "program runner thread")
    runnerThread.start()

    Thread.sleep(2000)
    val failureCause = jobError.get()

    if (failureCause != null) {
      failureCause.printStackTrace()
      fail("Test failed prematurely with: " + failureCause.getMessage)
    }

    // cancel
    flinkClient.cancel(jobId)

    // wait for the program to be done and validate that we failed with the right exception
    runnerThread.join()

    assert(flinkClient.getJobStatus(jobId).get().toString == JobStatus.CANCELED.toString)
    if (generator.isAlive) {
      generator.shutdown()
      generator.join()
    } else {
      val t = generator.getError
      if (t != null) {
        t.printStackTrace()
        fail(s"Generator failed ${t.getMessage}")
      } else {
        fail("Generator failed with no exception")
      }
    }
  }

  test("canceling on empty input") {
    val tp = newTopic()
    val parallelism = 3
    createTopic(tp, parallelism)

    // launch a consumer asynchronously
    val jobError = new AtomicReference[Throwable]()

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(parallelism)
    env.enableCheckpointing(100)
    env.getConfig.disableSysoutLogging()

    val prop = sourceProperties()
    prop.setProperty(TOPIC_SINGLE, tp)
    val source = new FlinkPulsarSource(prop)

    env.addSource(source)(null).addSink(new DiscardingSink[Row]())

    val jobGraph = StreamingJobGraphGenerator.createJobGraph(env.getStreamGraph)
    val jobId = jobGraph.getJobID

    val jobRunner = new Runnable {
      override def run(): Unit = {
        try {
          flinkClient.setDetached(false)
          flinkClient.submitJob(jobGraph, getClass.getClassLoader)
        } catch {
          case e: Throwable =>
            jobError.set(e)
        }
      }
    }

    val runnerThread = new Thread(jobRunner, "program runner thread")
    runnerThread.start()

    Thread.sleep(2000)
    val failureCause = jobError.get()

    if (failureCause != null) {
      failureCause.printStackTrace()
      fail("Test failed prematurely with: " + failureCause.getMessage)
    }

    // cancel
    flinkClient.cancel(jobId)

    // wait for the program to be done and validate that we failed with the right exception
    runnerThread.join()

    assert(flinkClient.getJobStatus(jobId).get().toString == JobStatus.CANCELED.toString)
  }

  test("partition number grows during sink") {
    import org.apache.flink.streaming.api.scala._

    val inputTp = newTopic()
    createTopic(inputTp, 3)

    val outTP = newTopic()
    val originParallelism = 3
    val changdParallelism = 5
    createTopic(outTP, originParallelism)

    val messages = (0 until 50)
    sendTypedMessages[Int](inputTp, SchemaType.INT32, messages, None)

    val see = StreamExecutionEnvironment.getExecutionEnvironment
    see.getConfig.disableSysoutLogging()
    see.setParallelism(3)

    val sourceProps = sourceProperties()
    sourceProps.setProperty(TOPIC_SINGLE, inputTp)

    val sinkProp = sinkProperties()
    sinkProp.setProperty(FLUSH_ON_CHECKPOINT, "true")
    sinkProp.setProperty(TOPIC_SINGLE, outTP)
    sinkProp.setProperty("pulsar.producer.batchingEnabled", "false")

    val source = new FlinkPulsarSource(sourceProps)
    val stream = see.addSource(source)

    stream.addSink(new FlinkPulsarRowSink(intRowType(), sinkProp))

    // launch a consumer asynchronously
    val jobError = new AtomicReference[Throwable]()

    val jobGraph = StreamingJobGraphGenerator.createJobGraph(see.getStreamGraph)
    val jobId = jobGraph.getJobID

    val jobRunner = new Runnable {
      override def run(): Unit = {
        try {
          flinkClient.setDetached(false)
          flinkClient.submitJob(jobGraph, getClass.getClassLoader)
        } catch {
          case e: Throwable =>
            jobError.set(e)
        }
      }
    }

    val runnerThread = new Thread(jobRunner, "program runner thread")
    runnerThread.start()

    Thread.sleep(2000)

    addPartitions(outTP, changdParallelism)

    val messages2 = (51 until 100)
    sendTypedMessages[Int](inputTp, SchemaType.INT32, messages2, None)

    val failureCause = jobError.get()

    if (failureCause != null) {
      failureCause.printStackTrace()
      fail("Test failed prematurely with: " + failureCause.getMessage)
    }

    val sourceProps1 = sourceProperties()
    sourceProps1.setProperty(TOPIC_SINGLE, s"$outTP-partition-4")

    val see1 = StreamExecutionEnvironment.getExecutionEnvironment
    see1.getConfig.disableSysoutLogging()
    see1.setParallelism(1)

    val source1 = new FlinkPulsarSource(sourceProps1)

    see1.addSource(source1)
      .map(new FailingIdentityMapper[Row](1))
      .addSink(new DiscardingSink[Row]())

    TestUtils.tryExecute(see1.getJavaEnv, "Assert new partition got data")

    // cancel
    flinkClient.cancel(jobId)

    // wait for the program to be done and validate that we failed with the right exception
    runnerThread.join()

    assert(flinkClient.getJobStatus(jobId).get().toString == JobStatus.CANCELED.toString)

  }

  def generateRandomizedIntegerSequence(
    env: StreamExecutionEnvironment,
    tp: String,
    numPartitions: Int,
    numElements: Int,
    randomizedOrder: Boolean): Unit = {

    env.setParallelism(numPartitions)
    env.getConfig.disableSysoutLogging()
    env.setRestartStrategy(RestartStrategies.noRestart())

    implicit val tpe = intRowWithTopicTypeInfo()
    val stream = env.addSource(
      new RandomizedIntegerSeq(tp, numPartitions, numElements, randomizedOrder))

    produceIntoPulsar(stream, intRowWithTopicType(), sinkProperties())
    env.execute("scrambles in sequence generator")
  }


  def roughEquals(a: Map[String, MessageId], b: Map[String, MessageId]): Boolean = {
    a.foreach { case (k, v) =>
      val bmid = b.getOrElse(k, MessageId.latest)
      if (!SourceSinkUtils.messageIdRoughEquals(v, bmid)) {
        return false
      }
    }
    true
  }

  def produceIntoPulsar(stream: DataStream[Row], schema: DataType, props: Properties): Unit = {
    props.setProperty(FLUSH_ON_CHECKPOINT, "true")
    stream.addSink(new FlinkPulsarRowSink(schema, props))
  }

  def causedBySpecificException[T](e: Throwable, messagePart: String): Boolean = {
    if (e.isInstanceOf[T] && e.getMessage.contains(messagePart)) {
        true
    } else {
      if (e.getCause == null) {
        return false
      }
      causedBySpecificException(e.getCause, messagePart)
    }
  }

  def causedBySpecificException[T](e: Throwable): Boolean = {
    if (e.isInstanceOf[T]) {
      true
    } else {
      if (e.getCause == null) {
        return false
      }
      causedBySpecificException(e.getCause)
    }
  }

  def intRowWithTopic(i: Int, tp: String): Row = {
    val r = new Row(2)
    r.setField(0, tp)
    r.setField(1, i)
    r
  }

  def intPulsarRow(): TypeInformation[Row] = {
    val dt = DataTypes.ROW(
      DataTypes.FIELD("value", DataTypes.INT()),
      DataTypes.FIELD(
        KEY_ATTRIBUTE_NAME,
        DataTypes.BYTES()),
      DataTypes.FIELD(
        TOPIC_ATTRIBUTE_NAME,
        DataTypes.STRING()),
      DataTypes.FIELD(
        MESSAGE_ID_NAME,
        DataTypes.BYTES()),
      DataTypes.FIELD(
        PUBLISH_TIME_NAME,
        DataTypes.TIMESTAMP(3).bridgedTo(classOf[java.sql.Timestamp])),
      DataTypes.FIELD(
        EVENT_TIME_NAME,
        DataTypes.TIMESTAMP(3).bridgedTo(classOf[java.sql.Timestamp]))
    )
    LegacyTypeInfoDataTypeConverter
      .toLegacyTypeInfo(dt)
      .asInstanceOf[TypeInformation[Row]]
  }

  def intRowType(): DataType = {
    DataTypes.ROW(
      DataTypes.FIELD("v", DataTypes.INT())
    )
  }

  def intRowTypeInfo(): TypeInformation[Row] = {
    LegacyTypeInfoDataTypeConverter
      .toLegacyTypeInfo(intRowType())
      .asInstanceOf[TypeInformation[Row]]
  }

  def intRowWithTopicType(): DataType = {
    DataTypes.ROW(
      DataTypes.FIELD(TOPIC_ATTRIBUTE_NAME, DataTypes.STRING()),
      DataTypes.FIELD("v", DataTypes.INT())
    )
  }

  def intRowWithTopicTypeInfo(): TypeInformation[Row] = {
    LegacyTypeInfoDataTypeConverter
      .toLegacyTypeInfo(intRowWithTopicType())
      .asInstanceOf[TypeInformation[Row]]
  }

  def sinkProperties(): Properties = {
    val prop = new Properties()
    prop.setProperty(SERVICE_URL_OPTION_KEY, serviceUrl)
    prop.setProperty(ADMIN_URL_OPTION_KEY, adminUrl)
    prop.setProperty(FLUSH_ON_CHECKPOINT, "true")
    prop.setProperty(FAIL_ON_WRITE, "true")
    prop
  }

  def sourceProperties(): Properties = {
    val prop = new Properties()
    prop.setProperty(SERVICE_URL_OPTION_KEY, serviceUrl)
    prop.setProperty(ADMIN_URL_OPTION_KEY, adminUrl)
    prop.setProperty(PARTITION_DISCOVERY_INTERVAL_MS, "5000")
    prop.setProperty(STARTING_OFFSETS_OPTION_KEY, "earliest")
    prop
  }

  private val topicId = new AtomicInteger(0)

  private def newTopic(): String = TopicName.get(s"topic-${topicId.getAndIncrement()}").toString

  class InfiniteStringsGenerator(tp: String) extends Thread {
    @volatile var running: Boolean = true
    @volatile var error: Throwable = null

    override def run(): Unit = {
      // we manually feed data into the Pulsar sink

      try {
        val prop = sinkProperties()
        prop.setProperty(TOPIC_SINGLE, tp)
        prop.setProperty(FLUSH_ON_CHECKPOINT, "true")

        val ps = new FlinkPulsarSinkBase[String](prop, null) {
          override def pulsarSchema: Schema[_] = Schema.STRING
        }
        val sink = new StreamSink[String](ps)

        val testHarness = new OneInputStreamOperatorTestHarness[String, Object](sink)

        testHarness.open()

        val bld = new StringBuilder
        val rnd = new Random

        while (running) {
          bld.setLength(0)
          val len = rnd.nextInt(100) + 1
          var i = 0
          while (i < len) {
            bld.append((rnd.nextInt(20) + 'a').toChar)

            i += 1
          }

          val next = bld.toString
          testHarness.processElement(new StreamRecord[String](next))
        }
      } catch {
        case e: Throwable =>
          this.error = e
      }
    }

    def shutdown(): Unit = {
      this.running = false
      this.interrupt()
    }

    def getError: Throwable = this.error
  }
}

class AssertSink(cacheSize: Int, schema: DataType, props: Properties)
    extends FlinkPulsarRowSink(schema, props) {
  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    assert(CachedPulsarClient.getCacheSize() == cacheSize)
  }
}

class CheckAllMessageIdExist(expected: Map[String, Set[Int]], total: Int)
    extends RichFlatMapFunction[Row, Row] {

  val map = mutable.HashMap.empty[String, Seq[Int]]
  var count = 0

  override def flatMap(in: Row, collector: Collector[Row]): Unit = {
    val tp = in.getField(2).asInstanceOf[String]
    val value = in.getField(0).asInstanceOf[Int]
    val current = map.getOrElse(tp, Seq.empty[Int])
    map.put(tp, value +: current)
    count += 1

    if (count == total) {
      map.foreach { case (tp, seq) =>
        val s = seq.toSet
        if (s.size != seq.size) {
          throw new RuntimeException(s"duplicate elements in $tp: $seq")
        }
        val expectedSet = expected.getOrElse(tp, null)
        if (expectedSet == null) {
          throw new RuntimeException("Unknown topic seen $tp")
        } else {
          if (expectedSet != s) {
            throw new RuntimeException(s"$expectedSet \n $s")
          }
        }
      }
      throw new SuccessException
    }
  }
}

class MutiTopicSource(topics: Seq[String], numElements: Int, base: Int)
    extends RichParallelSourceFunction[Row] {
  def this(topics: Seq[String], numElements: Int) = this(topics, numElements, 0)
  override def run(ctx: SourceFunction.SourceContext[Row]): Unit = {
    topics.foreach { tp =>
      (0 until numElements).foreach { eid =>
        ctx.collect(intRowWithTopic(eid + base, tp))
      }
    }
  }

  override def cancel(): Unit = {}

  def intRowWithTopic(i: Int, tp: String): Row = {
    val r = new Row(2)
    r.setField(0, tp)
    r.setField(1, i)
    r
  }
}

class CountMessageNumberFM(numElements: Int) extends FlatMapFunction[Row, Int] {
  val map = mutable.HashMap.empty[String, Int]
  override def flatMap(t: Row, collector: Collector[Int]): Unit = {
    val tp = t.getField(2).asInstanceOf[String]
    val old = map.getOrElse(tp, 0)
    map.put(tp, old + 1)

    for (elem <- map) {
      if (elem._2 < numElements) {
        return
      } else if (elem._2 > numElements) {
        throw new RuntimeException(s"${elem._1} has ${elem._2} elements")
      }
    }
    throw new SuccessException
  }
}

class FlinkPulsarSourceSub(parameters: Properties, sub: String) extends FlinkPulsarSource(parameters) {
  @transient override lazy val subscriptionPrefix: String = s"flink-pulsar-$sub"
}

class RandomizedIntegerSeq(tp: String, numPartitions: Int, numElements: Int, randomizeOrder: Boolean) extends RichParallelSourceFunction[Row] {

  @volatile var running = true

  override def run(sourceContext: SourceFunction.SourceContext[Row]): Unit = {
    val subIndex = getRuntimeContext.getIndexOfThisSubtask
    val allNum = getRuntimeContext.getNumberOfParallelSubtasks

    val rows = (0 until numElements).map { elem =>
      intRowWithTopic(subIndex + elem * allNum, topicName(tp, subIndex))
    }.toArray

    if (randomizeOrder) {
      val rand = new Random()
      (0 until numElements).foreach { i =>
        val otherPos = rand.nextInt(numElements)
        val tmp = rows(i)
        rows(i) = rows(otherPos)
        rows(otherPos) = tmp
      }
    }

    rows.foreach(sourceContext.collect)
  }

  override def cancel(): Unit = {
    running = false
  }

  def topicName(tp: String, index: Int): String = {
    s"$tp-partition-$index"
  }

  def intRowWithTopic(i: Int, tp: String): Row = {
    val r = new Row(2)
    r.setField(0, tp)
    r.setField(1, i)
    r
  }
}

class PartitionValidatorMapper(numPartitions: Int, maxPartitions: Int)
    extends MapFunction[Row, Row]{
  val myTopics = mutable.HashSet.empty[String]

  override def map(t: Row): Row = {
    val tp = t.getField(2).asInstanceOf[String]
    myTopics.add(tp)
    if (myTopics.size > maxPartitions) {
      throw new Exception(s"Error: Elements from too many different partitions: $myTopics " +
        s"Expect elements only from $maxPartitions partitions")
    }
    t
  }
}

class MockTransformation extends Transformation[String](
    "MockTransformation", BasicTypeInfo.STRING_TYPE_INFO, 1) {

  override def getTransitivePredecessors() = null
}

class DummyStreamExecutionEnvironment extends JavaEnv {

  override def execute(streamGraph: StreamGraph): JobExecutionResult = null
}
