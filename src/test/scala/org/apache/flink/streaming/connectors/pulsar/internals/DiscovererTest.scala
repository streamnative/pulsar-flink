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

import org.apache.flink.pulsar.{PulsarFunSuite, PulsarMetadataReader}
import org.scalatest.mockito.MockitoSugar
import org.mockito.Mockito._
import org.mockito.ArgumentMatchers._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer

class DiscovererTest extends PulsarFunSuite {
  val TEST_TOPIC = "test-topic"
  val TEST_TOPIC_PATTERN = "^" + TEST_TOPIC + "[0-9]*$"

  val mapTp = Map("topic" -> TEST_TOPIC)
  val mapPattern = Map("topicspattern" -> TEST_TOPIC_PATTERN)

  for (mp <- Seq(mapTp, mapPattern)) {

    test(s"partition number == consumer number $mp") {
      val mockAllTopics = Set(
        topicName(TEST_TOPIC, 0),
        topicName(TEST_TOPIC, 1),
        topicName(TEST_TOPIC, 2),
        topicName(TEST_TOPIC, 3))

      val numSubtasks = mockAllTopics.size

      var subtaskIndex = 0
      while (subtaskIndex < mockAllTopics.size) {
        val discoverer =
          new TestMetadataReader(mp, subtaskIndex, numSubtasks,
            TestMetadataReader.createMockGetAllTopicsSequenceFromFixedReturn(mockAllTopics))

        val initials = discoverer.discoverTopicsChange()
        assert(1 == initials.size)
        assert(initials.subsetOf(mockAllTopics))
        assert(
          TestMetadataReader.getExpectedSubtaskIndex(initials.toSeq(0), numSubtasks)
            == subtaskIndex)

        val second = discoverer.discoverTopicsChange()
        val third = discoverer.discoverTopicsChange()

        assert(0 == second.size)
        assert(0 == third.size)
        subtaskIndex += 1
      }
    }

    test(s"partition num > consumer num $mp") {
      val mockAllTopics = (0 to 10).map(topicName(TEST_TOPIC, _)).toSet
      var allTopics = (0 to 10).map(topicName(TEST_TOPIC, _)).toSet

      val numTasks = 3
      val minPartitionsPerTask = mockAllTopics.size / numTasks
      val maxPartitionsPerTask = mockAllTopics.size / numTasks + 1

      var subtaskIndex = 0
      while (subtaskIndex < numTasks) {
        val discoverer = new TestMetadataReader(mp, subtaskIndex, numTasks,
          TestMetadataReader.createMockGetAllTopicsSequenceFromFixedReturn(mockAllTopics))

        val initials = discoverer.discoverTopicsChange()
        val isize = initials.size
        assert(isize >= minPartitionsPerTask && isize <= maxPartitionsPerTask)

        initials.foreach { tp =>
          assert(allTopics.contains(tp))
          assert(TestMetadataReader.getExpectedSubtaskIndex(tp, numTasks) == subtaskIndex)
          allTopics -= tp
        }

        val second = discoverer.discoverTopicsChange()
        val third = discoverer.discoverTopicsChange()

        assert(0 == second.size)
        assert(0 == third.size)

        subtaskIndex += 1
      }
      assert(allTopics.isEmpty)
    }

    test(s"partition num < consumer num $mp") {
      val mockAllTopics = (0 to 3).map(topicName(TEST_TOPIC, _)).toSet
      var allTopics = (0 to 3).map(topicName(TEST_TOPIC, _)).toSet

      val numTasks = 2 * mockAllTopics.size

      var subIndex = 0
      while (subIndex < numTasks) {
        val discoverer = new TestMetadataReader(mp, subIndex, numTasks,
          TestMetadataReader.createMockGetAllTopicsSequenceFromFixedReturn(mockAllTopics))

        val initials = discoverer.discoverTopicsChange()
        assert(initials.size <= 1)

        initials.foreach { tp =>
          assert(allTopics.contains(tp))
          assert(TestMetadataReader.getExpectedSubtaskIndex(tp, numTasks) == subIndex)
          allTopics -= tp
        }

        val second = discoverer.discoverTopicsChange()
        val third = discoverer.discoverTopicsChange()

        assert(0 == second.size)
        assert(0 == third.size)

        subIndex += 1
      }
      assert(allTopics.isEmpty)
    }

    test(s"growing partitions $mp") {
      val mockAllTopics = (0 to 10).map(topicName(TEST_TOPIC, _)).toSet
      var allTopics = (0 to 10).map(topicName(TEST_TOPIC, _)).toSet

      val initial = mockAllTopics.toSeq.slice(0, 7).toSet
      var initialAll = mockAllTopics.toSeq.slice(0, 7).toSet

      val mockGet = Seq(initial, mockAllTopics)

      val numTasks = 3
      val minInitialPartitionsPerConsumer = initial.size / numTasks
      val maxInitialPartitionsPerConsumer = initial.size / numTasks + 1
      val minAll = allTopics.size / numTasks
      val maxAll = allTopics.size / numTasks + 1

      val discover1 = new TestMetadataReader(mp, 0, numTasks,
        TestMetadataReader.createMockGetAllTopicsSequenceFromTwoReturns(mockGet))

      val discover2 = new TestMetadataReader(mp, 1, numTasks,
        TestMetadataReader.createMockGetAllTopicsSequenceFromTwoReturns(mockGet))

      val discover3 = new TestMetadataReader(mp, 2, numTasks,
        TestMetadataReader.createMockGetAllTopicsSequenceFromTwoReturns(mockGet))

      val initials1 = discover1.discoverTopicsChange()
      val initials2 = discover2.discoverTopicsChange()
      val initials3 = discover3.discoverTopicsChange()

      assert(initials1.size >= minInitialPartitionsPerConsumer
        && initials1.size <= maxInitialPartitionsPerConsumer)
      assert(initials2.size >= minInitialPartitionsPerConsumer
        && initials2.size <= maxInitialPartitionsPerConsumer)
      assert(initials3.size >= minInitialPartitionsPerConsumer
        && initials3.size <= maxInitialPartitionsPerConsumer)

      initials1.foreach { tp =>
        assert(initialAll.contains(tp))
        assert(TestMetadataReader.getExpectedSubtaskIndex(tp, numTasks) == 0)
        initialAll -= tp
      }

      initials2.foreach { tp =>
        assert(initialAll.contains(tp))
        assert(TestMetadataReader.getExpectedSubtaskIndex(tp, numTasks) == 1)
        initialAll -= tp
      }

      initials3.foreach { tp =>
        assert(initialAll.contains(tp))
        assert(TestMetadataReader.getExpectedSubtaskIndex(tp, numTasks) == 2)
        initialAll -= tp
      }

      assert(initialAll.isEmpty)

      val seconds1 = discover1.discoverTopicsChange()
      val seconds2 = discover2.discoverTopicsChange()
      val seconds3 = discover3.discoverTopicsChange()

      assert((seconds1 & initials1).isEmpty)
      assert((seconds2 & initials2).isEmpty)
      assert((seconds3 & initials3).isEmpty)

      assert(initials1.size + seconds1.size >= minAll
        && initials1.size + seconds1.size <= maxAll)
      assert(initials2.size + seconds2.size >= minAll
        && initials2.size + seconds2.size <= maxAll)
      assert(initials3.size + seconds3.size >= minAll
        && initials3.size + seconds3.size <= maxAll)

      initials1.foreach { tp =>
        assert(allTopics.contains(tp))
        assert(TestMetadataReader.getExpectedSubtaskIndex(tp, numTasks) == 0)
        allTopics -= tp
      }

      initials2.foreach { tp =>
        assert(allTopics.contains(tp))
        assert(TestMetadataReader.getExpectedSubtaskIndex(tp, numTasks) == 1)
        allTopics -= tp
      }

      initials3.foreach { tp =>
        assert(allTopics.contains(tp))
        assert(TestMetadataReader.getExpectedSubtaskIndex(tp, numTasks) == 2)
        allTopics -= tp
      }

      seconds1.foreach { tp =>
        assert(allTopics.contains(tp))
        assert(TestMetadataReader.getExpectedSubtaskIndex(tp, numTasks) == 0)
        allTopics -= tp
      }

      seconds2.foreach { tp =>
        assert(allTopics.contains(tp))
        assert(TestMetadataReader.getExpectedSubtaskIndex(tp, numTasks) == 1)
        allTopics -= tp
      }

      seconds3.foreach { tp =>
        assert(allTopics.contains(tp))
        assert(TestMetadataReader.getExpectedSubtaskIndex(tp, numTasks) == 2)
        allTopics -= tp
      }

      assert(allTopics.isEmpty)
    }
  }
}

class TestMetadataReader(
    caseInsensitiveMap: Map[String, String],
    indexOfThisSubtask: Int,
    numParallelSubtasks: Int,
    mockGetAllTopicsReturnSequence: Seq[Set[String]])
  extends PulsarMetadataReader("", null, "",
    caseInsensitiveMap, indexOfThisSubtask, numParallelSubtasks) {

  var getAllTopicsInvCount = 0

  override def getTopicPartitionsAll(): Set[String] = {
    val out = mockGetAllTopicsReturnSequence(getAllTopicsInvCount)
    getAllTopicsInvCount += 1
    out
  }
}

object TestMetadataReader extends MockitoSugar {
  def createMockGetAllTopicsSequenceFromFixedReturn(fixed: Set[String]) = {
    val mockSequence: Seq[Set[String]] = mock[Seq[Set[String]]]
    when(mockSequence(anyInt())).thenAnswer(new Answer[Set[String]] {
      override def answer(
        invocation: InvocationOnMock): Set[String] = fixed
    })
    mockSequence
  }

  def createMockGetAllTopicsSequenceFromTwoReturns(fixed: Seq[Set[String]]) = {
    val mockSequence: Seq[Set[String]] = mock[Seq[Set[String]]]
    when(mockSequence(0)).thenAnswer(new Answer[Set[String]] {
      override def answer(
        invocation: InvocationOnMock): Set[String] = fixed(0)
    })
    when(mockSequence(1)).thenAnswer(new Answer[Set[String]] {
      override def answer(
        invocation: InvocationOnMock): Set[String] = fixed(1)
    })
    mockSequence
  }

  def getExpectedSubtaskIndex(tp: String, numTasks: Int) = {
    ((tp.hashCode * 31) & 0x7FFFFFFF) % numTasks
  }
}