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

import org.scalatest.{BeforeAndAfterEach, FunSuite}

import org.apache.pulsar.client.api.MessageId
import org.apache.pulsar.client.impl.{BatchMessageIdImpl, MessageIdImpl}

class JsonUtilsTestSuite extends PulsarFunSuite with BeforeAndAfterEach {

  test("serialize and deserialize topics") {
    val topics: Array[String] = 0
      .to(9)
      .map { i =>
        s"topic-${i}"
      }
      .toArray

    val str = JsonUtils.topics(topics)
    val readTopics = JsonUtils.topics(str).toSeq.sorted.toArray

    assert(10 == readTopics.size)
    0.to(9) map { i =>
      {
        assert(s"topic-${i}" == readTopics(i))
      }
    }
  }

  test("serialize and deserialize topic earliest offsets") {
    val topicOffsets: Map[String, MessageId] = 0
      .to(9)
      .map { i =>
        (s"topic-${i}", MessageId.earliest)
      }
      .toMap

    val str = JsonUtils.topicOffsets(topicOffsets)
    val readTopicOffsets = JsonUtils.topicOffsets(str)

    assert(10 == readTopicOffsets.size)
    0.to(9) map { i =>
      {
        val topic = s"topic-${i}"
        val offset = readTopicOffsets(topic)
        assert(MessageId.earliest.compareTo(offset) == 0)
      }
    }
  }

  test("serialize and deserialize topic latest offsets") {
    val topicOffsets: Map[String, MessageId] = 0
      .to(9)
      .map { i =>
        (s"topic-${i}", MessageId.latest)
      }
      .toMap

    val str = JsonUtils.topicOffsets(topicOffsets)
    val readTopicOffsets = JsonUtils.topicOffsets(str)

    assert(10 == readTopicOffsets.size)
    0.to(9) map { i =>
      {
        val topic = s"topic-${i}"
        val offset = readTopicOffsets(topic)
        assert(MessageId.latest.compareTo(offset) == 0)
      }
    }
  }

  test("serialize and deserialize topic specific offsets") {
    val topicOffsets: Map[String, MessageId] = 0
      .to(9)
      .map { i =>
        (s"topic-${i}", new MessageIdImpl(10 + i, 100 + i, i))
      }
      .toMap

    val str = JsonUtils.topicOffsets(topicOffsets)
    val readTopicOffsets = JsonUtils.topicOffsets(str)

    assert(10 == readTopicOffsets.size)
    0.to(9) map { i =>
      {
        val topic = s"topic-${i}"
        val offset = readTopicOffsets(topic)
        val messageId = new MessageIdImpl(10 + i, 100 + i, i)
        assert(messageId.compareTo(offset) == 0)
      }
    }
  }

  test("serialize and deserialize topic specific batch offsets") {
    val topicOffsets: Map[String, MessageId] = 0
      .to(9)
      .map { i =>
        (s"topic-${i}", new BatchMessageIdImpl(10 + i, 100 + i, i, i))
      }
      .toMap

    val str = JsonUtils.topicOffsets(topicOffsets)
    val readTopicOffsets = JsonUtils.topicOffsets(str)

    assert(10 == readTopicOffsets.size)
    0.to(9) map { i =>
      {
        val topic = s"topic-${i}"
        val offset = readTopicOffsets(topic)
        val messageId = new BatchMessageIdImpl(10 + i, 100 + i, i, i)
        assert(messageId.compareTo(offset) == 0)
      }
    }
  }
}
