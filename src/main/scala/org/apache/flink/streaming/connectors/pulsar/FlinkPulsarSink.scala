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

import scala.reflect.ClassTag

import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.connectors.pulsar.internal.PulsarOptions.TOPIC_SINGLE

import org.apache.pulsar.client.api.Schema

class FlinkPulsarSink[T: ClassTag](
  parameters: Properties,
  topicKeyExtractor: TopicKeyExtractor[T])
  extends FlinkPulsarSinkBase[T](parameters, topicKeyExtractor) {

  @transient lazy val pulsarSchema: Schema[_] = Schema.AVRO(implicitly[ClassTag[T]].runtimeClass)

  /**
   * Writes the given value to the sink. This function is called for every record.
   *
   * <p>You have to override this method when implementing a {@code SinkFunction}, this is a
   * {@code default} method for backward compatibility with the old-style method only.
   *
   * @param value   The input record.
   * @param context Additional context about the input record.
   * @throws Exception This method may throw exceptions.
   *                   Throwing an exception will cause the operation
   *                   to fail and may trigger recovery.
   */
  override def invoke(value: T, context: SinkFunction.Context[_]): Unit = {
    checkForErrors()

    val mb = if (forcedTopic) {
      getProducer(topicName).newMessage().value(value)
    } else {
      val key = topicKeyExtractor.serializeKey(value)
      val topic = topicKeyExtractor.getTopic(value)

      if (topic == null) {
        if (doFailOnWrite) {
          throw new NullPointerException(s"null topic present in the data. Use the " +
            s"$TOPIC_SINGLE option for setting a topic.")
        }
        return
      }

      val mb = getProducer(topic).newMessage().value(value)
      if (null != key) {
        mb.keyBytes(key)
      }
      mb
    }

    if (doFlushOnCheckpoint) {
      pendingRecordsLock.synchronized {
        pendingRecords += 1
      }
    }
    mb.sendAsync().whenComplete(sendCallback)
  }
}
