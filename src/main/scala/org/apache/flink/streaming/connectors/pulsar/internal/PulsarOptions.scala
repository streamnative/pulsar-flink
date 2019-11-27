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
package org.apache.flink.streaming.connectors.pulsar.internal

import org.apache.pulsar.common.naming.TopicName

// All options should be lowercase to simplify parameter matching
object PulsarOptions {

  // option key prefix for different modules
  val PULSAR_CLIENT_OPTION_KEY_PREFIX = "pulsar.client."
  val PULSAR_PRODUCER_OPTION_KEY_PREFIX = "pulsar.producer."
  val PULSAR_CONSUMER_OPTION_KEY_PREFIX = "pulsar.consumer."
  val PULSAR_READER_OPTION_KEY_PREFIX = "pulsar.reader."

  // options

  val TOPIC_SINGLE = "topic"
  val TOPIC_MULTI = "topics"
  val TOPIC_PATTERN = "topicspattern"

  val PARTITION_SUFFIX = TopicName.PARTITIONED_TOPIC_SUFFIX

  val TOPIC_OPTION_KEYS = Set(
    TOPIC_SINGLE,
    TOPIC_MULTI,
    TOPIC_PATTERN
  )

  val SERVICE_URL_OPTION_KEY = "service.url"
  val ADMIN_URL_OPTION_KEY = "admin.url"
  val STARTING_OFFSETS_OPTION_KEY = "startingoffsets"
  val ENDING_OFFSETS_OPTION_KEY = "endingoffsets"

  val EXTERNAL_SUB_NAME = "subscriptionname"
  val REMOVE_SUB_ON_STOP = "removesubscriptiononstop"

  val PARTITION_DISCOVERY_INTERVAL_MS = "partitiondiscoveryintervalmillis"
  val USE_METRICS_OPTION_KEY = "usemetrics"

  val CLIENT_CACHE_SIZE = "clientcachesize"

  val FLUSH_ON_CHECKPOINT = "flushoncheckpoint"
  val FAIL_ON_WRITE = "failonwrite"

  val POLL_TIMEOUT_MS = "polltimeoutms"

  val NUM_PARTITIONS = "table.partitions"

  // TODO: Will flink lose data? how does it happen?
  val FAIL_ON_DATA_LOSS_OPTION_KEY = "failondataloss"

  val INSTRUCTION_FOR_FAIL_ON_DATA_LOSS_FALSE =
    """
      |Some data may have been lost because they are not available in Pulsar any more; either the
      | data was aged out by Pulsar or the topic may have been deleted before all the data in the
      | topic was processed. If you want your streaming query to fail on such cases, set the source
      | option "failOnDataLoss" to "true".
    """.stripMargin

  val INSTRUCTION_FOR_FAIL_ON_DATA_LOSS_TRUE =
    """
      |Some data may have been lost because they are not available in Pulsar any more; either the
      | data was aged out by Pulsar or the topic may have been deleted before all the data in the
      | topic was processed. If you don't want your streaming query to fail on such cases, set the
      | source option "failOnDataLoss" to "false".
    """.stripMargin

  val TOPIC_SCHEMA_CLASS_OPTION_KEY = "topic.schema.class"

  val FILTERED_KEYS: Set[String] =
    Set(TOPIC_SINGLE, SERVICE_URL_OPTION_KEY, TOPIC_SCHEMA_CLASS_OPTION_KEY)

  val TOPIC_ATTRIBUTE_NAME: String = "__topic"
  val KEY_ATTRIBUTE_NAME: String = "__key"
  val MESSAGE_ID_NAME: String = "__messageId"
  val PUBLISH_TIME_NAME: String = "__publishTime"
  val EVENT_TIME_NAME: String = "__eventTime"

  val META_FIELD_NAMES = Set(
    TOPIC_ATTRIBUTE_NAME,
    KEY_ATTRIBUTE_NAME,
    MESSAGE_ID_NAME,
    PUBLISH_TIME_NAME,
    EVENT_TIME_NAME
  )
}
