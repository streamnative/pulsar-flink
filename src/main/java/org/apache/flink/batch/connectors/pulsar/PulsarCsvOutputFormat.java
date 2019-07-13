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
package org.apache.flink.batch.connectors.pulsar;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.batch.connectors.pulsar.serialization.CsvSerializationSchema;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.impl.conf.ProducerConfigurationData;

/**
 * Pulsar Csv Output Format to write Flink DataSets into a Pulsar topic in Csv format.
 */
public class PulsarCsvOutputFormat<T extends Tuple> extends BasePulsarOutputFormat<T> {

    private static final long serialVersionUID = -4461671510903404196L;

    public PulsarCsvOutputFormat(String serviceUrl, String topicName, Authentication authentication) {
        super(serviceUrl, topicName, authentication);
        this.serializationSchema = new CsvSerializationSchema<>();
    }

    public PulsarCsvOutputFormat(
            ClientConfigurationData clientConfigurationData,
            ProducerConfigurationData producerConfigurationData) {
        super(clientConfigurationData, producerConfigurationData);
        this.serializationSchema = new CsvSerializationSchema<>();
    }

}
