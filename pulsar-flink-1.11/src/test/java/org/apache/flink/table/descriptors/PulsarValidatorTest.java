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

package org.apache.flink.table.descriptors;

import org.apache.flink.util.ExceptionUtils;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.junit.Assert.assertTrue;

/**
 * Tests for the {@link PulsarValidator}.
 */
public class PulsarValidatorTest {
	private static final String adminUrl = "http://localhost:8080";
	private static final String serviceUrl = "pulsar://localhost:6650";

	@Test
	public void testValidateWithoutUrls() {
		// without adminUrl and serviceUrl
		final Map<String, String> props = new HashMap<>();
		props.put("connector.property-version", "1");
		props.put("connector.type", "pulsar");
		props.put("connector.topic", "MyTopic");

		final DescriptorProperties descriptorProperties = new DescriptorProperties();
		descriptorProperties.putProperties(props);
		try {
			new PulsarValidator().validate(descriptorProperties);
		} catch (Exception e) {
			Optional<Throwable> throwable =
				ExceptionUtils.findThrowableWithMessage(e,
					"Could not find required property 'connector.service-url'.");
			assertTrue(throwable.isPresent());
		}
	}

	@Test
	public void testValidateWithoutTopic() {
		// without topic
		final Map<String, String> props = new HashMap<>();
		props.put("connector.property-version", "1");
		props.put("connector.type", "pulsar");
		props.put("connector.service-url", serviceUrl);
		props.put("connector.admin-url", adminUrl);

		final DescriptorProperties descriptorProperties = new DescriptorProperties();
		descriptorProperties.putProperties(props);
		try {
			new PulsarValidator().validate(descriptorProperties);
		} catch (Exception e) {
			Optional<Throwable> throwable =
					ExceptionUtils.findThrowableWithMessage(e,
							"Could not find required property 'connector.topic'.");
			assertTrue(throwable.isPresent());
		}
	}

	@Test
	public void testValidateWithoutExtendField() {
		// without topic
		final Map<String, String> props = new HashMap<>();
		props.put("connector.property-version", "1");
		props.put("connector.type", "pulsar");
		props.put("connector.service-url", serviceUrl);
		props.put("connector.admin-url", adminUrl);

		final DescriptorProperties descriptorProperties = new DescriptorProperties();
		descriptorProperties.putProperties(props);
		try {
			new PulsarValidator().validate(descriptorProperties);
		} catch (Exception e) {
			Optional<Throwable> throwable =
					ExceptionUtils.findThrowableWithMessage(e,
							"Could not find required property 'connector.topic'.");
			assertTrue(throwable.isPresent());
		}
	}
}
