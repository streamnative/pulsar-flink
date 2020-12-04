/*
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

package org.apache.flink.streaming.connectors.pulsar.testutils;

import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.utils.TableTestMatchers;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.CloseableIterator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

/**
 * Utils for pulsar table tests.
 * */
public class PulsarTableTestUtils {
	public static List<Row> collectRows(Table table, int expectedSize) throws Exception {
		final TableResult result = table.execute();
		final List<Row> collectedRows = new ArrayList<>();
		try (CloseableIterator<Row> iterator = result.collect()) {
			while (collectedRows.size() < expectedSize && iterator.hasNext()) {
				collectedRows.add(iterator.next());
			}
		}
		result.getJobClient().ifPresent(jc -> {
			try {
				jc.cancel().get(5, TimeUnit.SECONDS);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		});

		return collectedRows;
	}

	public static void comparedWithKeyAndOrder(Map<Row, List<Row>> expectedData, List<Row> actual, int[] keyLoc) {
		Map<Row, LinkedList<Row>> actualData = new HashMap<>();
		for (Row row: actual) {
			Row key = Row.project(row, keyLoc);
			// ignore row kind
			key.setKind(RowKind.INSERT);
			actualData.computeIfAbsent(key, k -> new LinkedList<>()).add(row);
		}
		// compare key first
		assertEquals("Actual result: " + actual, expectedData.size(), actualData.size());
		// compare by value
		for (Row key: expectedData.keySet()) {
			assertThat(
				actualData.get(key),
				TableTestMatchers.deepEqualTo(expectedData.get(key), false));
		}
	}
}
