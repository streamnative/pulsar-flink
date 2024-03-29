/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.pulsar;

import org.apache.flink.api.common.JobID;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.metrics.jmx.JMXReporter;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.table.api.TableColumn;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.test.util.MiniClusterWithClientResource;

import org.apache.pulsar.shade.org.apache.commons.lang3.StringUtils;
import org.junit.Before;
import org.junit.Rule;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

/** Setup flink mini cluster. */
public abstract class PulsarTestBaseWithFlink extends PulsarTestBase {

    protected static final int NUM_TMS = 1;

    protected static final int TM_SLOTS = 8;

    protected ClusterClient<?> client;

    @Rule
    public MiniClusterWithClientResource flink =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setConfiguration(getFlinkConfiguration())
                            .setNumberTaskManagers(NUM_TMS)
                            .setNumberSlotsPerTaskManager(TM_SLOTS)
                            .build());

    @Before
    public void noJobIsRunning() throws Exception {
        client = flink.getClusterClient();
        waitUntilNoJobIsRunning(client);
    }

    protected static Configuration getFlinkConfiguration() {
        Configuration flinkConfig = new Configuration();

        flinkConfig.setString(TaskManagerOptions.MANAGED_MEMORY_SIZE.key(), "16m");
        flinkConfig.setString(
                ConfigConstants.METRICS_REPORTER_PREFIX
                        + "my_reporter."
                        + ConfigConstants.METRICS_REPORTER_CLASS_SUFFIX,
                JMXReporter.class.getName());
        flinkConfig.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, NUM_TMS);
        flinkConfig.setInteger(TaskManagerOptions.NUM_TASK_SLOTS, TM_SLOTS);
        flinkConfig.setBoolean(WebOptions.SUBMIT_ENABLE, false);
        return flinkConfig;
    }

    public static void waitUntilJobIsRunning(ClusterClient<?> client) throws Exception {
        while (getRunningJobs(client).isEmpty()) {
            Thread.sleep(50);
        }
    }

    public static void waitUntilNoJobIsRunning(ClusterClient<?> client) throws Exception {
        while (!getRunningJobs(client).isEmpty()) {
            Thread.sleep(50);
            cancelRunningJobs(client);
        }
    }

    public static List<JobID> getRunningJobs(ClusterClient<?> client) throws Exception {
        Collection<JobStatusMessage> statusMessages = client.listJobs().get();
        return statusMessages.stream()
                .filter(status -> !status.getJobState().isGloballyTerminalState())
                .map(JobStatusMessage::getJobId)
                .collect(Collectors.toList());
    }

    public static void cancelRunningJobs(ClusterClient<?> client) throws Exception {
        List<JobID> runningJobs = getRunningJobs(client);
        for (JobID runningJob : runningJobs) {
            client.cancel(runningJob);
        }
    }

    protected String createTableSql(
            String tableName, String topic, TableSchema tableSchema, String formatType) {
        List<String> columns = new ArrayList<>();
        for (TableColumn tableColumn : tableSchema.getTableColumns()) {
            final String column =
                    MessageFormat.format(
                            " `{0}` {1}",
                            tableColumn.getName(),
                            tableColumn.getType().getLogicalType().asSerializableString());
            columns.add(column);
        }

        return "create table "
                + tableName
                + "(\n"
                + " "
                + StringUtils.join(columns, ",\n")
                + ") with (\n"
                + "   'connector' = 'pulsar',\n"
                + "   'generic' = 'true',\n"
                + "   'topic' = '"
                + topic
                + "',\n"
                + "   'service-url' = '"
                + getServiceUrl()
                + "',\n"
                + "   'admin-url' = '"
                + getAdminUrl()
                + "',\n"
                + "   'scan.startup.mode' = 'earliest',\n"
                + "   'partition.discovery.interval-millis' = '5000',\n"
                + "   'format' = '"
                + formatType
                + "'\n"
                + ")";
    }
}
