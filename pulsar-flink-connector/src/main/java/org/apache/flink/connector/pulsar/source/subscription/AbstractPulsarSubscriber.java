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

package org.apache.flink.connector.pulsar.source.subscription;

import org.apache.flink.connector.pulsar.source.AbstractPartition;
import org.apache.flink.connector.pulsar.source.PulsarSubscriber;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * The base implementations of {@link PulsarSubscriber}.
 */
public abstract class AbstractPulsarSubscriber extends PulsarSubscriber {
    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Override
    public PartitionChange getPartitionChanges(
            PulsarAdmin pulsarAdmin,
            Set<AbstractPartition> currentAssignment) throws PulsarAdminException, InterruptedException, IOException {
        Set<AbstractPartition> newPartitions = new HashSet<>();
        Set<AbstractPartition> removedPartitions = new HashSet<>(currentAssignment);
        for (AbstractPartition partition : getCurrentPartitions(pulsarAdmin)) {
            if (!removedPartitions.remove(partition)) {
                newPartitions.add(partition);
            }
        }
        if (!removedPartitions.isEmpty()) {
            logger.warn("The following partitions have been removed from the Pulsar cluster. {}", removedPartitions);
        }
        if (!newPartitions.isEmpty()) {
            logger.info("The following partitions have been added to the Pulsar cluster. {}", newPartitions);
        }
        return new PartitionChange(newPartitions, removedPartitions);
    }

    protected abstract Collection<AbstractPartition> getCurrentPartitions(PulsarAdmin pulsarAdmin) throws PulsarAdminException, InterruptedException, IOException;
}
