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

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.shade.com.google.common.collect.ImmutableList;
import org.apache.pulsar.shade.com.google.common.collect.Lists;
import org.apache.pulsar.shade.org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.pulsar.shade.org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.pulsar.shade.org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.pulsar.shade.org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.shade.org.apache.bookkeeper.mledger.impl.ReadOnlyCursorImpl;
import org.apache.pulsar.shade.org.apache.bookkeeper.mledger.offload.OffloadUtils;
import org.apache.pulsar.shade.org.apache.bookkeeper.mledger.proto.MLDataFormats;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Utilities for split generation logic.
 */
@Slf4j
public class SplitUtils {

    public static Collection<InputLedger> getLedgersInBetween(
            String topic,
            MessageIdImpl start,
            MessageIdImpl end,
            CachedClients cachedClients) throws Exception {

        ReadOnlyCursorImpl readOnlyCursor = null;

        try {

            ManagedLedgerFactory mlFactory = cachedClients.getManagedLedgerFactory();
            readOnlyCursor = (ReadOnlyCursorImpl) mlFactory.openReadOnlyCursor(
                    TopicName.get(topic).getPersistenceNamingEncoding(), PositionImpl.earliest,
                    new ManagedLedgerConfig());
            ManagedLedgerImpl ml = (ManagedLedgerImpl) readOnlyCursor.getManagedLedger();

            List<MLDataFormats.ManagedLedgerInfo.LedgerInfo> allLedgers =
                    Lists.newArrayList(
                            ml.getLedgersInfo().subMap(start.getLedgerId(), true, end.getLedgerId(), true).values());

            long actualStartLedger = allLedgers.get(0).getLedgerId();
            long actualStartEntry = start.getEntryId() > 0 ? start.getEntryId() : 0;

            MLDataFormats.ManagedLedgerInfo.LedgerInfo endLedger = allLedgers.get(allLedgers.size() - 1);
            long actualEndLedger = endLedger.getLedgerId();
            long lastEntry = endLedger.getEntries();
            long actualEndEntry =
                    end.getEntryId() >= lastEntry ?
                            lastEntry :
                            (end.getEntryId() >= 0 ? end.getEntryId() : 0);

            Map<Long, InputLedger> ledgersToRead = new HashMap<>();

            for (MLDataFormats.ManagedLedgerInfo.LedgerInfo li : allLedgers) {
                if (li.hasOffloadContext() && li.getOffloadContext().getComplete()) {
                    UUID uid = new UUID(li.getOffloadContext().getUidMsb(), li.getOffloadContext().getUidLsb());
                    Map metadata = OffloadUtils.getOffloadDriverMetadata(li);
                    metadata.put("ManagedLedgerName", topic);
                    ledgersToRead.put(li.getLedgerId(),
                            new InputLedger(topic, li.getLedgerId(), 0L, li.getEntries() - 1, uid, metadata));
                } else {
                    ledgersToRead.put(li.getLedgerId(),
                            new InputLedger(topic, li.getLedgerId(), 0L, li.getEntries() - 1, null, null));
                }
            }

            ledgersToRead.get(actualStartLedger).setStartEntryId(actualStartEntry);
            ledgersToRead.get(actualEndLedger).setEndEntryId(actualEndEntry);

            return ledgersToRead.values();

        } catch (Exception e) {
            e.printStackTrace();
            return null;
        } finally {

            if (readOnlyCursor != null) {
                try {
                    readOnlyCursor.close();
                } catch (Exception e) {
                    log.error("Failed to close readOnly cursor", e);
                }
            }
        }
    }

    /**
     * Best effort partitioning based on ledger size (use backtracking method).
     * If ledger number less than or equal to the target parallelism, make a partition for each ledger.
     * @param ledgers The ledgers list to be partitioned.
     * @param parallelism The number of target partition.
     * @return Ledgers grouped into partitions.
     */
    public static List<List<InputLedger>> partitionToNSplits(List<InputLedger> ledgers, int parallelism) {

        if (ledgers.size() <= parallelism) {
            return ledgers.stream().map(l -> ImmutableList.of(l)).collect(Collectors.toList());
        }

        long totalSize = sizeOfLedgerList(ledgers);
        long avgSizePerSplit = totalSize / parallelism;

        ledgers.sort(null);

        int li = ledgers.size() - 1;
        int k = parallelism;

        List<InputLedger>[] outputGroups = new List[k];
        for (int i = 0; i < outputGroups.length; i++) {
            outputGroups[i] = new ArrayList<>();
        }

        while (li >= 0 && ledgers.get(li).ledgerSize() >= avgSizePerSplit) {
            outputGroups[k - 1].add(ledgers.get(li));
            li--;
            k--;
        }

        search(outputGroups, li, ledgers, avgSizePerSplit);

        return Arrays.asList(outputGroups);
    }

    private static boolean search(
            List<InputLedger>[] outputGroups,
            int li,
            List<InputLedger> ledgers,
            long avgSizePerSplit) {

        if (li < 0) {
            return true;
        }

        InputLedger currentLedger = ledgers.get(li);
        li -= 1;

        long smallestGroupSize = Long.MAX_VALUE;
        int smallestGroupIndex = 0;

        for (int i = 0; i < outputGroups.length; i++) {
            long groupISize = sizeOfLedgerList(outputGroups[i]);

            if (groupISize + currentLedger.ledgerSize() <= avgSizePerSplit) {
                outputGroups[i].add(currentLedger);
                if (search(outputGroups, li, ledgers, avgSizePerSplit)) {
                    return true;
                }
                outputGroups[i].remove(outputGroups[i].size() - 1);
            }

            if (groupISize < smallestGroupSize) {
                smallestGroupSize = groupISize;
                smallestGroupIndex = i;
            }
        }

        // put current ledger to the smallest-sized list and restart the search
        outputGroups[smallestGroupIndex].add(currentLedger);
        search(outputGroups, li, ledgers, avgSizePerSplit);

        return true;
    }

    public static long sizeOfLedgerList(List<InputLedger> ledgers) {
        return ledgers.stream().mapToLong(InputLedger::ledgerSize).sum();
    }
}
