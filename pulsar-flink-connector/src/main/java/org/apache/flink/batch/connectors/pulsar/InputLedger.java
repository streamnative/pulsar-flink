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

package org.apache.flink.batch.connectors.pulsar;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.pulsar.shade.com.google.common.base.MoreObjects;

import javax.validation.constraints.NotNull;

import java.io.Serializable;
import java.util.Map;
import java.util.UUID;

/**
 * A ledger with a range to be read.
 */
@Data
@AllArgsConstructor
public class InputLedger implements Serializable, Comparable<InputLedger> {

    private String topic;

    private long ledgerId;

    private long startEntryId;

    private long endEntryId;

    private UUID uuid;

    private Map<String, String> offloaderDrvierMeta;

    public long ledgerSize() {
        return endEntryId - startEntryId + 1;
    }

    public boolean isOffloadedLedger() {
        return uuid != null;
    }

    @Override
    public int compareTo(@NotNull InputLedger o) {
        return ledgerSize() > o.ledgerSize() ? 1 :
                (ledgerSize() == o.ledgerSize() ? 0 : -1);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("topic", topic)
                .add("ledger", ledgerId)
                .add("startEntryId", startEntryId)
                .add("endEntryId", endEntryId)
                .add("size", ledgerSize())
                .toString();
    }
}
