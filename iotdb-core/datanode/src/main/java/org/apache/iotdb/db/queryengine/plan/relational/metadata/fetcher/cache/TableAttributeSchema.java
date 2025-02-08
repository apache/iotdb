/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.queryengine.plan.relational.metadata.fetcher.cache;

import org.apache.iotdb.db.schemaengine.schemaregion.attribute.update.UpdateDetailContainer;
import org.apache.iotdb.db.schemaengine.table.DataNodeTableCache;

import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.RamUsageEstimator;

import javax.annotation.Nonnull;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class TableAttributeSchema implements IDeviceSchema {

  static final int INSTANCE_SIZE =
      (int) RamUsageEstimator.shallowSizeOfInstance(TableAttributeSchema.class)
          + (int) RamUsageEstimator.shallowSizeOfInstance(ConcurrentHashMap.class);

  private final Map<String, Binary> attributeMap = new ConcurrentHashMap<>();

  public int updateAttribute(
      final String database, final String tableName, final @Nonnull Map<String, Binary> updateMap) {
    final AtomicInteger diff = new AtomicInteger(0);
    updateMap.forEach(
        (k, v) -> {
          if (v != Binary.EMPTY_VALUE) {
            if (!attributeMap.containsKey(k)) {
              k = DataNodeTableCache.getInstance().tryGetInternColumnName(database, tableName, k);
              // Removing attribute column, do not put cache
              if (Objects.isNull(k)) {
                return;
              }
            }
            final Binary previousValue = attributeMap.put(k, v);
            final long newValueSize = UpdateDetailContainer.sizeOf(v);
            diff.addAndGet(
                (int)
                    (Objects.nonNull(previousValue)
                        ? newValueSize - UpdateDetailContainer.sizeOf(previousValue)
                        : RamUsageEstimator.HASHTABLE_RAM_BYTES_PER_ENTRY + newValueSize));
          } else {
            attributeMap.remove(k);
            diff.addAndGet(
                (int)
                    (-RamUsageEstimator.HASHTABLE_RAM_BYTES_PER_ENTRY
                        - UpdateDetailContainer.sizeOf(v)));
          }
        });
    // Typically the "update" and "invalidate" won't be concurrently called
    // Here we reserve the check for consistency and potential safety
    return diff.get();
  }

  public int removeAttribute(final String attribute) {
    final Binary previousValue = attributeMap.remove(attribute);
    return Objects.nonNull(previousValue)
        ? (int)
            -(RamUsageEstimator.HASHTABLE_RAM_BYTES_PER_ENTRY
                + UpdateDetailContainer.sizeOf(previousValue))
        : 0;
  }

  public Map<String, Binary> getAttributeMap() {
    return attributeMap;
  }

  public int estimateSize() {
    return (int) RamUsageEstimator.HASHTABLE_RAM_BYTES_PER_ENTRY * attributeMap.size()
        + attributeMap.values().stream()
            .mapToInt(attrValue -> (int) attrValue.ramBytesUsed())
            .reduce(0, Integer::sum);
  }
}
