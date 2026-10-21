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

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class TableAttributeSchema implements IDeviceSchema {

  private static final int ATTRIBUTE_ENTRY_SIZE =
      (int) RamUsageEstimator.HASHTABLE_RAM_BYTES_PER_ENTRY;

  static final int INSTANCE_SIZE =
      (int) RamUsageEstimator.shallowSizeOfInstance(TableAttributeSchema.class)
          + 2 * (int) RamUsageEstimator.shallowSizeOfInstance(ConcurrentHashMap.class);

  private final Map<String, Binary> attributeMap = new ConcurrentHashMap<>();
  private final Set<String> fetchedAttributeSet = ConcurrentHashMap.newKeySet();

  public int updateAttribute(
      final String database, final String tableName, final @Nonnull Map<String, Binary> updateMap) {
    return updateAttribute(database, tableName, updateMap, Collections.emptySet());
  }

  public int updateAttribute(
      final String database,
      final String tableName,
      final @Nonnull Map<String, Binary> updateMap,
      final @Nonnull Collection<String> fetchedAttributeNames) {
    final AtomicInteger diff = new AtomicInteger(0);
    fetchedAttributeNames.forEach(
        attribute -> diff.addAndGet(addFetchedAttribute(database, tableName, attribute)));
    updateMap.forEach(
        (k, v) -> {
          final String attribute = getInternAttributeName(database, tableName, k);
          if (Objects.isNull(attribute)) {
            return;
          }
          if (fetchedAttributeSet.add(attribute)) {
            diff.addAndGet(ATTRIBUTE_ENTRY_SIZE);
          }
          if (v != Binary.EMPTY_VALUE) {
            final Binary previousValue = attributeMap.put(attribute, v);
            final long newValueSize = UpdateDetailContainer.sizeOf(v);
            diff.addAndGet(
                (int)
                    (Objects.nonNull(previousValue)
                        ? newValueSize - UpdateDetailContainer.sizeOf(previousValue)
                        : ATTRIBUTE_ENTRY_SIZE + newValueSize));
          } else {
            final Binary previousValue = attributeMap.remove(attribute);
            diff.addAndGet(
                (int)
                    (Objects.nonNull(previousValue)
                        ? -ATTRIBUTE_ENTRY_SIZE - UpdateDetailContainer.sizeOf(previousValue)
                        : 0));
          }
        });
    // Typically the "update" and "invalidate" won't be concurrently called
    // Here we reserve the check for consistency and potential safety
    return diff.get();
  }

  public int removeAttribute(final String attribute) {
    final Binary previousValue = attributeMap.remove(attribute);
    final boolean removedFromFetchedSet = fetchedAttributeSet.remove(attribute);
    return (int)
        ((Objects.nonNull(previousValue)
                ? ATTRIBUTE_ENTRY_SIZE + UpdateDetailContainer.sizeOf(previousValue)
                : 0)
            + (removedFromFetchedSet ? ATTRIBUTE_ENTRY_SIZE : 0));
  }

  public Map<String, Binary> getAttributeMap() {
    return attributeMap;
  }

  public boolean containsAttributes(final Collection<String> attributeNames) {
    return fetchedAttributeSet.containsAll(attributeNames);
  }

  private int addFetchedAttribute(
      final String database, final String tableName, final String attributeName) {
    final String attribute = getInternAttributeName(database, tableName, attributeName);
    return Objects.nonNull(attribute) && fetchedAttributeSet.add(attribute)
        ? ATTRIBUTE_ENTRY_SIZE
        : 0;
  }

  private String getInternAttributeName(
      final String database, final String tableName, final String attributeName) {
    if (attributeMap.containsKey(attributeName) || fetchedAttributeSet.contains(attributeName)) {
      return attributeName;
    }
    return DataNodeTableCache.getInstance()
        .tryGetInternColumnName(database, tableName, attributeName);
  }

  @Override
  public int estimateSize() {
    return INSTANCE_SIZE
        + ATTRIBUTE_ENTRY_SIZE * (attributeMap.size() + fetchedAttributeSet.size())
        + attributeMap.values().stream()
            .mapToInt(attrValue -> (int) attrValue.ramBytesUsed())
            .reduce(0, Integer::sum);
  }
}
