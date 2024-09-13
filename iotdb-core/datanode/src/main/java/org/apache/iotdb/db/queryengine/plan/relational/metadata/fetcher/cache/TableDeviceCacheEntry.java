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

import org.apache.tsfile.utils.RamUsageEstimator;

import javax.annotation.Nonnull;

import java.util.Map;
import java.util.Objects;

public class TableDeviceCacheEntry {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(TableDeviceCacheEntry.class);

  // the cached attributeMap may not be the latest, but there won't be any correctness problems
  // because when missing getting the key-value from this attributeMap, caller will try to get or
  // create from remote
  // there may exist key is not null, but value is null in this map, which means that the key's
  // corresponding value is null, doesn't mean that the key doesn't exist
  private final Map<String, String> attributeMap;

  public TableDeviceCacheEntry(final @Nonnull Map<String, String> attributeMap) {
    this.attributeMap = attributeMap;
  }

  public void update(final @Nonnull Map<String, String> updateMap) {
    updateMap.forEach(
        (k, v) -> {
          if (Objects.nonNull(v)) {
            attributeMap.put(k, v);
          } else {
            attributeMap.remove(k);
          }
        });
  }

  public String getAttribute(final String key) {
    return attributeMap.get(key);
  }

  public Map<String, String> getAttributeMap() {
    return attributeMap;
  }

  public int estimateSize() {
    return (int) (INSTANCE_SIZE + RamUsageEstimator.sizeOfMap(attributeMap));
  }
}
