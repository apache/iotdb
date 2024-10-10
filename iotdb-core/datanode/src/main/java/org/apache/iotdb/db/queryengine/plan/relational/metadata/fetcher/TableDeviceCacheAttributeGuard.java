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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.queryengine.plan.relational.metadata.fetcher;

import org.apache.iotdb.mpp.rpc.thrift.TAttributeUpdateReq;
import org.apache.iotdb.mpp.rpc.thrift.TSchemaRegionAttributeInfo;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.stream.Collectors;

public class TableDeviceCacheAttributeGuard {

  // Unbounded queue
  final LinkedBlockingDeque<Set<?>> applyQueue = new LinkedBlockingDeque<>();
  final Map<Integer, Long> fetchedSchemaRegionIds2LargestVersionMap = new ConcurrentHashMap<>();

  public boolean isRegionFetched(final Integer schemaRegionId) {
    return fetchedSchemaRegionIds2LargestVersionMap.containsKey(schemaRegionId);
  }

  public void addFetchedRegion(final Integer schemaRegionId) {
    fetchedSchemaRegionIds2LargestVersionMap.put(schemaRegionId, Long.MIN_VALUE);
  }

  public void handleAttributeUpdate(final TAttributeUpdateReq updateReq) {
    // Trim the schema regions with lower or equal version
    // or with this node restarted before fetching
    updateReq
        .getAttributeUpdateMap()
        .entrySet()
        .removeIf(
            entry -> {
              if (!fetchedSchemaRegionIds2LargestVersionMap.containsKey(entry.getKey())
                  || entry.getValue().getVersion()
                      <= fetchedSchemaRegionIds2LargestVersionMap.get(entry.getKey())) {
                return true;
              }
              fetchedSchemaRegionIds2LargestVersionMap.put(
                  entry.getKey(), entry.getValue().getVersion());
              return false;
            });
    applyQueue.add(
        updateReq.getAttributeUpdateMap().values().stream()
            .map(TSchemaRegionAttributeInfo::getBody)
            .collect(Collectors.toSet()));
    tryUpdateCache();
  }

  public synchronized void tryUpdateCache() {
    while (!applyQueue.isEmpty()) {
      final Set<?> firstElement = applyQueue.peek();
      if (firstElement instanceof HashSet) {
        applyUpdateMessage(firstElement);
      } else if (firstElement.isEmpty()) {
        applyQueue.removeFirst();
      } else {
        break;
      }
    }
  }

  private void applyUpdateMessage(final Set<?> updateContainerBytes) {
    for (final Object element : updateContainerBytes) {
      final byte[] containerByte = (byte[]) element;
    }
  }
}
