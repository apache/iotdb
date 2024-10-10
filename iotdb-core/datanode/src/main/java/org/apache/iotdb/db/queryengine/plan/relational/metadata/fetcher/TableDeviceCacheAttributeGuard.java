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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class TableDeviceCacheAttributeGuard {
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
  }
}
