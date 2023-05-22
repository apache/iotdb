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
package org.apache.iotdb.confignode.manager.schema;

import javax.validation.constraints.NotNull;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class ClusterSchemaQuotaStatistics {

  // TODO: it can be merged with statistics in ClusterQuotaManager
  private final Map<Integer, Long> countMap = new ConcurrentHashMap<>();

  public void updateCount(@NotNull Map<Integer, Long> schemaCountMap) {
    countMap.putAll(schemaCountMap);
  }

  public long getSchemaQuotaCount(Set<Integer> consensusGroupIdSet) {
    return countMap.entrySet().stream()
        .filter(i -> consensusGroupIdSet.contains(i.getKey()))
        .mapToLong(Map.Entry::getValue)
        .sum();
  }

  public void clear() {
    countMap.clear();
  }
}
