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

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.commons.schema.ClusterSchemaQuotaLevel;
import org.apache.iotdb.mpp.rpc.thrift.TSchemaQuotaLevel;
import org.apache.iotdb.mpp.rpc.thrift.TSchemaQuotaReq;
import org.apache.iotdb.mpp.rpc.thrift.TSchemaQuotaResp;

import javax.validation.constraints.NotNull;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class ClusterSchemaQuotaManager {

  private final Map<TConsensusGroupId, Long> countMap = new ConcurrentHashMap<>();
  private ClusterSchemaQuotaLevel level = ClusterSchemaQuotaLevel.MEASUREMENT;
  private long limit = -1;

  public void updateConfig(ClusterSchemaQuotaLevel level, long limit) {
    this.level = level;
    this.limit = limit;
  }

  public void updateCount(@NotNull TSchemaQuotaResp resp, List<TConsensusGroupId> leaderGroupIds) {
    for (TConsensusGroupId groupId : leaderGroupIds) {
      if (resp.regionIdCountMap.containsKey(groupId)) {
        countMap.put(groupId, resp.regionIdCountMap.get(groupId));
      }
    }
  }

  public TSchemaQuotaReq generateReq() {
    if (limit == -1) {
      return null;
    } else {
      return new TSchemaQuotaReq(
          level == ClusterSchemaQuotaLevel.MEASUREMENT
              ? TSchemaQuotaLevel.MEASUREMENT
              : TSchemaQuotaLevel.DEVICE,
          countMap.values().stream().mapToLong(i -> i).sum(),
          limit);
    }
  }

  public void invalidateSchemaRegion(Set<TConsensusGroupId> consensusGroupIdSet) {
    for (TConsensusGroupId consensusGroupId : consensusGroupIdSet) {
      countMap.remove(consensusGroupId);
    }
  }

  private ClusterSchemaQuotaManager() {}

  public static ClusterSchemaQuotaManager getInstance() {
    return ClusterSchemaQuotaManager.ClusterSchemaQuotaManagerHolder.INSTANCE;
  }

  private static class ClusterSchemaQuotaManagerHolder {
    private static final ClusterSchemaQuotaManager INSTANCE = new ClusterSchemaQuotaManager();

    private ClusterSchemaQuotaManagerHolder() {
      // empty constructor
    }
  }
}
