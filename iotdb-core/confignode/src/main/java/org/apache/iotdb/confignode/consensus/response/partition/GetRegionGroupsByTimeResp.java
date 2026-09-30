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

package org.apache.iotdb.confignode.consensus.response.partition;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.confignode.rpc.thrift.TGetRegionGroupsByTimeResp;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.rpc.TSStatusCode;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class GetRegionGroupsByTimeResp implements DataSet {

  private final TSStatus status;

  private final Set<TRegionReplicaSet> regionReplicaSets;

  public GetRegionGroupsByTimeResp(
      final TSStatus status, final Set<TRegionReplicaSet> regionReplicaSets) {
    this.status = status;
    this.regionReplicaSets = regionReplicaSets;
  }

  public TSStatus getStatus() {
    return status;
  }

  /** Return a response whose replica lists put the current leader first. */
  public GetRegionGroupsByTimeResp reorderByLeader(
      final Map<TConsensusGroupId, Integer> regionLeaderMap) {
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return this;
    }
    final Set<TRegionReplicaSet> reordered = new HashSet<>();
    for (final TRegionReplicaSet replicaSet : regionReplicaSets) {
      final TRegionReplicaSet copy = replicaSet.deepCopy();
      final int leaderId = regionLeaderMap.getOrDefault(copy.getRegionId(), -1);
      for (int i = 0; i < copy.getDataNodeLocationsSize(); i++) {
        if (copy.getDataNodeLocations().get(i).getDataNodeId() == leaderId) {
          Collections.swap(copy.getDataNodeLocations(), 0, i);
          break;
        }
      }
      reordered.add(copy);
    }
    return new GetRegionGroupsByTimeResp(status, reordered);
  }

  public TGetRegionGroupsByTimeResp convertToRpcResp() {
    TGetRegionGroupsByTimeResp resp = new TGetRegionGroupsByTimeResp();
    resp.setStatus(status);

    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      resp.setRegionReplicaSets(regionReplicaSets);
    }

    return resp;
  }
}
