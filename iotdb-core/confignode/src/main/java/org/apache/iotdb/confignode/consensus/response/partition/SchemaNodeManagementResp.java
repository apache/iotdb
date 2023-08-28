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
import org.apache.iotdb.common.rpc.thrift.TSchemaNode;
import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.commons.partition.SchemaPartitionTable;
import org.apache.iotdb.confignode.rpc.thrift.TSchemaNodeManagementResp;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.rpc.TSStatusCode;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class SchemaNodeManagementResp implements DataSet {

  private TSStatus status;

  // Map<StorageGroup, SchemaPartitionTable>
  // TODO: Replace this map with new SchemaPartition
  private Map<String, SchemaPartitionTable> schemaPartition;

  private Set<TSchemaNode> matchedNode;

  public SchemaNodeManagementResp() {
    // empty constructor
  }

  public TSStatus getStatus() {
    return status;
  }

  public void setStatus(TSStatus status) {
    this.status = status;
  }

  public void setSchemaPartition(Map<String, SchemaPartitionTable> schemaPartition) {
    this.schemaPartition = schemaPartition;
  }

  public void setMatchedNode(Set<TSchemaNode> matchedNode) {
    this.matchedNode = matchedNode;
  }

  public TSchemaNodeManagementResp convertToRpcSchemaNodeManagementPartitionResp(
      Map<TConsensusGroupId, TRegionReplicaSet> replicaSetMap) {
    TSchemaNodeManagementResp resp = new TSchemaNodeManagementResp();
    resp.setStatus(status);
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      resp.setMatchedNode(matchedNode);

      Map<String, Map<TSeriesPartitionSlot, TRegionReplicaSet>> schemaPartitionMap =
          new ConcurrentHashMap<>();

      schemaPartition.forEach(
          (storageGroup, schemaPartitionTable) -> {
            Map<TSeriesPartitionSlot, TRegionReplicaSet> seriesPartitionSlotMap =
                new ConcurrentHashMap<>();

            schemaPartitionTable
                .getSchemaPartitionMap()
                .forEach(
                    (seriesPartitionSlot, consensusGroupId) ->
                        seriesPartitionSlotMap.put(
                            seriesPartitionSlot, replicaSetMap.get(consensusGroupId)));

            schemaPartitionMap.put(storageGroup, seriesPartitionSlotMap);
          });

      resp.setSchemaRegionMap(schemaPartitionMap);
    }

    return resp;
  }
}
