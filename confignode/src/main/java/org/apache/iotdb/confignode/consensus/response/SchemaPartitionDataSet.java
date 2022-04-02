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

package org.apache.iotdb.confignode.consensus.response;

import org.apache.iotdb.commons.partition.SchemaPartition;
import org.apache.iotdb.confignode.rpc.thrift.RegionReplicaSet;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.service.rpc.thrift.TEndpoint;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SchemaPartitionDataSet implements DataSet {
  private SchemaPartition schemaPartitionInfo;

  public SchemaPartition getSchemaPartitionInfo() {
    return schemaPartitionInfo;
  }

  public void setSchemaPartitionInfo(SchemaPartition schemaPartitionInfos) {
    this.schemaPartitionInfo = schemaPartitionInfos;
  }

  public static org.apache.iotdb.confignode.rpc.thrift.SchemaPartitionInfo
      convertRpcSchemaPartition(SchemaPartition schemaPartitionInfo) {
    org.apache.iotdb.confignode.rpc.thrift.SchemaPartitionInfo rpcSchemaPartitionInfo =
        new org.apache.iotdb.confignode.rpc.thrift.SchemaPartitionInfo();

    Map<String, Map<Integer, RegionReplicaSet>> schemaRegionReplicaSets = new HashMap<>();

    schemaPartitionInfo.getSchemaPartition().entrySet().stream()
        .forEach(
            entity -> {
              schemaRegionReplicaSets.putIfAbsent(entity.getKey(), new HashMap<>());
              entity
                  .getValue()
                  .entrySet()
                  .forEach(
                      replica -> {
                        RegionReplicaSet regionReplicaSet = new RegionReplicaSet();
                        regionReplicaSet.setRegionId(replica.getValue().getId().getId());
                        List<TEndpoint> endPoints = new ArrayList<>();
                        replica
                            .getValue()
                            .getDataNodeList()
                            .forEach(
                                dataNode -> {
                                  TEndpoint endPoint = new TEndpoint(dataNode.getEndPoint().getIp(), dataNode.getEndPoint().getPort());
                                  endPoints.add(endPoint);
                                });
                        regionReplicaSet.setEndpoint(endPoints);
                        schemaRegionReplicaSets
                            .get(entity.getKey())
                            .put(replica.getKey().getDeviceGroupId(), regionReplicaSet);
                      });
            });
    rpcSchemaPartitionInfo.setSchemaRegionDataNodesMap(schemaRegionReplicaSets);
    return rpcSchemaPartitionInfo;
  }
}
