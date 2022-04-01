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

import org.apache.iotdb.confignode.partition.SchemaPartitionInfo;
import org.apache.iotdb.confignode.rpc.thrift.RegionReplicaSet;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.service.rpc.thrift.EndPoint;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SchemaPartitionDataSet implements DataSet {
  private SchemaPartitionInfo schemaPartitionInfo;

  public SchemaPartitionInfo getSchemaPartitionInfo() {
    return schemaPartitionInfo;
  }

  public void setSchemaPartitionInfo(SchemaPartitionInfo schemaPartitionInfos) {
    this.schemaPartitionInfo = schemaPartitionInfos;
  }

  public static org.apache.iotdb.confignode.rpc.thrift.SchemaPartitionInfo
      convertRpcSchemaPartition(SchemaPartitionInfo schemaPartitionInfo) {
    org.apache.iotdb.confignode.rpc.thrift.SchemaPartitionInfo rpcSchemaPartitionInfo =
        new org.apache.iotdb.confignode.rpc.thrift.SchemaPartitionInfo();

    Map<String, Map<Integer, RegionReplicaSet>> schemaRegionRelicatSets = new HashMap<>();

    schemaPartitionInfo.getSchemaPartitionInfo().entrySet().stream()
        .forEach(
            entity -> {
              schemaRegionRelicatSets.putIfAbsent(entity.getKey(), new HashMap<>());
              entity
                  .getValue()
                  .entrySet()
                  .forEach(
                      replica -> {
                        RegionReplicaSet regionReplicaSet = new RegionReplicaSet();
                        regionReplicaSet.setRegionId(replica.getKey());
                        List<EndPoint> endPoints = new ArrayList<>();
                        replica
                            .getValue()
                            .getEndPointList()
                            .forEach(
                                point -> {
                                  EndPoint endPoint = new EndPoint(point.getIp(), point.getPort());
                                  endPoints.add(endPoint);
                                });
                        regionReplicaSet.setEndpoint(endPoints);
                        schemaRegionRelicatSets
                            .get(entity.getKey())
                            .put(replica.getKey(), regionReplicaSet);
                      });
            });
    rpcSchemaPartitionInfo.setSchemaRegionDataNodesMap(schemaRegionRelicatSets);
    return rpcSchemaPartitionInfo;
  }
}
