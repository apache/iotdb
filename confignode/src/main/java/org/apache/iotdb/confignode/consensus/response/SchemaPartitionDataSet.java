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
import org.apache.iotdb.confignode.rpc.thrift.RegionMessage;
import org.apache.iotdb.confignode.rpc.thrift.SchemaPartitionResp;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.service.rpc.thrift.EndPoint;
import org.apache.iotdb.service.rpc.thrift.TSStatus;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SchemaPartitionDataSet implements DataSet {

  private TSStatus status;

  private SchemaPartition schemaPartition;

  public SchemaPartitionDataSet() {
    // empty constructor
  }

  public TSStatus getStatus() {
    return status;
  }

  public void setStatus(TSStatus status) {
    this.status = status;
  }

  public SchemaPartition getSchemaPartition() {
    return schemaPartition;
  }

  public void setSchemaPartition(SchemaPartition schemaPartition) {
    this.schemaPartition = schemaPartition;
  }

  public void convertToRpcSchemaPartitionResp(SchemaPartitionResp resp) {
    Map<String, Map<Integer, RegionMessage>> schemaRegionMap = new HashMap<>();

    schemaPartition
        .getSchemaPartitionMap()
        .forEach(
            (storageGroup, seriesPartitionSlotRegionReplicaSetMap) -> {
              // Extract StorageGroupName
              schemaRegionMap.putIfAbsent(storageGroup, new HashMap<>());

              // Extract Map<SeriesPartitionSlot, RegionReplicaSet>
              seriesPartitionSlotRegionReplicaSetMap.forEach(
                  ((seriesPartitionSlot, regionReplicaSet) -> {
                    RegionMessage regionMessage = new RegionMessage();
                    regionMessage.setRegionId(regionReplicaSet.getId().getId());
                    List<EndPoint> endPointList = new ArrayList<>();
                    regionReplicaSet
                        .getDataNodeList()
                        .forEach(
                            dataNodeLocation ->
                                endPointList.add(
                                    new EndPoint(
                                        dataNodeLocation.getEndPoint().getIp(),
                                        dataNodeLocation.getEndPoint().getPort())));
                    regionMessage.setEndpoint(endPointList);
                    schemaRegionMap
                        .get(storageGroup)
                        .put(seriesPartitionSlot.getDeviceGroupId(), regionMessage);
                  }));
            });

    resp.setSchemaRegionMap(schemaRegionMap);
  }
}
