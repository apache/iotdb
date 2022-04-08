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

import org.apache.iotdb.commons.partition.DataPartition;
import org.apache.iotdb.confignode.rpc.thrift.TDataPartitionResp;
import org.apache.iotdb.confignode.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.confignode.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.confignode.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.service.rpc.thrift.EndPoint;
import org.apache.iotdb.service.rpc.thrift.TSStatus;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DataPartitionDataSet implements DataSet {

  private TSStatus status;

  private DataPartition dataPartition;

  public TSStatus getStatus() {
    return status;
  }

  public void setStatus(TSStatus status) {
    this.status = status;
  }

  public DataPartition getDataPartition() {
    return dataPartition;
  }

  public void setDataPartition(DataPartition dataPartition) {
    this.dataPartition = dataPartition;
  }

  /**
   * Convert DataPartitionDataSet to TDataPartitionResp
   *
   * @param resp TDataPartitionResp
   */
  public void convertToRpcDataPartitionResp(TDataPartitionResp resp) {
    Map<String, Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TRegionReplicaSet>>>> dataPartitionMap = new HashMap<>();

    dataPartition
        .getDataPartitionMap()
        .forEach(
            ((storageGroup, seriesPartitionSlotTimePartitionSlotRegionReplicaSetListMap) -> {
              // Extract StorageGroupName
              dataPartitionMap.putIfAbsent(storageGroup, new HashMap<>());

              seriesPartitionSlotTimePartitionSlotRegionReplicaSetListMap.forEach(
                  ((seriesPartitionSlot, timePartitionSlotReplicaSetListMap) -> {
                    // Extract TSeriesPartitionSlot
                    TSeriesPartitionSlot tSeriesPartitionSlot = new TSeriesPartitionSlot(seriesPartitionSlot.getDeviceGroupId());
                    dataPartitionMap.get(storageGroup).putIfAbsent(tSeriesPartitionSlot, new HashMap<>());

                    // Extract Map<TimePartitionSlot, List<RegionReplicaSet>>
                    timePartitionSlotReplicaSetListMap.forEach(
                        ((timePartitionSlot, regionReplicaSets) -> {
                          // Extract TTimePartitionSlot
                          TTimePartitionSlot tTimePartitionSlot = new TTimePartitionSlot(timePartitionSlot.getStartTime());
                          dataPartitionMap.get(storageGroup).get(tSeriesPartitionSlot).putIfAbsent(tTimePartitionSlot, new ArrayList<>());

                          // Extract TRegionReplicaSets
                          regionReplicaSets.forEach(
                              regionReplicaSet -> {
                                TRegionReplicaSet tRegionReplicaSet = new TRegionReplicaSet();

                                // Set TRegionReplicaSet's RegionId
                                tRegionReplicaSet.setRegionId(regionReplicaSet.getId().getId());

                                // Set TRegionReplicaSet's GroupType
                                tRegionReplicaSet.setGroupType("DataRegion");

                                // Set TRegionReplicaSet's EndPoints
                                List<EndPoint> endPointList = new ArrayList<>();
                                regionReplicaSet
                                    .getDataNodeList()
                                    .forEach(
                                        dataNodeLocation ->
                                            endPointList.add(
                                                new EndPoint(
                                                    dataNodeLocation.getEndPoint().getIp(),
                                                    dataNodeLocation.getEndPoint().getPort())));
                                tRegionReplicaSet.setEndpoint(endPointList);

                                dataPartitionMap.get(storageGroup).get(tSeriesPartitionSlot).get(tTimePartitionSlot).add(tRegionReplicaSet);
                              });
                        }));
                  }));
            }));

    resp.setDataPartitionMap(dataPartitionMap);
  }
}
