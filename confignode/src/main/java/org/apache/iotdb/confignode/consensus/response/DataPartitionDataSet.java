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
import org.apache.iotdb.confignode.rpc.thrift.DataPartitionResp;
import org.apache.iotdb.confignode.rpc.thrift.RegionMessage;
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

  public void convertToRpcDataPartitionResp(DataPartitionResp resp) {
    Map<String, Map<Integer, Map<Long, List<RegionMessage>>>> dataPartitionMap = new HashMap<>();

    dataPartition
        .getDataPartitionMap()
        .forEach(
            ((storageGroup, seriesPartitionSlotTimePartitionSlotRegionReplicaSetListMap) -> {
              // Extract StorageGroupName
              dataPartitionMap.putIfAbsent(storageGroup, new HashMap<>());

              seriesPartitionSlotTimePartitionSlotRegionReplicaSetListMap.forEach(
                  ((seriesPartitionSlot, timePartitionSlotReplicaSetListMap) -> {
                    // Extract SeriesPartitionSlot
                    dataPartitionMap
                        .get(storageGroup)
                        .putIfAbsent(seriesPartitionSlot.getDeviceGroupId(), new HashMap<>());

                    // Extract Map<TimePartitionSlot, List<RegionReplicaSet>>
                    timePartitionSlotReplicaSetListMap.forEach(
                        ((timePartitionSlot, regionReplicaSetList) -> {
                          List<RegionMessage> regionMessages = new ArrayList<>();
                          regionReplicaSetList.forEach(
                              regionReplicaSet -> {
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
                                regionMessages.add(regionMessage);
                              });
                          dataPartitionMap
                              .get(storageGroup)
                              .get(seriesPartitionSlot.getDeviceGroupId())
                              .put(timePartitionSlot.getStartTime(), regionMessages);
                        }));
                  }));
            }));

    resp.setDataPartitionMap(dataPartitionMap);
  }
}
