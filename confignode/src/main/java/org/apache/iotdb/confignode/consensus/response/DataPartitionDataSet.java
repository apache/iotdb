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
import org.apache.iotdb.confignode.rpc.thrift.DataPartitionInfo;
import org.apache.iotdb.confignode.rpc.thrift.RegionReplicaSet;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.service.rpc.thrift.EndPoint;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DataPartitionDataSet implements DataSet {
  private DataPartition dataPartitionInfo;

  public DataPartition getDataPartitionInfo() {
    return dataPartitionInfo;
  }

  public void setDataPartitionInfos(DataPartition dataPartitionInfo) {
    this.dataPartitionInfo = dataPartitionInfo;
  }

  public DataPartitionInfo convertRpcDataPartitionInfo() {
    DataPartitionInfo rpcDataPartitionInfo = new DataPartitionInfo();
    Map<String, Map<Integer, Map<Long, List<RegionReplicaSet>>>> deviceGroupStartTimeDataRegionGroupMap = new HashMap<>();

    dataPartitionInfo.getDataPartitionMap().forEach(((storageGroup, seriesPartitionSlotTimePartitionSlotRegionReplicaSetListMap) -> {
      deviceGroupStartTimeDataRegionGroupMap.putIfAbsent(storageGroup, new HashMap<>());
      seriesPartitionSlotTimePartitionSlotRegionReplicaSetListMap.forEach(((seriesPartitionSlot, timePartitionSlotReplicaSetListMap) -> {
        deviceGroupStartTimeDataRegionGroupMap.get(storageGroup).putIfAbsent(seriesPartitionSlot.getDeviceGroupId(), new HashMap<>());
        timePartitionSlotReplicaSetListMap.forEach(((timePartitionSlot, regionReplicaSetList) -> {
          List<RegionReplicaSet> rpcRegionReplicaSetList = new ArrayList<>();
          regionReplicaSetList.forEach(regionReplicaSet -> {
            RegionReplicaSet rpcRegionReplicaSet = new RegionReplicaSet();
            rpcRegionReplicaSet.setRegionId(regionReplicaSet.getId().getId());
            List<EndPoint> endPointList = new ArrayList<>();
            regionReplicaSet.getDataNodeList().forEach(dataNodeLocation -> endPointList.add(new EndPoint(dataNodeLocation.getEndPoint().getIp(), dataNodeLocation.getEndPoint().getPort())));
            rpcRegionReplicaSet.setEndpoint(endPointList);
            rpcRegionReplicaSetList.add(rpcRegionReplicaSet);
          });
          deviceGroupStartTimeDataRegionGroupMap.get(storageGroup).get(seriesPartitionSlot.getDeviceGroupId()).put(timePartitionSlot.getStartTime(), rpcRegionReplicaSetList);
        }));
      }));
    }));

    rpcDataPartitionInfo.setDeviceGroupStartTimeDataRegionGroupMap(deviceGroupStartTimeDataRegionGroupMap);
    return rpcDataPartitionInfo;
  }
}
