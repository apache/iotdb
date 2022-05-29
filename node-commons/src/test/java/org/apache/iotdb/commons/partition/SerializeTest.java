/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.commons.partition;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;

import org.apache.thrift.TException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class SerializeTest {

  abstract void testSerialize() throws TException, IOException;

  public String seriesPartitionExecutorClass =
      "org.apache.iotdb.commons.partition.executor.hash.APHashExecutor";

  public Map<String, Map<TSeriesPartitionSlot, TRegionReplicaSet>> generateCreateSchemaPartitionMap(
      int startFlag, TConsensusGroupId tConsensusGroupId) {
    // Map<StorageGroup, Map<TSeriesPartitionSlot, TSchemaRegionPlaceInfo>>
    Map<String, Map<TSeriesPartitionSlot, TRegionReplicaSet>> assignedSchemaPartition =
        new HashMap<>();
    Map<TSeriesPartitionSlot, TRegionReplicaSet> relationInfo = new HashMap<>();
    relationInfo.put(
        new TSeriesPartitionSlot(startFlag),
        generateTRegionReplicaSet(startFlag, tConsensusGroupId));
    assignedSchemaPartition.put("root.test.sg", relationInfo);
    return assignedSchemaPartition;
  }

  public TRegionReplicaSet generateTRegionReplicaSet(
      int startFlag, TConsensusGroupId tConsensusGroupId) {
    TRegionReplicaSet tRegionReplicaSet = new TRegionReplicaSet();
    tRegionReplicaSet.setRegionId(tConsensusGroupId);
    List<TDataNodeLocation> dataNodeLocations = new ArrayList<>();
    int locationNum = 5;
    for (int i = startFlag; i < locationNum + startFlag; i++) {
      TDataNodeLocation tDataNodeLocation = new TDataNodeLocation();
      tDataNodeLocation.setDataNodeId(i);
      tDataNodeLocation.setExternalEndPoint(new TEndPoint("127.0.0.1", 6000 + i));
      tDataNodeLocation.setInternalEndPoint(new TEndPoint("127.0.0.1", 7000 + i));
      tDataNodeLocation.setDataBlockManagerEndPoint(new TEndPoint("127.0.0.1", 8000 + i));
      tDataNodeLocation.setDataRegionConsensusEndPoint(new TEndPoint("127.0.0.1", 9000 + i));
      tDataNodeLocation.setSchemaRegionConsensusEndPoint(new TEndPoint("127.0.0.1", 10000 + i));
      dataNodeLocations.add(tDataNodeLocation);
    }
    tRegionReplicaSet.setDataNodeLocations(dataNodeLocations);
    return tRegionReplicaSet;
  }

  public TConsensusGroupId generateTConsensusGroupId(int startFlag) {
    return new TConsensusGroupId(TConsensusGroupType.PartitionRegion, 111000 + startFlag);
  }

  public Map<String, Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TRegionReplicaSet>>>>
      generateCreateDataPartitionMap(int startFlag, TConsensusGroupId tConsensusGroupId) {
    // Map<StorageGroup, Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TRegionMessage>>>>
    Map<String, Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TRegionReplicaSet>>>>
        dataPartitionMap = new HashMap<>();

    Map<TTimePartitionSlot, List<TRegionReplicaSet>> relationInfo = new HashMap<>();
    relationInfo.put(
        new TTimePartitionSlot(System.currentTimeMillis() / 1000),
        Collections.singletonList(generateTRegionReplicaSet(startFlag, tConsensusGroupId)));

    Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TRegionReplicaSet>>> slotInfo =
        new HashMap<>();
    slotInfo.put(new TSeriesPartitionSlot(startFlag), relationInfo);

    dataPartitionMap.put("root.test.data.sg", slotInfo);
    return dataPartitionMap;
  }
}
