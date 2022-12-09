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

package org.apache.iotdb.db.mpp.plan.analyze;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.partition.DataPartition;
import org.apache.iotdb.commons.partition.DataPartitionQueryParam;
import org.apache.iotdb.commons.partition.SchemaNodeManagementPartition;
import org.apache.iotdb.commons.partition.SchemaPartition;
import org.apache.iotdb.commons.partition.executor.SeriesPartitionExecutor;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.mpp.rpc.thrift.TRegionRouteReq;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FakePartitionFetcherImpl implements IPartitionFetcher {

  private final String seriesSlotExecutorName =
      IoTDBDescriptor.getInstance().getConfig().getSeriesPartitionExecutorClass();

  private final int seriesPartitionSlotNum =
      IoTDBDescriptor.getInstance().getConfig().getSeriesPartitionSlotNum();

  private final SeriesPartitionExecutor partitionExecutor =
      SeriesPartitionExecutor.getSeriesPartitionExecutor(
          seriesSlotExecutorName, seriesPartitionSlotNum);

  @Override
  public SchemaPartition getSchemaPartition(PathPatternTree patternTree) {
    String device1 = "root.sg.d1";
    String device2 = "root.sg.d22";
    String device3 = "root.sg.d333";

    SchemaPartition schemaPartition =
        new SchemaPartition(
            IoTDBDescriptor.getInstance().getConfig().getSeriesPartitionExecutorClass(),
            IoTDBDescriptor.getInstance().getConfig().getSeriesPartitionSlotNum());
    Map<String, Map<TSeriesPartitionSlot, TRegionReplicaSet>> schemaPartitionMap = new HashMap<>();

    Map<TSeriesPartitionSlot, TRegionReplicaSet> regionMap = new HashMap<>();
    TRegionReplicaSet region1 =
        new TRegionReplicaSet(
            new TConsensusGroupId(TConsensusGroupType.SchemaRegion, 1),
            Arrays.asList(
                new TDataNodeLocation()
                    .setDataNodeId(11)
                    .setClientRpcEndPoint(new TEndPoint("192.0.1.1", 9000)),
                new TDataNodeLocation()
                    .setDataNodeId(12)
                    .setClientRpcEndPoint(new TEndPoint("192.0.1.2", 9000))));
    regionMap.put(new TSeriesPartitionSlot(device1.length()), region1);

    TRegionReplicaSet region2 =
        new TRegionReplicaSet(
            new TConsensusGroupId(TConsensusGroupType.SchemaRegion, 2),
            Arrays.asList(
                new TDataNodeLocation()
                    .setDataNodeId(31)
                    .setClientRpcEndPoint(new TEndPoint("192.0.3.1", 9000)),
                new TDataNodeLocation()
                    .setDataNodeId(32)
                    .setClientRpcEndPoint(new TEndPoint("192.0.3.2", 9000))));
    regionMap.put(new TSeriesPartitionSlot(device2.length()), region2);

    TRegionReplicaSet region3 =
        new TRegionReplicaSet(
            new TConsensusGroupId(TConsensusGroupType.SchemaRegion, 3),
            Arrays.asList(
                new TDataNodeLocation()
                    .setDataNodeId(11)
                    .setClientRpcEndPoint(new TEndPoint("192.0.1.1", 9000)),
                new TDataNodeLocation()
                    .setDataNodeId(12)
                    .setClientRpcEndPoint(new TEndPoint("192.0.1.2", 9000))));
    regionMap.put(new TSeriesPartitionSlot(device3.length()), region3);

    schemaPartitionMap.put("root.sg", regionMap);

    schemaPartition.setSchemaPartitionMap(schemaPartitionMap);

    return schemaPartition;
  }

  @Override
  public SchemaPartition getOrCreateSchemaPartition(PathPatternTree patternTree) {
    return null;
  }

  @Override
  public SchemaNodeManagementPartition getSchemaNodeManagementPartitionWithLevel(
      PathPatternTree patternTree, Integer level) {
    return null;
  }

  @Override
  public DataPartition getDataPartition(
      Map<String, List<DataPartitionQueryParam>> sgNameToQueryParamsMap) {
    String device1 = "root.sg.d1";
    String device2 = "root.sg.d22";
    String device3 = "root.sg.d333";

    DataPartition dataPartition =
        new DataPartition(
            IoTDBDescriptor.getInstance().getConfig().getSeriesPartitionExecutorClass(),
            IoTDBDescriptor.getInstance().getConfig().getSeriesPartitionSlotNum());
    Map<String, Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TRegionReplicaSet>>>>
        dataPartitionMap = new HashMap<>();
    Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TRegionReplicaSet>>> sgPartitionMap =
        new HashMap<>();

    List<TRegionReplicaSet> d1DataRegions = new ArrayList<>();
    d1DataRegions.add(
        new TRegionReplicaSet(
            new TConsensusGroupId(TConsensusGroupType.DataRegion, 1),
            Arrays.asList(
                new TDataNodeLocation()
                    .setDataNodeId(11)
                    .setClientRpcEndPoint(new TEndPoint("192.0.1.1", 9000)),
                new TDataNodeLocation()
                    .setDataNodeId(12)
                    .setClientRpcEndPoint(new TEndPoint("192.0.1.2", 9000)))));
    d1DataRegions.add(
        new TRegionReplicaSet(
            new TConsensusGroupId(TConsensusGroupType.DataRegion, 2),
            Arrays.asList(
                new TDataNodeLocation()
                    .setDataNodeId(21)
                    .setClientRpcEndPoint(new TEndPoint("192.0.2.1", 9000)),
                new TDataNodeLocation()
                    .setDataNodeId(22)
                    .setClientRpcEndPoint(new TEndPoint("192.0.2.2", 9000)))));
    Map<TTimePartitionSlot, List<TRegionReplicaSet>> d1DataRegionMap = new HashMap<>();
    d1DataRegionMap.put(new TTimePartitionSlot(), d1DataRegions);

    List<TRegionReplicaSet> d2DataRegions = new ArrayList<>();
    d2DataRegions.add(
        new TRegionReplicaSet(
            new TConsensusGroupId(TConsensusGroupType.DataRegion, 3),
            Arrays.asList(
                new TDataNodeLocation()
                    .setDataNodeId(31)
                    .setClientRpcEndPoint(new TEndPoint("192.0.3.1", 9000)),
                new TDataNodeLocation()
                    .setDataNodeId(32)
                    .setClientRpcEndPoint(new TEndPoint("192.0.3.2", 9000)))));
    Map<TTimePartitionSlot, List<TRegionReplicaSet>> d2DataRegionMap = new HashMap<>();
    d2DataRegionMap.put(new TTimePartitionSlot(), d2DataRegions);

    List<TRegionReplicaSet> d3DataRegions = new ArrayList<>();
    d3DataRegions.add(
        new TRegionReplicaSet(
            new TConsensusGroupId(TConsensusGroupType.DataRegion, 1),
            Arrays.asList(
                new TDataNodeLocation()
                    .setDataNodeId(11)
                    .setClientRpcEndPoint(new TEndPoint("192.0.1.1", 9000)),
                new TDataNodeLocation()
                    .setDataNodeId(12)
                    .setClientRpcEndPoint(new TEndPoint("192.0.1.2", 9000)))));
    d3DataRegions.add(
        new TRegionReplicaSet(
            new TConsensusGroupId(TConsensusGroupType.DataRegion, 4),
            Arrays.asList(
                new TDataNodeLocation()
                    .setDataNodeId(41)
                    .setClientRpcEndPoint(new TEndPoint("192.0.4.1", 9000)),
                new TDataNodeLocation()
                    .setDataNodeId(42)
                    .setClientRpcEndPoint(new TEndPoint("192.0.4.2", 9000)))));
    Map<TTimePartitionSlot, List<TRegionReplicaSet>> d3DataRegionMap = new HashMap<>();
    d3DataRegionMap.put(new TTimePartitionSlot(), d3DataRegions);

    sgPartitionMap.put(new TSeriesPartitionSlot(device1.length()), d1DataRegionMap);
    sgPartitionMap.put(new TSeriesPartitionSlot(device2.length()), d2DataRegionMap);
    sgPartitionMap.put(new TSeriesPartitionSlot(device3.length()), d3DataRegionMap);

    dataPartitionMap.put("root.sg", sgPartitionMap);

    dataPartition.setDataPartitionMap(dataPartitionMap);

    return dataPartition;
  }

  @Override
  public DataPartition getDataPartitionWithUnclosedTimeRange(
      Map<String, List<DataPartitionQueryParam>> sgNameToQueryParamsMap) {
    return getDataPartition(sgNameToQueryParamsMap);
  }

  @Override
  public DataPartition getOrCreateDataPartition(
      Map<String, List<DataPartitionQueryParam>> sgNameToQueryParamsMap) {
    return null;
  }

  @Override
  public DataPartition getOrCreateDataPartition(
      List<DataPartitionQueryParam> dataPartitionQueryParams) {

    // only test root.sg
    DataPartition dataPartition =
        new DataPartition(
            IoTDBDescriptor.getInstance().getConfig().getSeriesPartitionExecutorClass(),
            IoTDBDescriptor.getInstance().getConfig().getSeriesPartitionSlotNum());

    Map<String, Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TRegionReplicaSet>>>>
        dataPartitionMap = new HashMap<>();
    Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TRegionReplicaSet>>> sgPartitionMap =
        new HashMap<>();
    dataPartitionMap.put("root.sg", sgPartitionMap);
    dataPartition.setDataPartitionMap(dataPartitionMap);

    List<TRegionReplicaSet> d1DataRegions = new ArrayList<>();
    d1DataRegions.add(
        new TRegionReplicaSet(
            new TConsensusGroupId(TConsensusGroupType.DataRegion, 1),
            Arrays.asList(
                new TDataNodeLocation()
                    .setDataNodeId(11)
                    .setClientRpcEndPoint(new TEndPoint("192.0.1.1", 9000)),
                new TDataNodeLocation()
                    .setDataNodeId(12)
                    .setClientRpcEndPoint(new TEndPoint("192.0.1.2", 9000)))));
    d1DataRegions.add(
        new TRegionReplicaSet(
            new TConsensusGroupId(TConsensusGroupType.DataRegion, 2),
            Arrays.asList(
                new TDataNodeLocation()
                    .setDataNodeId(21)
                    .setClientRpcEndPoint(new TEndPoint("192.0.2.1", 9000)),
                new TDataNodeLocation()
                    .setDataNodeId(22)
                    .setClientRpcEndPoint(new TEndPoint("192.0.2.2", 9000)))));

    List<TRegionReplicaSet> d2DataRegions = new ArrayList<>();
    d2DataRegions.add(
        new TRegionReplicaSet(
            new TConsensusGroupId(TConsensusGroupType.DataRegion, 3),
            Arrays.asList(
                new TDataNodeLocation()
                    .setDataNodeId(31)
                    .setClientRpcEndPoint(new TEndPoint("192.0.3.1", 9000)),
                new TDataNodeLocation()
                    .setDataNodeId(32)
                    .setClientRpcEndPoint(new TEndPoint("192.0.3.2", 9000)))));
    Map<TTimePartitionSlot, List<TRegionReplicaSet>> d2DataRegionMap = new HashMap<>();
    d2DataRegionMap.put(new TTimePartitionSlot(), d2DataRegions);

    for (DataPartitionQueryParam dataPartitionQueryParam : dataPartitionQueryParams) {
      TSeriesPartitionSlot seriesPartitionSlot =
          partitionExecutor.getSeriesPartitionSlot(dataPartitionQueryParam.getDevicePath());
      Map<TTimePartitionSlot, List<TRegionReplicaSet>> timePartitionSlotListMap =
          sgPartitionMap.computeIfAbsent(seriesPartitionSlot, k -> new HashMap<>());
      for (TTimePartitionSlot timePartitionSlot :
          dataPartitionQueryParam.getTimePartitionSlotList()) {
        if (timePartitionSlot.startTime == 0) {
          timePartitionSlotListMap.put(timePartitionSlot, d1DataRegions);
        } else {
          timePartitionSlotListMap.put(timePartitionSlot, d2DataRegions);
        }
      }
    }
    return dataPartition;
  }

  @Override
  public boolean updateRegionCache(TRegionRouteReq req) {
    return true;
  }

  @Override
  public void invalidAllCache() {}
}
