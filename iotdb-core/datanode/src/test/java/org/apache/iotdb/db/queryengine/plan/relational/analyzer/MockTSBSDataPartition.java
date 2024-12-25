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

package org.apache.iotdb.db.queryengine.plan.relational.analyzer;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.partition.DataPartition;
import org.apache.iotdb.commons.partition.SchemaPartition;
import org.apache.iotdb.commons.partition.executor.SeriesPartitionExecutor;
import org.apache.iotdb.db.conf.IoTDBDescriptor;

import com.google.common.collect.ImmutableMap;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.TSBSMetadata.DB1;

public class MockTSBSDataPartition {

  private static final SeriesPartitionExecutor EXECUTOR =
      SeriesPartitionExecutor.getSeriesPartitionExecutor(
          IoTDBDescriptor.getInstance().getConfig().getSeriesPartitionExecutorClass(),
          IoTDBDescriptor.getInstance().getConfig().getSeriesPartitionSlotNum());

  private static final String DB_NAME = "root." + DB1;

  static final String T1_DEVICE_1 = "diagnostics.trunk1.South.Rodney.G-2000";
  static final String T1_DEVICE_2 = "diagnostics1.trunk2.South.Rodney.G-1000";
  static final String T1_DEVICE_3 = "diagnostics.trunk2.West.Eric.G-2000";

  static final String T2_DEVICE_1 = "diagnostics.trunk1.South.Rodney.G-2000";
  static final String T2_DEVICE_2 = "diagnostics1.trunk2.South.Rodney.G-1000";
  static final String T2_DEVICE_3 = "diagnostics.trunk2.West.Eric.G-2000";

  static final List<String> T1_DEVICE_1_ATTRIBUTES = Arrays.asList("high", "big");
  static final List<String> T1_DEVICE_2_ATTRIBUTES = Arrays.asList("high", "small");
  static final List<String> T1_DEVICE_3_ATTRIBUTES = Arrays.asList("low", "small");
  static final List<String> T2_DEVICE_1_ATTRIBUTES = Arrays.asList("low", "big");
  static final List<String> T2_DEVICE_2_ATTRIBUTES = Arrays.asList("mid", "big");
  static final List<String> T2_DEVICE_3_ATTRIBUTES = Arrays.asList("mid", "small");

  private static final TRegionReplicaSet DATA_REGION_GROUP_1 = genDataRegionGroup(10, 1, 2);
  private static final TRegionReplicaSet DATA_REGION_GROUP_2 = genDataRegionGroup(11, 3, 2);
  private static final TRegionReplicaSet DATA_REGION_GROUP_3 = genDataRegionGroup(12, 2, 1);

  /*
   * DataPartition:
   *
   * t1_device1(startTime:0): DataRegionGroup_1,
   * t1_device2(startTime:0): DataRegionGroup_2,
   * t1_device3(startTime:0): DataRegionGroup_3,
   *
   * t2_device1(startTime:0): DataRegionGroup_1,
   * t2_device1(startTime:100): DataRegionGroup_2,
   * t2_device2(startTime:0): DataRegionGroup_2,
   * t2_device3(startTime:0): DataRegionGroup_3
   */
  public static DataPartition constructDataPartition() {
    DataPartition dataPartition =
        new DataPartition(
            IoTDBDescriptor.getInstance().getConfig().getSeriesPartitionExecutorClass(),
            IoTDBDescriptor.getInstance().getConfig().getSeriesPartitionSlotNum());

    Map<String, Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TRegionReplicaSet>>>>
        dbPartitionMap = new HashMap<>();
    Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TRegionReplicaSet>>> devicePartitionMap =
        new HashMap<>();

    Map<TTimePartitionSlot, List<TRegionReplicaSet>> dataRegionMap1 =
        Collections.singletonMap(
            new TTimePartitionSlot(0L), Collections.singletonList(DATA_REGION_GROUP_1));
    devicePartitionMap.put(EXECUTOR.getSeriesPartitionSlot(T1_DEVICE_1), dataRegionMap1);

    Map<TTimePartitionSlot, List<TRegionReplicaSet>> dataRegionMap2 =
        ImmutableMap.<TTimePartitionSlot, List<TRegionReplicaSet>>builder()
            .put(new TTimePartitionSlot(0L), Collections.singletonList(DATA_REGION_GROUP_1))
            .put(new TTimePartitionSlot(100L), Collections.singletonList(DATA_REGION_GROUP_2))
            .build();
    devicePartitionMap.put(EXECUTOR.getSeriesPartitionSlot(T2_DEVICE_1), dataRegionMap2);

    Map<TTimePartitionSlot, List<TRegionReplicaSet>> dataRegionMap3 =
        Collections.singletonMap(
            new TTimePartitionSlot(0L), Collections.singletonList(DATA_REGION_GROUP_2));
    devicePartitionMap.put(EXECUTOR.getSeriesPartitionSlot(T1_DEVICE_2), dataRegionMap3);
    devicePartitionMap.put(EXECUTOR.getSeriesPartitionSlot(T2_DEVICE_2), dataRegionMap3);

    Map<TTimePartitionSlot, List<TRegionReplicaSet>> dataRegionMap4 =
        Collections.singletonMap(
            new TTimePartitionSlot(0L), Collections.singletonList(DATA_REGION_GROUP_3));
    devicePartitionMap.put(EXECUTOR.getSeriesPartitionSlot(T1_DEVICE_3), dataRegionMap4);
    devicePartitionMap.put(EXECUTOR.getSeriesPartitionSlot(T2_DEVICE_3), dataRegionMap4);

    dbPartitionMap.put(DB_NAME, devicePartitionMap);
    dataPartition.setDataPartitionMap(dbPartitionMap);

    return dataPartition;
  }

  public static SchemaPartition constructSchemaPartition() {
    SchemaPartition schemaPartition =
        new SchemaPartition(
            IoTDBDescriptor.getInstance().getConfig().getSeriesPartitionExecutorClass(),
            IoTDBDescriptor.getInstance().getConfig().getSeriesPartitionSlotNum());
    Map<String, Map<TSeriesPartitionSlot, TRegionReplicaSet>> schemaPartitionMap = new HashMap<>();

    TRegionReplicaSet schemaRegion1 =
        new TRegionReplicaSet(
            new TConsensusGroupId(TConsensusGroupType.SchemaRegion, 11),
            Arrays.asList(
                genDataNodeLocation(11, "192.0.1.1"), genDataNodeLocation(12, "192.0.1.2")));

    TRegionReplicaSet schemaRegion2 =
        new TRegionReplicaSet(
            new TConsensusGroupId(TConsensusGroupType.SchemaRegion, 21),
            Arrays.asList(
                genDataNodeLocation(21, "192.0.2.1"), genDataNodeLocation(22, "192.0.2.2")));

    Map<TSeriesPartitionSlot, TRegionReplicaSet> schemaRegionMap = new HashMap<>();
    schemaRegionMap.put(EXECUTOR.getSeriesPartitionSlot(T1_DEVICE_1), schemaRegion1);
    schemaRegionMap.put(EXECUTOR.getSeriesPartitionSlot(T1_DEVICE_2), schemaRegion2);
    schemaRegionMap.put(EXECUTOR.getSeriesPartitionSlot(T1_DEVICE_3), schemaRegion2);
    schemaRegionMap.put(EXECUTOR.getSeriesPartitionSlot(T2_DEVICE_1), schemaRegion1);
    schemaRegionMap.put(EXECUTOR.getSeriesPartitionSlot(T2_DEVICE_2), schemaRegion1);
    schemaRegionMap.put(EXECUTOR.getSeriesPartitionSlot(T2_DEVICE_3), schemaRegion2);
    schemaPartitionMap.put(DB_NAME, schemaRegionMap);
    schemaPartition.setSchemaPartitionMap(schemaPartitionMap);

    return schemaPartition;
  }

  private static TRegionReplicaSet genDataRegionGroup(
      int regionGroupId, int dataNodeId1, int dataNodeId2) {
    return new TRegionReplicaSet(
        new TConsensusGroupId(TConsensusGroupType.DataRegion, regionGroupId),
        Arrays.asList(
            genDataNodeLocation(dataNodeId1, String.format("192.0.%s.1", regionGroupId)),
            genDataNodeLocation(dataNodeId2, String.format("192.0.%s.2", regionGroupId))));
  }

  private static TDataNodeLocation genDataNodeLocation(int dataNodeId, String ip) {
    return new TDataNodeLocation()
        .setDataNodeId(dataNodeId)
        .setClientRpcEndPoint(new TEndPoint(ip, 9000))
        .setMPPDataExchangeEndPoint(new TEndPoint(ip, 9001))
        .setInternalEndPoint(new TEndPoint(ip, 9002));
  }
}
