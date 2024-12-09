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
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.utils.Binary;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MockTableModelDataPartition {

  private static final SeriesPartitionExecutor EXECUTOR =
      SeriesPartitionExecutor.getSeriesPartitionExecutor(
          IoTDBDescriptor.getInstance().getConfig().getSeriesPartitionExecutorClass(),
          IoTDBDescriptor.getInstance().getConfig().getSeriesPartitionSlotNum());

  private static final String DB_NAME = "root.testdb";

  static final String DEVICE_1 = "table1.beijing.A1.ZZ";
  static final String DEVICE_2 = "table1.beijing.A2.XX";
  static final String DEVICE_3 = "table1.shanghai.A3.YY";
  static final String DEVICE_4 = "table1.shanghai.B3.YY";
  static final String DEVICE_5 = "table1.shenzhen.B2.ZZ";
  static final String DEVICE_6 = "table1.shenzhen.B1.XX";

  static final List<Binary> DEVICE_1_ATTRIBUTES =
      Arrays.asList(
          new Binary("high", TSFileConfig.STRING_CHARSET),
          new Binary("big", TSFileConfig.STRING_CHARSET));
  static final List<Binary> DEVICE_2_ATTRIBUTES =
      Arrays.asList(
          new Binary("high", TSFileConfig.STRING_CHARSET),
          new Binary("small", TSFileConfig.STRING_CHARSET));
  static final List<Binary> DEVICE_3_ATTRIBUTES =
      Arrays.asList(
          new Binary("low", TSFileConfig.STRING_CHARSET),
          new Binary("small", TSFileConfig.STRING_CHARSET));
  static final List<Binary> DEVICE_4_ATTRIBUTES =
      Arrays.asList(
          new Binary("low", TSFileConfig.STRING_CHARSET),
          new Binary("big", TSFileConfig.STRING_CHARSET));
  static final List<Binary> DEVICE_5_ATTRIBUTES =
      Arrays.asList(
          new Binary("mid", TSFileConfig.STRING_CHARSET),
          new Binary("big", TSFileConfig.STRING_CHARSET));
  static final List<Binary> DEVICE_6_ATTRIBUTES =
      Arrays.asList(
          new Binary("mid", TSFileConfig.STRING_CHARSET),
          new Binary("small", TSFileConfig.STRING_CHARSET));

  private static final TRegionReplicaSet DATA_REGION_GROUP_1 = genDataRegionGroup(10, 1, 2);
  private static final TRegionReplicaSet DATA_REGION_GROUP_2 = genDataRegionGroup(11, 3, 2);
  private static final TRegionReplicaSet DATA_REGION_GROUP_3 = genDataRegionGroup(12, 2, 1);
  private static final TRegionReplicaSet DATA_REGION_GROUP_4 = genDataRegionGroup(13, 1, 3);

  /*
   * DataPartition:
   *
   * device1(startTime:0): DataRegionGroup_1,
   * device2(startTime:0): DataRegionGroup_1,
   * device3(startTime:0): DataRegionGroup_2,
   * device4(startTime:0): DataRegionGroup_2,
   * device5(startTime:0): DataRegionGroup_2,
   * device5(startTime:100): DataRegionGroup_3,
   * device6(startTime:0): DataRegionGroup_2,
   * device6(startTime:100): DataRegionGroup_3,
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

    List<TRegionReplicaSet> regionGroup1 = Collections.singletonList(DATA_REGION_GROUP_1);
    Map<TTimePartitionSlot, List<TRegionReplicaSet>> dataRegionMap1 =
        Collections.singletonMap(new TTimePartitionSlot(0L), regionGroup1);
    devicePartitionMap.put(EXECUTOR.getSeriesPartitionSlot(DEVICE_1), dataRegionMap1);
    devicePartitionMap.put(EXECUTOR.getSeriesPartitionSlot(DEVICE_2), dataRegionMap1);

    List<TRegionReplicaSet> regionGroup2 = Collections.singletonList(DATA_REGION_GROUP_2);
    Map<TTimePartitionSlot, List<TRegionReplicaSet>> dataRegionMap2 =
        Collections.singletonMap(new TTimePartitionSlot(0L), regionGroup2);
    devicePartitionMap.put(EXECUTOR.getSeriesPartitionSlot(DEVICE_3), dataRegionMap2);
    devicePartitionMap.put(EXECUTOR.getSeriesPartitionSlot(DEVICE_4), dataRegionMap2);

    List<TRegionReplicaSet> regionGroup3 = Collections.singletonList(DATA_REGION_GROUP_2);
    List<TRegionReplicaSet> regionGroup4 = Collections.singletonList(DATA_REGION_GROUP_3);
    Map<TTimePartitionSlot, List<TRegionReplicaSet>> dataRegionMap3 =
        ImmutableMap.<TTimePartitionSlot, List<TRegionReplicaSet>>builder()
            .put(new TTimePartitionSlot(0L), regionGroup3)
            .put(new TTimePartitionSlot(100L), regionGroup4)
            .build();
    devicePartitionMap.put(EXECUTOR.getSeriesPartitionSlot(DEVICE_5), dataRegionMap3);
    devicePartitionMap.put(EXECUTOR.getSeriesPartitionSlot(DEVICE_6), dataRegionMap3);

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
    schemaRegionMap.put(EXECUTOR.getSeriesPartitionSlot(DEVICE_1), schemaRegion1);
    schemaRegionMap.put(EXECUTOR.getSeriesPartitionSlot(DEVICE_2), schemaRegion2);
    schemaRegionMap.put(EXECUTOR.getSeriesPartitionSlot(DEVICE_3), schemaRegion2);
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

  public static TDataNodeLocation genDataNodeLocation(int dataNodeId, String ip) {
    return new TDataNodeLocation()
        .setDataNodeId(dataNodeId)
        .setClientRpcEndPoint(new TEndPoint(ip, 9000))
        .setMPPDataExchangeEndPoint(new TEndPoint(ip, 9001))
        .setInternalEndPoint(new TEndPoint(ip, 9002));
  }
}
