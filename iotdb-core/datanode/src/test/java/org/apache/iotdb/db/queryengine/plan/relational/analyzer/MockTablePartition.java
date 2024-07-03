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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MockTablePartition {

  private static final SeriesPartitionExecutor EXECUTOR =
      SeriesPartitionExecutor.getSeriesPartitionExecutor(
          IoTDBDescriptor.getInstance().getConfig().getSeriesPartitionExecutorClass(),
          IoTDBDescriptor.getInstance().getConfig().getSeriesPartitionSlotNum());

  private static final String DB_NAME = "root.testdb";

  private static final String device1 = "root.testdb.d1";
  private static final String device2 = "root.testdb.d22";
  private static final String device3 = "root.testdb.d333";
  private static final String device4 = "root.testdb.d4444";
  private static final String device5 = "root.testdb.d55555";
  private static final String device6 = "root.testdb.d666666";

  public static DataPartition constructDataPartition() {
    TRegionReplicaSet dataRegion1 =
        new TRegionReplicaSet(
            new TConsensusGroupId(TConsensusGroupType.DataRegion, 1),
            Arrays.asList(
                genDataNodeLocation(11, "192.0.1.1"), genDataNodeLocation(12, "192.0.1.2")));

    TRegionReplicaSet dataRegion2 =
        new TRegionReplicaSet(
            new TConsensusGroupId(TConsensusGroupType.DataRegion, 2),
            Arrays.asList(
                genDataNodeLocation(21, "192.0.2.1"), genDataNodeLocation(22, "192.0.2.2")));

    TRegionReplicaSet dataRegion3 =
        new TRegionReplicaSet(
            new TConsensusGroupId(TConsensusGroupType.DataRegion, 3),
            Arrays.asList(
                genDataNodeLocation(31, "192.0.3.1"), genDataNodeLocation(32, "192.0.3.2")));

    TRegionReplicaSet dataRegion4 =
        new TRegionReplicaSet(
            new TConsensusGroupId(TConsensusGroupType.DataRegion, 4),
            Arrays.asList(
                genDataNodeLocation(41, "192.0.4.1"), genDataNodeLocation(42, "192.0.4.2")));

    DataPartition dataPartition =
        new DataPartition(
            IoTDBDescriptor.getInstance().getConfig().getSeriesPartitionExecutorClass(),
            IoTDBDescriptor.getInstance().getConfig().getSeriesPartitionSlotNum());

    Map<String, Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TRegionReplicaSet>>>>
        dataPartitionMap = new HashMap<>();
    Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TRegionReplicaSet>>> sgPartitionMap =
        new HashMap<>();

    List<TRegionReplicaSet> d1DataRegions = new ArrayList<>();
    d1DataRegions.add(dataRegion1);
    d1DataRegions.add(dataRegion2);
    Map<TTimePartitionSlot, List<TRegionReplicaSet>> d1DataRegionMap = new HashMap<>();
    d1DataRegionMap.put(new TTimePartitionSlot(), d1DataRegions);

    List<TRegionReplicaSet> d2DataRegions = new ArrayList<>();
    d2DataRegions.add(dataRegion3);
    Map<TTimePartitionSlot, List<TRegionReplicaSet>> d2DataRegionMap = new HashMap<>();
    d2DataRegionMap.put(new TTimePartitionSlot(), d2DataRegions);

    List<TRegionReplicaSet> d3DataRegions = new ArrayList<>();
    d3DataRegions.add(dataRegion1);
    d3DataRegions.add(dataRegion4);
    Map<TTimePartitionSlot, List<TRegionReplicaSet>> d3DataRegionMap = new HashMap<>();
    d3DataRegionMap.put(new TTimePartitionSlot(), d3DataRegions);

    List<TRegionReplicaSet> d4DataRegions = new ArrayList<>();
    d4DataRegions.add(dataRegion1);
    d4DataRegions.add(dataRegion4);
    Map<TTimePartitionSlot, List<TRegionReplicaSet>> d4DataRegionMap = new HashMap<>();
    d4DataRegionMap.put(new TTimePartitionSlot(), d4DataRegions);

    List<TRegionReplicaSet> d5DataRegions = new ArrayList<>();
    d5DataRegions.add(dataRegion4);
    Map<TTimePartitionSlot, List<TRegionReplicaSet>> d5DataRegionMap = new HashMap<>();
    d5DataRegionMap.put(new TTimePartitionSlot(), d5DataRegions);

    List<TRegionReplicaSet> d6DataRegions = new ArrayList<>();
    d6DataRegions.add(dataRegion1);
    d6DataRegions.add(dataRegion2);
    Map<TTimePartitionSlot, List<TRegionReplicaSet>> d6DataRegionMap = new HashMap<>();
    d6DataRegionMap.put(new TTimePartitionSlot(), d6DataRegions);

    sgPartitionMap.put(EXECUTOR.getSeriesPartitionSlot(device1), d1DataRegionMap);
    sgPartitionMap.put(EXECUTOR.getSeriesPartitionSlot(device2), d2DataRegionMap);
    sgPartitionMap.put(EXECUTOR.getSeriesPartitionSlot(device3), d3DataRegionMap);
    sgPartitionMap.put(EXECUTOR.getSeriesPartitionSlot(device4), d4DataRegionMap);
    sgPartitionMap.put(EXECUTOR.getSeriesPartitionSlot(device5), d5DataRegionMap);
    sgPartitionMap.put(EXECUTOR.getSeriesPartitionSlot(device6), d6DataRegionMap);

    dataPartitionMap.put(DB_NAME, sgPartitionMap);
    dataPartition.setDataPartitionMap(dataPartitionMap);

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
    schemaRegionMap.put(EXECUTOR.getSeriesPartitionSlot(device1), schemaRegion1);
    schemaRegionMap.put(EXECUTOR.getSeriesPartitionSlot(device2), schemaRegion2);
    schemaRegionMap.put(EXECUTOR.getSeriesPartitionSlot(device3), schemaRegion2);
    schemaPartitionMap.put(DB_NAME, schemaRegionMap);
    schemaPartition.setSchemaPartitionMap(schemaPartitionMap);

    return schemaPartition;
  }

  private static TDataNodeLocation genDataNodeLocation(int dataNodeId, String ip) {
    return new TDataNodeLocation()
        .setDataNodeId(dataNodeId)
        .setClientRpcEndPoint(new TEndPoint(ip, 9000))
        .setMPPDataExchangeEndPoint(new TEndPoint(ip, 9001))
        .setInternalEndPoint(new TEndPoint(ip, 9002));
  }
}
