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

package org.apache.iotdb.db.mpp.sql.analyze;

import org.apache.iotdb.commons.cluster.DataNodeLocation;
import org.apache.iotdb.commons.cluster.Endpoint;
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.partition.*;

import java.util.*;

public class FakePartitionFetcherImpl implements IPartitionFetcher {
  @Override
  public DataPartition fetchDataPartitionInfo(DataPartitionQueryParam parameter) {
    return null;
  }

  @Override
  public DataPartition fetchDataPartitionInfos(List<DataPartitionQueryParam> parameterList) {
    String device1 = "root.sg.d1";
    String device2 = "root.sg.d22";
    String device3 = "root.sg.d333";

    DataPartition dataPartition = new DataPartition();
    Map<String, Map<SeriesPartitionSlot, Map<TimePartitionSlot, List<RegionReplicaSet>>>>
        dataPartitionMap = new HashMap<>();
    Map<SeriesPartitionSlot, Map<TimePartitionSlot, List<RegionReplicaSet>>> sgPartitionMap =
        new HashMap<>();

    List<RegionReplicaSet> d1DataRegions = new ArrayList<>();
    d1DataRegions.add(
        new RegionReplicaSet(
            new DataRegionId(1),
            Arrays.asList(
                new DataNodeLocation(11, new Endpoint("192.0.1.1", 9000)),
                new DataNodeLocation(12, new Endpoint("192.0.1.2", 9000)))));
    d1DataRegions.add(
        new RegionReplicaSet(
            new DataRegionId(2),
            Arrays.asList(
                new DataNodeLocation(21, new Endpoint("192.0.2.1", 9000)),
                new DataNodeLocation(22, new Endpoint("192.0.2.2", 9000)))));
    Map<TimePartitionSlot, List<RegionReplicaSet>> d1DataRegionMap = new HashMap<>();
    d1DataRegionMap.put(new TimePartitionSlot(), d1DataRegions);

    List<RegionReplicaSet> d2DataRegions = new ArrayList<>();
    d2DataRegions.add(
        new RegionReplicaSet(
            new DataRegionId(3),
            Arrays.asList(
                new DataNodeLocation(31, new Endpoint("192.0.3.1", 9000)),
                new DataNodeLocation(32, new Endpoint("192.0.3.2", 9000)))));
    Map<TimePartitionSlot, List<RegionReplicaSet>> d2DataRegionMap = new HashMap<>();
    d2DataRegionMap.put(new TimePartitionSlot(), d2DataRegions);

    List<RegionReplicaSet> d3DataRegions = new ArrayList<>();
    d3DataRegions.add(
        new RegionReplicaSet(
            new DataRegionId(1),
            Arrays.asList(
                new DataNodeLocation(11, new Endpoint("192.0.1.1", 9000)),
                new DataNodeLocation(12, new Endpoint("192.0.1.2", 9000)))));
    d3DataRegions.add(
        new RegionReplicaSet(
            new DataRegionId(4),
            Arrays.asList(
                new DataNodeLocation(41, new Endpoint("192.0.4.1", 9000)),
                new DataNodeLocation(42, new Endpoint("192.0.4.2", 9000)))));
    Map<TimePartitionSlot, List<RegionReplicaSet>> d3DataRegionMap = new HashMap<>();
    d3DataRegionMap.put(new TimePartitionSlot(), d3DataRegions);

    sgPartitionMap.put(new SeriesPartitionSlot(device1.length()), d1DataRegionMap);
    sgPartitionMap.put(new SeriesPartitionSlot(device2.length()), d2DataRegionMap);
    sgPartitionMap.put(new SeriesPartitionSlot(device3.length()), d3DataRegionMap);

    dataPartitionMap.put("root.sg", sgPartitionMap);

    dataPartition.setDataPartitionMap(dataPartitionMap);

    return dataPartition;
  }

  @Override
  public SchemaPartition fetchSchemaPartitionInfo(String devicePath) {
    return null;
  }

  @Override
  public SchemaPartition fetchSchemaPartitionInfos(List<String> devicePath) {
    return null;
  }

  @Override
  public PartitionInfo fetchPartitionInfo(DataPartitionQueryParam parameter) {
    return null;
  }

  @Override
  public PartitionInfo fetchPartitionInfos(List<DataPartitionQueryParam> parameterList) {
    return null;
  }
}
