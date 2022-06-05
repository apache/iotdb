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
package org.apache.iotdb.commons.utils;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;

public class ThriftCommonsSerDeUtilsTest {

  private static final ByteBuffer buffer = ByteBuffer.allocate(1024 * 10);

  @After
  public void cleanBuffer() {
    buffer.clear();
  }

  @Test
  public void readWriteTEndPointTest() {
    TEndPoint endPoint0 = new TEndPoint("0.0.0.0", 6667);
    ThriftCommonsSerDeUtils.serializeTEndPoint(endPoint0, buffer);
    buffer.flip();
    TEndPoint endPoint1 = ThriftCommonsSerDeUtils.deserializeTEndPoint(buffer);
    Assert.assertEquals(endPoint0, endPoint1);
  }

  @Test
  public void readWriteTDataNodeLocationTest() {
    TDataNodeLocation dataNodeLocation0 = new TDataNodeLocation();
    dataNodeLocation0.setDataNodeId(0);
    dataNodeLocation0.setExternalEndPoint(new TEndPoint("0.0.0.0", 6667));
    dataNodeLocation0.setInternalEndPoint(new TEndPoint("0.0.0.0", 9003));
    dataNodeLocation0.setDataBlockManagerEndPoint(new TEndPoint("0.0.0.0", 8777));
    dataNodeLocation0.setDataRegionConsensusEndPoint(new TEndPoint("0.0.0.0", 40010));
    dataNodeLocation0.setSchemaRegionConsensusEndPoint(new TEndPoint("0.0.0.0", 50010));
    ThriftCommonsSerDeUtils.serializeTDataNodeLocation(dataNodeLocation0, buffer);
    buffer.flip();
    TDataNodeLocation dataNodeLocation1 =
        ThriftCommonsSerDeUtils.deserializeTDataNodeLocation(buffer);
    Assert.assertEquals(dataNodeLocation0, dataNodeLocation1);
  }

  @Test
  public void readWriteTSeriesPartitionSlotTest() {
    TSeriesPartitionSlot seriesPartitionSlot0 = new TSeriesPartitionSlot(10);
    ThriftCommonsSerDeUtils.serializeTSeriesPartitionSlot(seriesPartitionSlot0, buffer);
    buffer.flip();
    TSeriesPartitionSlot seriesPartitionSlot1 =
        ThriftCommonsSerDeUtils.deserializeTSeriesPartitionSlot(buffer);
    Assert.assertEquals(seriesPartitionSlot0, seriesPartitionSlot1);
  }

  @Test
  public void writeTTimePartitionSlot() {
    TTimePartitionSlot timePartitionSlot0 = new TTimePartitionSlot(100);
    ThriftCommonsSerDeUtils.serializeTTimePartitionSlot(timePartitionSlot0, buffer);
    buffer.flip();
    TTimePartitionSlot timePartitionSlot1 =
        ThriftCommonsSerDeUtils.deserializeTTimePartitionSlot(buffer);
    Assert.assertEquals(timePartitionSlot0, timePartitionSlot1);
  }

  @Test
  public void readWriteTConsensusGroupIdTest() {
    TConsensusGroupId consensusGroupId0 =
        new TConsensusGroupId(TConsensusGroupType.PartitionRegion, 0);
    ThriftCommonsSerDeUtils.serializeTConsensusGroupId(consensusGroupId0, buffer);
    buffer.flip();
    TConsensusGroupId consensusGroupId1 =
        ThriftCommonsSerDeUtils.deserializeTConsensusGroupId(buffer);
    Assert.assertEquals(consensusGroupId0, consensusGroupId1);
  }

  @Test
  public void readWriteTRegionReplicaSetTest() {
    TRegionReplicaSet regionReplicaSet0 = new TRegionReplicaSet();
    regionReplicaSet0.setRegionId(new TConsensusGroupId(TConsensusGroupType.SchemaRegion, 0));
    regionReplicaSet0.setDataNodeLocations(new ArrayList<>());
    for (int i = 0; i < 3; i++) {
      TDataNodeLocation dataNodeLocation = new TDataNodeLocation();
      dataNodeLocation.setDataNodeId(i);
      dataNodeLocation.setExternalEndPoint(new TEndPoint("0.0.0.0", 6667 + i));
      dataNodeLocation.setInternalEndPoint(new TEndPoint("0.0.0.0", 9003 + i));
      dataNodeLocation.setDataBlockManagerEndPoint(new TEndPoint("0.0.0.0", 8777 + i));
      dataNodeLocation.setDataRegionConsensusEndPoint(new TEndPoint("0.0.0.0", 40010 + i));
      dataNodeLocation.setSchemaRegionConsensusEndPoint(new TEndPoint("0.0.0.0", 50010 + i));
      regionReplicaSet0.getDataNodeLocations().add(dataNodeLocation);
    }
    ThriftCommonsSerDeUtils.serializeTRegionReplicaSet(regionReplicaSet0, buffer);
    buffer.flip();
    TRegionReplicaSet regionReplicaSet1 =
        ThriftCommonsSerDeUtils.deserializeTRegionReplicaSet(buffer);
    Assert.assertEquals(regionReplicaSet0, regionReplicaSet1);
  }
}
