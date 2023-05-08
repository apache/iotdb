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
import org.apache.iotdb.common.rpc.thrift.TDataNodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TNodeResource;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.tsfile.utils.PublicBAOS;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;

public class ThriftCommonsSerDeUtilsTest {

  private static final ByteBuffer buffer = ByteBuffer.allocate(1024 * 10);

  @After
  public void cleanBuffer() {
    buffer.clear();
  }

  @Test
  public void readWriteTEndPointTest() throws IOException {
    TEndPoint endPoint0 = new TEndPoint("0.0.0.0", 6667);

    try (PublicBAOS byteArrayOutputStream = new PublicBAOS();
        DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      ThriftCommonsSerDeUtils.serializeTEndPoint(endPoint0, outputStream);
      TEndPoint endPoint1 =
          ThriftCommonsSerDeUtils.deserializeTEndPoint(
              ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size()));
      Assert.assertEquals(endPoint0, endPoint1);
    }
  }

  @Test
  public void readWriteTDataNodeConfigurationTest() throws IOException {
    TDataNodeLocation dataNodeLocation0 = new TDataNodeLocation();
    dataNodeLocation0.setDataNodeId(0);
    dataNodeLocation0.setClientRpcEndPoint(new TEndPoint("0.0.0.0", 6667));
    dataNodeLocation0.setInternalEndPoint(new TEndPoint("0.0.0.0", 10730));
    dataNodeLocation0.setMPPDataExchangeEndPoint(new TEndPoint("0.0.0.0", 10740));
    dataNodeLocation0.setDataRegionConsensusEndPoint(new TEndPoint("0.0.0.0", 10760));
    dataNodeLocation0.setSchemaRegionConsensusEndPoint(new TEndPoint("0.0.0.0", 10750));

    TNodeResource dataNodeResource0 = new TNodeResource();
    dataNodeResource0.setCpuCoreNum(16);
    dataNodeResource0.setMaxMemory(2022213861);

    TDataNodeConfiguration dataNodeConfiguration0 = new TDataNodeConfiguration();
    dataNodeConfiguration0.setLocation(dataNodeLocation0);
    dataNodeConfiguration0.setResource(dataNodeResource0);

    try (PublicBAOS byteArrayOutputStream = new PublicBAOS();
        DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      ThriftCommonsSerDeUtils.serializeTDataNodeConfiguration(dataNodeConfiguration0, outputStream);
      TDataNodeConfiguration dataNodeConfiguration1 =
          ThriftCommonsSerDeUtils.deserializeTDataNodeConfiguration(
              ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size()));
      Assert.assertEquals(dataNodeConfiguration0, dataNodeConfiguration1);
    }
  }

  @Test
  public void readWriteTDataNodeLocationTest() throws IOException {
    TDataNodeLocation dataNodeLocation0 = new TDataNodeLocation();
    dataNodeLocation0.setDataNodeId(0);
    dataNodeLocation0.setClientRpcEndPoint(new TEndPoint("0.0.0.0", 6667));
    dataNodeLocation0.setInternalEndPoint(new TEndPoint("0.0.0.0", 10730));
    dataNodeLocation0.setMPPDataExchangeEndPoint(new TEndPoint("0.0.0.0", 10740));
    dataNodeLocation0.setDataRegionConsensusEndPoint(new TEndPoint("0.0.0.0", 10760));
    dataNodeLocation0.setSchemaRegionConsensusEndPoint(new TEndPoint("0.0.0.0", 10750));

    try (PublicBAOS byteArrayOutputStream = new PublicBAOS();
        DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      ThriftCommonsSerDeUtils.serializeTDataNodeLocation(dataNodeLocation0, outputStream);
      TDataNodeLocation dataNodeLocation1 =
          ThriftCommonsSerDeUtils.deserializeTDataNodeLocation(
              ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size()));
      Assert.assertEquals(dataNodeLocation0, dataNodeLocation1);
    }
  }

  @Test
  public void readWriteTSeriesPartitionSlotTest() throws IOException {
    TSeriesPartitionSlot seriesPartitionSlot0 = new TSeriesPartitionSlot(10);
    try (PublicBAOS byteArrayOutputStream = new PublicBAOS();
        DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      ThriftCommonsSerDeUtils.serializeTSeriesPartitionSlot(seriesPartitionSlot0, outputStream);
      TSeriesPartitionSlot seriesPartitionSlot1 =
          ThriftCommonsSerDeUtils.deserializeTSeriesPartitionSlot(
              ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size()));
      Assert.assertEquals(seriesPartitionSlot0, seriesPartitionSlot1);
    }
  }

  @Test
  public void writeTTimePartitionSlot() throws IOException {
    TTimePartitionSlot timePartitionSlot0 = new TTimePartitionSlot(100);
    try (PublicBAOS byteArrayOutputStream = new PublicBAOS();
        DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      ThriftCommonsSerDeUtils.serializeTTimePartitionSlot(timePartitionSlot0, outputStream);
      TTimePartitionSlot timePartitionSlot1 =
          ThriftCommonsSerDeUtils.deserializeTTimePartitionSlot(
              ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size()));
      Assert.assertEquals(timePartitionSlot0, timePartitionSlot1);
    }
  }

  @Test
  public void readWriteTConsensusGroupIdTest() throws IOException {
    TConsensusGroupId consensusGroupId0 =
        new TConsensusGroupId(TConsensusGroupType.ConfigRegion, 0);
    try (PublicBAOS byteArrayOutputStream = new PublicBAOS();
        DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      ThriftCommonsSerDeUtils.serializeTConsensusGroupId(consensusGroupId0, outputStream);
      TConsensusGroupId consensusGroupId1 =
          ThriftCommonsSerDeUtils.deserializeTConsensusGroupId(
              ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size()));
      Assert.assertEquals(consensusGroupId0, consensusGroupId1);
    }
  }

  @Test
  public void readWriteTRegionReplicaSetTest() throws IOException {
    TRegionReplicaSet regionReplicaSet0 = new TRegionReplicaSet();
    regionReplicaSet0.setRegionId(new TConsensusGroupId(TConsensusGroupType.SchemaRegion, 0));
    regionReplicaSet0.setDataNodeLocations(new ArrayList<>());
    for (int i = 0; i < 3; i++) {
      TDataNodeLocation dataNodeLocation = new TDataNodeLocation();
      dataNodeLocation.setDataNodeId(i);
      dataNodeLocation.setClientRpcEndPoint(new TEndPoint("0.0.0.0", 6667 + i));
      dataNodeLocation.setInternalEndPoint(new TEndPoint("0.0.0.0", 10730 + i));
      dataNodeLocation.setMPPDataExchangeEndPoint(new TEndPoint("0.0.0.0", 10740 + i));
      dataNodeLocation.setDataRegionConsensusEndPoint(new TEndPoint("0.0.0.0", 10760 + i));
      dataNodeLocation.setSchemaRegionConsensusEndPoint(new TEndPoint("0.0.0.0", 10750 + i));
      regionReplicaSet0.getDataNodeLocations().add(dataNodeLocation);
    }
    try (PublicBAOS byteArrayOutputStream = new PublicBAOS();
        DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      ThriftCommonsSerDeUtils.serializeTRegionReplicaSet(regionReplicaSet0, outputStream);
      TRegionReplicaSet regionReplicaSet1 =
          ThriftCommonsSerDeUtils.deserializeTRegionReplicaSet(
              ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size()));
      Assert.assertEquals(regionReplicaSet0, regionReplicaSet1);
    }
  }
}
