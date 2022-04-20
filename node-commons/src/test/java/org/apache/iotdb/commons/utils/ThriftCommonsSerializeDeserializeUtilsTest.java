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

public class ThriftCommonsSerializeDeserializeUtilsTest {

  private static final ByteBuffer buffer = ByteBuffer.allocate(1024 * 10);

  @After
  public void cleanBuffer() {
    buffer.clear();
  }

  @Test
  public void readWriteTEndPointTest() {
    TEndPoint endPoint0 = new TEndPoint("0.0.0.0", 6667);
    ThriftCommonsSerializeDeserializeUtils.writeTEndPoint(endPoint0, buffer);
    buffer.flip();
    TEndPoint endPoint1 = ThriftCommonsSerializeDeserializeUtils.readTEndPoint(buffer);
    Assert.assertEquals(endPoint0, endPoint1);
  }

  @Test
  public void readWriteTDataNodeLocationTest() {
    TDataNodeLocation dataNodeLocation0 = new TDataNodeLocation();
    dataNodeLocation0.setDataNodeId(0);
    dataNodeLocation0.setExternalEndPoint(new TEndPoint("0.0.0.0", 6667));
    dataNodeLocation0.setInternalEndPoint(new TEndPoint("0.0.0.0", 9003));
    dataNodeLocation0.setDataBlockManagerEndPoint(new TEndPoint("0.0.0.0", 8777));
    dataNodeLocation0.setConsensusEndPoint(new TEndPoint("0.0.0.0", 40010));
    ThriftCommonsSerializeDeserializeUtils.writeTDataNodeLocation(dataNodeLocation0, buffer);
    buffer.flip();
    TDataNodeLocation dataNodeLocation1 =
        ThriftCommonsSerializeDeserializeUtils.readTDataNodeLocation(buffer);
    Assert.assertEquals(dataNodeLocation0, dataNodeLocation1);
  }

  @Test
  public void readWriteTSeriesPartitionSlotTest() {
    TSeriesPartitionSlot seriesPartitionSlot0 = new TSeriesPartitionSlot(10);
    ThriftCommonsSerializeDeserializeUtils.writeTSeriesPartitionSlot(seriesPartitionSlot0, buffer);
    buffer.flip();
    TSeriesPartitionSlot seriesPartitionSlot1 =
        ThriftCommonsSerializeDeserializeUtils.readTSeriesPartitionSlot(buffer);
    Assert.assertEquals(seriesPartitionSlot0, seriesPartitionSlot1);
  }

  @Test
  public void writeTTimePartitionSlot() {
    TTimePartitionSlot timePartitionSlot0 = new TTimePartitionSlot(100);
    ThriftCommonsSerializeDeserializeUtils.writeTTimePartitionSlot(timePartitionSlot0, buffer);
    buffer.flip();
    TTimePartitionSlot timePartitionSlot1 =
        ThriftCommonsSerializeDeserializeUtils.readTTimePartitionSlot(buffer);
    Assert.assertEquals(timePartitionSlot0, timePartitionSlot1);
  }

  @Test
  public void readWriteTConsensusGroupIdTest() {
    TConsensusGroupId consensusGroupId0 =
        new TConsensusGroupId(TConsensusGroupType.PartitionRegion, 0);
    ThriftCommonsSerializeDeserializeUtils.writeTConsensusGroupId(consensusGroupId0, buffer);
    buffer.flip();
    TConsensusGroupId consensusGroupId1 =
        ThriftCommonsSerializeDeserializeUtils.readTConsensusGroupId(buffer);
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
      dataNodeLocation.setConsensusEndPoint(new TEndPoint("0.0.0.0", 40010 + i));
      regionReplicaSet0.getDataNodeLocations().add(dataNodeLocation);
    }
    ThriftCommonsSerializeDeserializeUtils.writeTRegionReplicaSet(regionReplicaSet0, buffer);
    buffer.flip();
    TRegionReplicaSet regionReplicaSet1 =
        ThriftCommonsSerializeDeserializeUtils.readTRegionReplicaSet(buffer);
    Assert.assertEquals(regionReplicaSet0, regionReplicaSet1);
  }
}
