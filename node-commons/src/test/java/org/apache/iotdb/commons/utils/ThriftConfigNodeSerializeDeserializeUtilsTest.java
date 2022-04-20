package org.apache.iotdb.commons.utils;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.confignode.rpc.thrift.TStorageGroupSchema;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;

public class ThriftConfigNodeSerializeDeserializeUtilsTest {

  private static final ByteBuffer buffer = ByteBuffer.allocate(1024 * 10);

  @After
  public void cleanBuffer() {
    buffer.clear();
  }

  @Test
  public void readWriteTStorageGroupSchemaTest() {
    TStorageGroupSchema storageGroupSchema0 = new TStorageGroupSchema();
    storageGroupSchema0.setName("root.sg");
    storageGroupSchema0.setTTL(Long.MAX_VALUE);
    storageGroupSchema0.setSchemaReplicationFactor(3);
    storageGroupSchema0.setDataReplicationFactor(3);
    storageGroupSchema0.setTimePartitionInterval(604800);

    storageGroupSchema0.setSchemaRegionGroupIds(new ArrayList<>());
    storageGroupSchema0.setDataRegionGroupIds(new ArrayList<>());
    for (int i = 0; i < 3; i++) {
      storageGroupSchema0
          .getSchemaRegionGroupIds()
          .add(new TConsensusGroupId(TConsensusGroupType.SchemaRegion, i * 2));
      storageGroupSchema0
          .getDataRegionGroupIds()
          .add(new TConsensusGroupId(TConsensusGroupType.DataRegion, i * 2 + 1));
    }

    ThriftConfigNodeSerializeDeserializeUtils.writeTStorageGroupSchema(storageGroupSchema0, buffer);
    TStorageGroupSchema storageGroupSchema1 =
        ThriftConfigNodeSerializeDeserializeUtils.readTStorageGroupSchema(buffer);
    Assert.assertEquals(storageGroupSchema0, storageGroupSchema1);
  }
}
