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
import org.apache.iotdb.confignode.rpc.thrift.TStorageGroupSchema;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;

public class ThriftConfigNodeSerDeUtilsTest {

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

    ThriftConfigNodeSerDeUtils.writeTStorageGroupSchema(storageGroupSchema0, buffer);
    buffer.flip();
    TStorageGroupSchema storageGroupSchema1 =
        ThriftConfigNodeSerDeUtils.readTStorageGroupSchema(buffer);
    Assert.assertEquals(storageGroupSchema0, storageGroupSchema1);
  }
}
