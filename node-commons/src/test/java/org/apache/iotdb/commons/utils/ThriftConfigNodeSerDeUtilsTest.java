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

import org.apache.iotdb.common.rpc.thrift.TConfigNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.confignode.rpc.thrift.TDatabaseSchema;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

public class ThriftConfigNodeSerDeUtilsTest {

  private static final ByteBuffer buffer = ByteBuffer.allocate(1024 * 10);

  @After
  public void cleanBuffer() {
    buffer.clear();
  }

  @Test
  public void readWriteTStorageGroupSchemaTest() {
    TDatabaseSchema storageGroupSchema0 = new TDatabaseSchema();
    storageGroupSchema0.setName("root.sg");
    storageGroupSchema0.setTTL(Long.MAX_VALUE);
    storageGroupSchema0.setSchemaReplicationFactor(3);
    storageGroupSchema0.setDataReplicationFactor(3);
    storageGroupSchema0.setTimePartitionInterval(604800);

    ThriftConfigNodeSerDeUtils.serializeTStorageGroupSchema(storageGroupSchema0, buffer);
    buffer.flip();
    TDatabaseSchema storageGroupSchema1 =
        ThriftConfigNodeSerDeUtils.deserializeTStorageGroupSchema(buffer);
    Assert.assertEquals(storageGroupSchema0, storageGroupSchema1);
  }

  @Test
  public void readWriteTConfigNodeLocationTest() {
    TConfigNodeLocation configNodeLocation0 =
        new TConfigNodeLocation(
            0, new TEndPoint("0.0.0.0", 10710), new TEndPoint("0.0.0.0", 10720));

    ThriftConfigNodeSerDeUtils.serializeTConfigNodeLocation(configNodeLocation0, buffer);
    buffer.flip();
    TConfigNodeLocation configNodeLocation1 =
        ThriftConfigNodeSerDeUtils.deserializeTConfigNodeLocation(buffer);
    Assert.assertEquals(configNodeLocation0, configNodeLocation1);
  }
}
