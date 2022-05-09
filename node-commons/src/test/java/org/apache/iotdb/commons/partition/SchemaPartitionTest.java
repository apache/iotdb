/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.commons.partition;

import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;

import org.apache.thrift.TException;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class SchemaPartitionTest extends SerializeTest {

  @Test
  public void testSerialize() throws TException, IOException {

    int schemaPartitionFlag = 20;

    SchemaPartition schemaPartition =
        new SchemaPartition(new HashMap<>(), seriesPartitionExecutorClass, 1);
    Map<String, Map<TSeriesPartitionSlot, TRegionReplicaSet>> schemaPartitionMap =
        generateCreateSchemaPartitionMap(
            schemaPartitionFlag, generateTConsensusGroupId(schemaPartitionFlag));
    schemaPartition.setSchemaPartitionMap(schemaPartitionMap);
    ByteBuffer buffer = ByteBuffer.allocate(10 * 1024);
    schemaPartition.serialize(buffer);

    SchemaPartition newSchemaPartition =
        new SchemaPartition(new HashMap<>(), seriesPartitionExecutorClass, 1);
    buffer.flip();
    newSchemaPartition.deserialize(buffer);
    Assert.assertEquals(schemaPartitionMap, newSchemaPartition.getSchemaPartitionMap());
  }
}
