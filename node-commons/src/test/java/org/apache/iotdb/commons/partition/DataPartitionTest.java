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
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;

import org.apache.thrift.TException;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DataPartitionTest extends SerializeTest {

  @Test
  public void testSerialize() throws TException, IOException {

    int dataPartitionFlag = 10;

    DataPartition dataPartition = new DataPartition(seriesPartitionExecutorClass, 1);

    Map<String, Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TRegionReplicaSet>>>>
        assignedDataPartition =
            generateCreateDataPartitionMap(
                dataPartitionFlag, generateTConsensusGroupId(dataPartitionFlag));
    dataPartition.setDataPartitionMap(assignedDataPartition);
    ByteBuffer buffer = ByteBuffer.allocate(10 * 1024);
    dataPartition.serialize(buffer);

    DataPartition newDataPartition =
        new DataPartition(new HashMap<>(), seriesPartitionExecutorClass, 1);
    buffer.flip();
    newDataPartition.deserialize(buffer);
    Assert.assertEquals(assignedDataPartition, newDataPartition.getDataPartitionMap());
  }
}
