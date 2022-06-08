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
package org.apache.iotdb.commons.partition;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;

import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SeriesPartitionTableTest {

  @Test
  public void serDeSeriesPartitionTableTest() {
    Map<TTimePartitionSlot, List<TConsensusGroupId>> seriesPartitionMap = new HashMap<>();
    for (int i = 0; i < 5; i++) {
      TTimePartitionSlot timePartitionSlot = new TTimePartitionSlot(i);
      List<TConsensusGroupId> consensusGroupIds = new ArrayList<>();
      for (int j = 0; j < 10; j++) {
        consensusGroupIds.add(new TConsensusGroupId(TConsensusGroupType.DataRegion, j));
      }
      seriesPartitionMap.put(timePartitionSlot, consensusGroupIds);
    }

    SeriesPartitionTable table0 = new SeriesPartitionTable(seriesPartitionMap);

    ByteBuffer buffer = ByteBuffer.allocate(1024);
    table0.serialize(buffer);
    buffer.flip();
    SeriesPartitionTable table1 = new SeriesPartitionTable();
    table1.deserialize(buffer);
    Assert.assertEquals(table0, table1);
  }
}
