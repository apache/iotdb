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
import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;

import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DataPartitionTableTest {

  @Test
  public void serDeDataPartitionTest() {
    Map<TSeriesPartitionSlot, SeriesPartitionTable> dataPartitionMap = new HashMap<>();
    for (int i = 0; i < 2; i++) {
      TSeriesPartitionSlot seriesPartitionSlot = new TSeriesPartitionSlot(i);
      Map<TTimePartitionSlot, List<TConsensusGroupId>> seriesPartitionMap = new HashMap<>();
      for (int j = 0; j < 4; j++) {
        TTimePartitionSlot timePartitionSlot = new TTimePartitionSlot(j);
        List<TConsensusGroupId> consensusGroupIds = new ArrayList<>();
        for (int k = 0; k < 8; k++) {
          consensusGroupIds.add(new TConsensusGroupId(TConsensusGroupType.DataRegion, k));
        }
        seriesPartitionMap.put(timePartitionSlot, consensusGroupIds);
      }
      SeriesPartitionTable seriesPartitionTable = new SeriesPartitionTable(seriesPartitionMap);
      dataPartitionMap.put(seriesPartitionSlot, seriesPartitionTable);
    }
    DataPartitionTable table0 = new DataPartitionTable(dataPartitionMap);

    ByteBuffer buffer = ByteBuffer.allocate(10240);
    table0.serialize(buffer);
    buffer.flip();
    DataPartitionTable table1 = new DataPartitionTable();
    table1.deserialize(buffer);
    Assert.assertEquals(table0, table1);
  }
}
