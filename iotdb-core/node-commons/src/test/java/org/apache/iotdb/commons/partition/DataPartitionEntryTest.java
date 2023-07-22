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
import org.apache.iotdb.commons.conf.CommonDescriptor;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class DataPartitionEntryTest {

  private static final int SERIES_SLOT_NUM = 1000;
  private static final long TIME_PARTITION_INTERVAL =
      CommonDescriptor.getInstance().getConfig().getTimePartitionInterval();

  @Test
  public void testOrder() {
    List<DataPartitionEntry> entries = new ArrayList<>();
    for (int i = 0; i < SERIES_SLOT_NUM; i++) {
      entries.add(
          new DataPartitionEntry(
              new TSeriesPartitionSlot(i),
              new TTimePartitionSlot(TIME_PARTITION_INTERVAL * i),
              new TConsensusGroupId(TConsensusGroupType.DataRegion, i)));
    }

    List<DataPartitionEntry> sortedEntries = new ArrayList<>(entries);
    Collections.sort(sortedEntries);
    for (int i = 0; i < SERIES_SLOT_NUM; i++) {
      Assert.assertEquals(entries.get(SERIES_SLOT_NUM - i - 1), sortedEntries.get(i));
    }
  }
}
