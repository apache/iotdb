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

package org.apache.iotdb.commons.partition.executor;

import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;

import java.util.Map;
import java.util.TreeMap;

public class SimpleExecutor extends SeriesPartitionExecutor {

  private int lastSlotId = 0;
  private final int seriesPartitionSlotNum;
  private final Map<String, Integer> slotMap;

  public SimpleExecutor(int seriesPartitionSlotNum) {
    super(seriesPartitionSlotNum);
    this.slotMap = new TreeMap<>();
    this.seriesPartitionSlotNum = seriesPartitionSlotNum;
  }

  @Override
  public TSeriesPartitionSlot getSeriesPartitionSlot(String device) {
    if (slotMap.containsKey(device)) {
      return new TSeriesPartitionSlot(slotMap.get(device));
    }
    slotMap.put(device, lastSlotId);
    TSeriesPartitionSlot result = new TSeriesPartitionSlot(lastSlotId);
    lastSlotId = (lastSlotId + 1) % seriesPartitionSlotNum;
    return result;
  }
}
