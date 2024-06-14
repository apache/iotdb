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

package org.apache.iotdb.commons.partition.executor.hash;

import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.commons.partition.executor.SeriesPartitionExecutor;

import org.apache.tsfile.file.metadata.IDeviceID;

import static org.apache.iotdb.commons.conf.IoTDBConstant.PATH_SEPARATOR;

public class BKDRHashExecutor extends SeriesPartitionExecutor {

  private static final int SEED = 131;

  public BKDRHashExecutor(int deviceGroupCount) {
    super(deviceGroupCount);
  }

  @Override
  public TSeriesPartitionSlot getSeriesPartitionSlot(String device) {
    int hash = 0;

    for (int i = 0; i < device.length(); i++) {
      hash = hash * SEED + (int) device.charAt(i);
    }
    hash &= Integer.MAX_VALUE;

    return new TSeriesPartitionSlot(hash % seriesPartitionSlotNum);
  }

  @Override
  public TSeriesPartitionSlot getSeriesPartitionSlot(IDeviceID deviceID) {
    int hash = 0;
    int segmentNum = deviceID.segmentNum();

    for (int segmentID = 0; segmentID < segmentNum; segmentID++) {
      String segment = (String) deviceID.segment(segmentID);
      for (int i = 0; i < segment.length(); i++) {
        hash = hash * SEED + (int) segment.charAt(i);
      }
      if (segmentID < segmentNum - 1) {
        hash = hash * SEED + (int) PATH_SEPARATOR;
      }
    }
    hash &= Integer.MAX_VALUE;

    return new TSeriesPartitionSlot(hash % seriesPartitionSlotNum);
  }
}
