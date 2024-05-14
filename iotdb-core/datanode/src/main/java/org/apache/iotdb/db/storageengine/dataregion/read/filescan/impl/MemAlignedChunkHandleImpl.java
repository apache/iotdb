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

package org.apache.iotdb.db.storageengine.dataregion.read.filescan.impl;

import org.apache.tsfile.utils.BitMap;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MemAlignedChunkHandleImpl extends MemChunkHandleImpl {

  private final BitMap bitMapOfValue;

  public MemAlignedChunkHandleImpl(long[] dataOfTimestamp, BitMap bitMapOfValue) {
    super(dataOfTimestamp);
    this.bitMapOfValue = bitMapOfValue;
  }

  @Override
  public long[] getDataTime() throws IOException {
    List<Long> timeList = new ArrayList<>();
    for (int i = 0; i < dataOfTimestamp.length; i++) {
      if (!bitMapOfValue.isMarked(i)) {
        timeList.add(dataOfTimestamp[i]);
      }
    }
    hasRead = true;
    return timeList.stream().mapToLong(Long::longValue).toArray();
  }
}
