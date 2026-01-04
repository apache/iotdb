/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package org.apache.iotdb.db.utils.cte;

import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.db.queryengine.plan.planner.memory.MemoryReservationManager;

import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.utils.RamUsageEstimator;

public class MemoryReader implements CteDataReader {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(MemoryReader.class);

  // thread-safe memory manager
  private final MemoryReservationManager memoryReservationManager;
  // all the data in MemoryReader lies in memory
  private final CteDataStore dataStore;
  private int tsBlockIndex;

  public MemoryReader(CteDataStore dataStore, MemoryReservationManager memoryReservationManager) {
    this.dataStore = dataStore;
    this.tsBlockIndex = 0;
    this.memoryReservationManager = memoryReservationManager;
    if (dataStore.incrementAndGetCount() == 1) {
      memoryReservationManager.reserveMemoryCumulatively(dataStore.ramBytesUsed());
    }
  }

  @Override
  public boolean hasNext() throws IoTDBException {
    return dataStore.getCachedData() != null && tsBlockIndex < dataStore.getCachedData().size();
  }

  @Override
  public TsBlock next() throws IoTDBException {
    if (dataStore.getCachedData() == null || tsBlockIndex >= dataStore.getCachedData().size()) {
      return null;
    }
    return dataStore.getCachedData().get(tsBlockIndex++);
  }

  @Override
  public void close() throws IoTDBException {
    if (dataStore.decrementAndGetCount() == 0) {
      memoryReservationManager.releaseMemoryCumulatively(dataStore.ramBytesUsed());
    }
  }

  @Override
  public long ramBytesUsed() {
    // The calculation excludes the memory occupied by the CteDataStore.
    // memory allocate/release for CteDataStore is handled during constructor and close
    return INSTANCE_SIZE;
  }
}
