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
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.plan.planner.LocalExecutionPlanner;

import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.utils.RamUsageEstimator;

public class MemoryReader implements CteDataReader {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(MemoryReader.class);

  private final LocalExecutionPlanner LOCAL_EXECUTION_PLANNER = LocalExecutionPlanner.getInstance();
  // all the data in MemoryReader lies in memory
  private final CteDataStore dataStore;
  private int tsBlockIndex;

  public MemoryReader(CteDataStore dataStore, QueryId queryId) {
    this.dataStore = dataStore;
    this.tsBlockIndex = 0;
    if (dataStore.incrementAndGetCount() == 1) {
      LOCAL_EXECUTION_PLANNER.reserveFromFreeMemoryForOperators(
          dataStore.ramBytesUsed(), 0L, queryId.getId(), MemoryReader.class.getName());
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
      LOCAL_EXECUTION_PLANNER.releaseToFreeMemoryForOperators(dataStore.ramBytesUsed());
    }
  }

  @Override
  public long ramBytesUsed() {
    // The calculation excludes the memory occupied by the CteDataStore.
    // memory allocate/release for CteDataStore is handled during constructor and close
    return INSTANCE_SIZE;
  }
}
