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

import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.TableSchema;

import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.utils.Accountable;
import org.apache.tsfile.utils.RamUsageEstimator;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class CteDataStore implements Accountable {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(CteDataStore.class);

  private final TableSchema tableSchema;
  private final List<Integer> columnIndex2TsBlockColumnIndexList;

  private final List<TsBlock> cachedData;
  private long cachedBytes;
  private int cachedRows;

  // reference count by CteScanReader
  private final AtomicInteger count;

  public CteDataStore(TableSchema tableSchema, List<Integer> columnIndex2TsBlockColumnIndexList) {
    this.tableSchema = tableSchema;
    this.columnIndex2TsBlockColumnIndexList = columnIndex2TsBlockColumnIndexList;
    this.cachedData = new ArrayList<>();
    this.cachedBytes = 0L;
    this.cachedRows = 0;
    this.count = new AtomicInteger(0);
  }

  public boolean addTsBlock(TsBlock tsBlock) {
    IoTDBConfig iotConfig = IoTDBDescriptor.getInstance().getConfig();
    long bytesSize = tsBlock.getRetainedSizeInBytes();
    int rows = tsBlock.getPositionCount();
    if (bytesSize + cachedBytes >= iotConfig.getCteBufferSize()
        || rows + cachedRows >= iotConfig.getMaxRowsInCteBuffer()) {
      return false;
    }
    cachedData.add(tsBlock);
    cachedBytes += bytesSize;
    cachedRows += rows;
    return true;
  }

  public void clear() {
    cachedData.clear();
    cachedBytes = 0L;
    cachedRows = 0;
  }

  public List<TsBlock> getCachedData() {
    return cachedData;
  }

  public TableSchema getTableSchema() {
    return tableSchema;
  }

  public List<Integer> getColumnIndex2TsBlockColumnIndexList() {
    return columnIndex2TsBlockColumnIndexList;
  }

  public int incrementAndGetCount() {
    return count.incrementAndGet();
  }

  public int decrementAndGetCount() {
    return count.decrementAndGet();
  }

  @Override
  public long ramBytesUsed() {
    return INSTANCE_SIZE + cachedBytes;
  }

  @TestOnly
  public int getCount() {
    return count.get();
  }
}
