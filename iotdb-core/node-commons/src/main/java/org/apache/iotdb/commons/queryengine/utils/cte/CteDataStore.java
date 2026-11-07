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

package org.apache.iotdb.commons.queryengine.utils.cte;

import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.queryengine.plan.relational.metadata.TableSchema;
import org.apache.iotdb.commons.utils.TestOnly;

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
  private final long maxBytes;
  private final int maxRows;
  private final boolean inclusiveLimit;

  public enum AddResult {
    SUCCESS,
    BYTES_EXCEEDED,
    ROWS_EXCEEDED
  }

  // reference count by CteScanReader
  private final AtomicInteger count;

  /**
   * Bytes actually reserved from the operators free-memory pool for this CTE data store. May be
   * {@code 0} if reservation fell back (e.g. highest-priority query with insufficient pool).
   */
  private long actualReservedBytes;

  public CteDataStore(TableSchema tableSchema, List<Integer> columnIndex2TsBlockColumnIndexList) {
    this(
        tableSchema,
        columnIndex2TsBlockColumnIndexList,
        CommonDescriptor.getInstance().getConfig().getCteBufferSize(),
        CommonDescriptor.getInstance().getConfig().getMaxRowsInCteBuffer(),
        false);
  }

  /** A query-level PIVOT input store uses independent, inclusive budgets fixed at creation. */
  public CteDataStore(
      TableSchema tableSchema,
      List<Integer> columnIndex2TsBlockColumnIndexList,
      long maxBytes,
      int maxRows) {
    this(tableSchema, columnIndex2TsBlockColumnIndexList, maxBytes, maxRows, true);
  }

  private CteDataStore(
      TableSchema tableSchema,
      List<Integer> columnIndex2TsBlockColumnIndexList,
      long maxBytes,
      int maxRows,
      boolean inclusiveLimit) {
    this.tableSchema = tableSchema;
    this.columnIndex2TsBlockColumnIndexList = columnIndex2TsBlockColumnIndexList;
    this.maxBytes = maxBytes;
    this.maxRows = maxRows;
    this.inclusiveLimit = inclusiveLimit;
    this.cachedData = new ArrayList<>();
    this.count = new AtomicInteger(0);
  }

  public AddResult tryAddTsBlock(TsBlock tsBlock) {
    long bytes = tsBlock.getRetainedSizeInBytes();
    int rows = tsBlock.getPositionCount();
    // Compare against the remaining budget rather than adding counters, to avoid overflow.
    if (bytes > maxBytes - cachedBytes || (!inclusiveLimit && bytes == maxBytes - cachedBytes)) {
      return AddResult.BYTES_EXCEEDED;
    }
    if (rows > maxRows - cachedRows || (!inclusiveLimit && rows == maxRows - cachedRows)) {
      return AddResult.ROWS_EXCEEDED;
    }
    cachedData.add(tsBlock);
    cachedBytes += bytes;
    cachedRows += rows;
    return AddResult.SUCCESS;
  }

  /**
   * Charges memory held outside cached TsBlocks, such as discovered dynamic PIVOT values, to the
   * same byte budget. Only stores with an inclusive limit use this path.
   */
  public boolean tryReserveExtraBytes(long bytes) {
    // Compare against the remaining budget rather than adding counters, to avoid overflow.
    if (bytes > maxBytes - cachedBytes || (!inclusiveLimit && bytes == maxBytes - cachedBytes)) {
      return false;
    }
    cachedBytes += bytes;
    return true;
  }

  public boolean addTsBlock(TsBlock tsBlock) {
    return tryAddTsBlock(tsBlock) == AddResult.SUCCESS;
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

  public long getActualReservedBytes() {
    return actualReservedBytes;
  }

  public void setActualReservedBytes(long actualReservedBytes) {
    this.actualReservedBytes = actualReservedBytes;
  }
}
