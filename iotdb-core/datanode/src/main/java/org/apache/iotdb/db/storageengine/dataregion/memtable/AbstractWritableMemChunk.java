/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.storageengine.dataregion.memtable;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.db.queryengine.exception.MemoryNotEnoughException;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.queryengine.execution.fragment.QueryContext;
import org.apache.iotdb.db.queryengine.plan.planner.memory.MemoryReservationManager;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.IWALByteBufferView;
import org.apache.iotdb.db.utils.datastructure.BatchEncodeInfo;
import org.apache.iotdb.db.utils.datastructure.TVList;

import org.apache.tsfile.encrypt.EncryptParameter;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.write.chunk.IChunkWriter;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BlockingQueue;

public abstract class AbstractWritableMemChunk implements IWritableMemChunk {
  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractWritableMemChunk.class);

  protected static long RETRY_INTERVAL_MS = 100L;
  protected static long MAX_WAIT_QUERY_MS = 60 * 1000L;

  /**
   * Release the TVList if there is no query on it. Otherwise, it should set the first query as the
   * owner. TVList is released until all queries finish. If it throws memory-not-enough exception
   * during owner transfer, retry the release process after 100ms. If the problem is still not
   * solved in 60s, it starts to abort first query, kick it out of the query list and retry. This
   * method must ensure success because it's part of flushing.
   *
   * @param tvList
   */
  protected void maybeReleaseTvList(TVList tvList) {
    long startTimeInMs = System.currentTimeMillis();
    boolean succeed = false;
    int retryCount = 0;
    while (!succeed) {
      try {
        tryReleaseTvList(tvList);
        succeed = true;
      } catch (MemoryNotEnoughException ex) {
        // print log every 5 seconds
        if (retryCount % 50 == 0) {
          LOGGER.warn(
              "Failed to transfer tvlist memory owner to query engine, {}", ex.getMessage());
        }
        retryCount++;
        long waitQueryInMs = System.currentTimeMillis() - startTimeInMs;
        if (waitQueryInMs > MAX_WAIT_QUERY_MS) {
          // Abort first query in the list. When all queries in the list have been aborted,
          // tryReleaseTvList will ensure succeed finally.
          tvList.lockQueryList();
          try {
            // fail the first query
            Iterator<QueryContext> iterator = tvList.getQueryContextSet().iterator();
            if (iterator.hasNext()) {
              FragmentInstanceContext firstQuery = (FragmentInstanceContext) iterator.next();
              firstQuery.failed(
                  new MemoryNotEnoughException(
                      "Memory not enough to clone the tvlist during flush phase"));
            }
          } finally {
            tvList.unlockQueryList();
          }
        }

        // sleep 100ms to retry
        try {
          Thread.sleep(RETRY_INTERVAL_MS);
        } catch (InterruptedException ignore) {
          Thread.currentThread().interrupt();
        }
      }
    }
  }

  private void tryReleaseTvList(TVList tvList) {
    long tvListRamSize = tvList.calculateRamSize();
    tvList.lockQueryList();
    try {
      if (tvList.getQueryContextSet().isEmpty()) {
        tvList.clear();
      } else {
        QueryContext firstQuery = tvList.getQueryContextSet().iterator().next();
        // transfer memory from write process to read process. Here it reserves read memory and
        // releaseFlushedMemTable will release write memory.
        if (firstQuery instanceof FragmentInstanceContext) {
          MemoryReservationManager memoryReservationManager =
              ((FragmentInstanceContext) firstQuery).getMemoryReservationContext();
          memoryReservationManager.reserveMemoryCumulatively(tvListRamSize);
          tvList.setReservedMemoryBytes(tvListRamSize);
        }
        // update current TVList owner to first query in the list
        tvList.setOwnerQuery(firstQuery);
      }
    } finally {
      tvList.unlockQueryList();
    }
  }

  @Override
  public abstract void putLong(long t, long v);

  @Override
  public abstract void putInt(long t, int v);

  @Override
  public abstract void putFloat(long t, float v);

  @Override
  public abstract void putDouble(long t, double v);

  @Override
  public abstract void putBinary(long t, Binary v);

  @Override
  public abstract void putBoolean(long t, boolean v);

  @Override
  public abstract void putAlignedRow(long t, Object[] v);

  @Override
  public abstract void putLongs(long[] t, long[] v, BitMap bitMap, int start, int end);

  @Override
  public abstract void putInts(long[] t, int[] v, BitMap bitMap, int start, int end);

  @Override
  public abstract void putFloats(long[] t, float[] v, BitMap bitMap, int start, int end);

  @Override
  public abstract void putDoubles(long[] t, double[] v, BitMap bitMap, int start, int end);

  @Override
  public abstract void putBinaries(long[] t, Binary[] v, BitMap bitMap, int start, int end);

  @Override
  public abstract void putBooleans(long[] t, boolean[] v, BitMap bitMap, int start, int end);

  @Override
  public abstract void putAlignedTablet(
      long[] t, Object[] v, BitMap[] bitMaps, int start, int end, TSStatus[] results);

  @Override
  public abstract void writeNonAlignedPoint(long insertTime, Object objectValue);

  @Override
  public abstract void writeAlignedPoints(
      long insertTime, Object[] objectValue, List<IMeasurementSchema> schemaList);

  @Override
  public abstract void writeNonAlignedTablet(
      long[] times, Object valueList, BitMap bitMap, TSDataType dataType, int start, int end);

  @Override
  public abstract void writeAlignedTablet(
      long[] times,
      Object[] valueList,
      BitMap[] bitMaps,
      List<IMeasurementSchema> schemaList,
      int start,
      int end,
      TSStatus[] results);

  @Override
  public abstract long count();

  @Override
  public abstract long rowCount();

  @Override
  public abstract IMeasurementSchema getSchema();

  @Override
  public abstract void sortTvListForFlush();

  @Override
  public abstract int delete(long lowerBound, long upperBound);

  @Override
  public abstract IChunkWriter createIChunkWriter();

  @Override
  public abstract void encode(
      BlockingQueue<Object> ioTaskQueue, BatchEncodeInfo encodeInfo, long[] times);

  @Override
  public abstract void release();

  @Override
  public abstract boolean isEmpty();

  @Override
  public abstract List<? extends TVList> getSortedList();

  @Override
  public abstract TVList getWorkingTVList();

  @Override
  public abstract void setWorkingTVList(TVList list);

  @Override
  public abstract void serializeToWAL(IWALByteBufferView buffer);

  @Override
  public abstract int serializedSize();

  @Override
  public abstract void setEncryptParameter(EncryptParameter encryptParameter);
}
