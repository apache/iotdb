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
package org.apache.iotdb.db.storageengine.dataregion.memtable;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.WALEntryValue;
import org.apache.iotdb.db.utils.datastructure.BatchEncodeInfo;
import org.apache.iotdb.db.utils.datastructure.TVList;

import org.apache.tsfile.encrypt.EncryptParameter;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.write.chunk.IChunkWriter;
import org.apache.tsfile.write.schema.IMeasurementSchema;

import java.util.List;
import java.util.concurrent.BlockingQueue;

public interface IWritableMemChunk extends WALEntryValue {
  void putLong(long t, long v);

  void putInt(long t, int v);

  void putFloat(long t, float v);

  void putDouble(long t, double v);

  void putBinary(long t, Binary v);

  void putBoolean(long t, boolean v);

  void putAlignedRow(long t, Object[] v);

  void putLongs(long[] t, long[] v, BitMap bitMap, int start, int end);

  void putInts(long[] t, int[] v, BitMap bitMap, int start, int end);

  void putFloats(long[] t, float[] v, BitMap bitMap, int start, int end);

  void putDoubles(long[] t, double[] v, BitMap bitMap, int start, int end);

  void putBinaries(long[] t, Binary[] v, BitMap bitMap, int start, int end);

  void putBooleans(long[] t, boolean[] v, BitMap bitMap, int start, int end);

  void putAlignedTablet(
      long[] t, Object[] v, BitMap[] bitMaps, int start, int end, TSStatus[] results);

  void writeNonAlignedPoint(long insertTime, Object objectValue);

  void writeAlignedPoints(
      long insertTime, Object[] objectValue, List<IMeasurementSchema> schemaList);

  /**
   * write data in the range [start, end). Null value in the valueList will be replaced by the
   * subsequent non-null value, e.g., {1, null, 3, null, 5} will be {1, 3, 5, null, 5}
   */
  void writeNonAlignedTablet(
      long[] times, Object valueList, BitMap bitMap, TSDataType dataType, int start, int end);

  void writeAlignedTablet(
      long[] times,
      Object[] valueList,
      BitMap[] bitMaps,
      List<IMeasurementSchema> schemaList,
      int start,
      int end,
      TSStatus[] results);

  long count();

  long rowCount();

  IMeasurementSchema getSchema();

  /**
   * served for flush requests. The logic is just same as getSortedTVListForQuery, but without add
   * reference count
   *
   * <p>This interface should be synchronized for concurrent with getSortedTvListForQuery
   */
  void sortTvListForFlush();

  default long getMaxTime() {
    return Long.MAX_VALUE;
  }

  default long getMinTime() {
    return Long.MIN_VALUE;
  }

  /**
   * @return how many points are deleted
   */
  int delete(long lowerBound, long upperBound);

  IChunkWriter createIChunkWriter();

  void encode(BlockingQueue<Object> ioTaskQueue, BatchEncodeInfo encodeInfo, long[] times);

  void release();

  long getFirstPoint();

  long getLastPoint();

  boolean isEmpty();

  List<? extends TVList> getSortedList();

  TVList getWorkingTVList();

  void setWorkingTVList(TVList list);

  void setEncryptParameter(EncryptParameter encryptParameter);
}
