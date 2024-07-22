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

import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.WALEntryValue;
import org.apache.iotdb.db.utils.datastructure.TVList;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.write.chunk.IChunkWriter;
import org.apache.tsfile.write.schema.IMeasurementSchema;

import java.util.List;

public interface IWritableMemChunk extends WALEntryValue {

  boolean putLongWithFlushCheck(long t, long v);

  boolean putIntWithFlushCheck(long t, int v);

  boolean putFloatWithFlushCheck(long t, float v);

  boolean putDoubleWithFlushCheck(long t, double v);

  boolean putBinaryWithFlushCheck(long t, Binary v);

  boolean putBooleanWithFlushCheck(long t, boolean v);

  boolean putAlignedValueWithFlushCheck(long t, Object[] v);

  boolean putLongsWithFlushCheck(long[] t, long[] v, BitMap bitMap, int start, int end);

  boolean putIntsWithFlushCheck(long[] t, int[] v, BitMap bitMap, int start, int end);

  boolean putFloatsWithFlushCheck(long[] t, float[] v, BitMap bitMap, int start, int end);

  boolean putDoublesWithFlushCheck(long[] t, double[] v, BitMap bitMap, int start, int end);

  boolean putBinariesWithFlushCheck(long[] t, Binary[] v, BitMap bitMap, int start, int end);

  boolean putBooleansWithFlushCheck(long[] t, boolean[] v, BitMap bitMap, int start, int end);

  boolean putAlignedValuesWithFlushCheck(
      long[] t, Object[] v, BitMap[] bitMaps, int start, int end);

  boolean writeWithFlushCheck(long insertTime, Object objectValue);

  boolean writeAlignedValueWithFlushCheck(
      long insertTime, Object[] objectValue, List<IMeasurementSchema> schemaList);

  /**
   * write data in the range [start, end). Null value in the valueList will be replaced by the
   * subsequent non-null value, e.g., {1, null, 3, null, 5} will be {1, 3, 5, null, 5}
   */
  boolean writeWithFlushCheck(
      long[] times, Object valueList, BitMap bitMap, TSDataType dataType, int start, int end);

  boolean writeAlignedValuesWithFlushCheck(
      long[] times,
      Object[] valueList,
      BitMap[] bitMaps,
      List<IMeasurementSchema> schemaList,
      int start,
      int end);

  long count();

  IMeasurementSchema getSchema();

  /**
   * served for read requests.
   *
   * <p>if tv list has been sorted, just return reference of it
   *
   * <p>if tv list hasn't been sorted and has no reference, sort and return reference of it
   *
   * <p>if tv list hasn't been sorted and has reference we should copy and sort it, then return ths
   * list
   *
   * <p>the mechanism is just like copy on write
   *
   * <p>This interface should be synchronized for concurrent with sortTvListForFlush
   *
   * @return sorted tv list
   */
  TVList getSortedTvListForQuery();

  /**
   * served for vector read requests.
   *
   * <p>the mechanism is just like copy on write
   *
   * <p>This interface should be synchronized for concurrent with sortTvListForFlush
   *
   * @return sorted tv list
   */
  TVList getSortedTvListForQuery(List<IMeasurementSchema> schemaList);

  /**
   * served for flush requests. The logic is just same as getSortedTVListForQuery, but without add
   * reference count
   *
   * <p>This interface should be synchronized for concurrent with getSortedTvListForQuery
   */
  void sortTvListForFlush();

  default TVList getTVList() {
    return null;
  }

  default long getMaxTime() {
    return Long.MAX_VALUE;
  }

  /**
   * @return how many points are deleted
   */
  int delete(long lowerBound, long upperBound);

  IChunkWriter createIChunkWriter();

  void encode(IChunkWriter chunkWriter);

  void release();

  long getFirstPoint();

  long getLastPoint();

  boolean isEmpty();
}
