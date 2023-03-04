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
package org.apache.iotdb.db.engine.memtable;

import org.apache.iotdb.db.utils.datastructure.TVList;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.BitMap;
import org.apache.iotdb.tsfile.write.chunk.IChunkWriter;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;

import java.util.List;

public interface IWritableMemChunk {

  void putLong(long t, long v);

  void putInt(long t, int v);

  void putFloat(long t, float v);

  void putDouble(long t, double v);

  void putBinary(long t, Binary v);

  void putBoolean(long t, boolean v);

  void putAlignedValue(long t, Object[] v, int[] columnIndexArray);

  void putLongs(long[] t, long[] v, BitMap bitMap, int start, int end);

  void putInts(long[] t, int[] v, BitMap bitMap, int start, int end);

  void putFloats(long[] t, float[] v, BitMap bitMap, int start, int end);

  void putDoubles(long[] t, double[] v, BitMap bitMap, int start, int end);

  void putBinaries(long[] t, Binary[] v, BitMap bitMap, int start, int end);

  void putBooleans(long[] t, boolean[] v, BitMap bitMap, int start, int end);

  void putAlignedValues(
      long[] t, Object[] v, BitMap[] bitMaps, int[] columnIndexArray, int start, int end);

  void write(long insertTime, Object objectValue);

  void writeAlignedValue(
      long insertTime,
      Object[] objectValue,
      List<Integer> failedIndices,
      List<IMeasurementSchema> schemaList);

  /**
   * write data in the range [start, end). Null value in the valueList will be replaced by the
   * subsequent non-null value, e.g., {1, null, 3, null, 5} will be {1, 3, 5, null, 5}
   */
  void write(
      long[] times, Object valueList, BitMap bitMap, TSDataType dataType, int start, int end);

  void writeAlignedValues(
      long[] times,
      Object[] valueList,
      BitMap[] bitMaps,
      List<Integer> failedIndices,
      List<IMeasurementSchema> schemaList,
      int start,
      int end);

  long count();

  IMeasurementSchema getSchema();

  /**
   * served for query requests.
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
   * served for vector query requests.
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

  default long getMinTime() {
    return Long.MIN_VALUE;
  }

  /** @return how many points are deleted */
  int delete(long lowerBound, long upperBound);

  IChunkWriter createIChunkWriter();

  void encode(IChunkWriter chunkWriter);

  void release();

  long getFirstPoint();

  long getLastPoint();
}
