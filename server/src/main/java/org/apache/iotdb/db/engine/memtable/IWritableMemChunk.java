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
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

public interface IWritableMemChunk {

  void putLong(long t, long v);

  void putInt(long t, int v);

  void putFloat(long t, float v);

  void putDouble(long t, double v);

  void putBinary(long t, Binary v);

  void putBoolean(long t, boolean v);

  void putLongs(long[] t, long[] v, int start, int end);

  void putInts(long[] t, int[] v, int start, int end);

  void putFloats(long[] t, float[] v, int start, int end);

  void putDoubles(long[] t, double[] v, int start, int end);

  void putBinaries(long[] t, Binary[] v, int start, int end);

  void putBooleans(long[] t, boolean[] v, int start, int end);


  void write(long insertTime, Object objectValue);

  /**
   * [start, end)
   */
  void write(long[] times, Object valueList, TSDataType dataType, int start, int end);

  long count();

  MeasurementSchema getSchema();

  /**
   * served for query requests.
   *
   * @return
   */
  default TVList getSortedTVList() {
    return null;
  }

  default TVList getTVList() {
    return null;
  }

  default long getMinTime() {
    return Long.MIN_VALUE;
  }

  /**
   * @return how many points are deleted
   */
  int delete(long lowerBound, long upperBound);

}
