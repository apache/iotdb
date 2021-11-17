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

import org.apache.iotdb.tsfile.utils.BitMap;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class AlignedWritableMemChunkGroup implements IWritableMemChunkGroup {

  private AlignedWritableMemChunk memChunk;

  public AlignedWritableMemChunkGroup(List<IMeasurementSchema> schemaList) {
    memChunk = new AlignedWritableMemChunk(schemaList);
  }

  @Override
  public void writeValues(
      long[] times,
      Object[] columns,
      BitMap[] bitMaps,
      List<IMeasurementSchema> schemaList,
      int start,
      int end) {
    memChunk.writeAlignedValues(times, columns, bitMaps, schemaList, start, end);
  }

  @Override
  public void release() {
    memChunk.release();
  }

  @Override
  public long count() {
    return memChunk.count();
  }

  @Override
  public boolean contains(String measurement) {
    return memChunk.containsMeasurement(measurement);
  }

  @Override
  public void write(long insertTime, Object[] objectValue, List<IMeasurementSchema> schemaList) {
    memChunk.writeAlignedValue(insertTime, objectValue, schemaList);
  }

  @Override
  public Map<String, IWritableMemChunk> getMemChunkMap() {
    return Collections.singletonMap("", memChunk);
  }

  @Override
  public long getCurrentChunkPointNum(String measurement) {
    return memChunk.count();
  }

  public AlignedWritableMemChunk getAlignedMemChunk() {
    return memChunk;
  }
}
