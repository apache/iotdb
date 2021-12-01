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

import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.tsfile.utils.BitMap;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class WritableMemChunkGroup implements IWritableMemChunkGroup {

  private Map<String, IWritableMemChunk> memChunkMap;

  public WritableMemChunkGroup() {
    memChunkMap = new HashMap<>();
  }

  @Override
  public void writeValues(
      long[] times,
      Object[] columns,
      BitMap[] bitMaps,
      List<IMeasurementSchema> schemaList,
      int start,
      int end) {
    int emptyColumnCount = 0;
    for (int i = 0; i < columns.length; i++) {
      if (columns[i] == null) {
        emptyColumnCount++;
        continue;
      }
      IWritableMemChunk memChunk =
          createMemChunkIfNotExistAndGet(schemaList.get(i - emptyColumnCount));
      memChunk.write(
          times,
          columns[i],
          bitMaps == null ? null : bitMaps[i],
          schemaList.get(i - emptyColumnCount).getType(),
          start,
          end);
    }
  }

  private IWritableMemChunk createMemChunkIfNotExistAndGet(IMeasurementSchema schema) {
    return memChunkMap.computeIfAbsent(
        schema.getMeasurementId(), k -> new WritableMemChunk(schema));
  }

  @Override
  public void release() {
    for (IWritableMemChunk memChunk : memChunkMap.values()) {
      memChunk.release();
    }
  }

  @Override
  public long count() {
    long count = 0;
    for (IWritableMemChunk memChunk : memChunkMap.values()) {
      count += memChunk.count();
    }
    return count;
  }

  @Override
  public boolean contains(String measurement) {
    return memChunkMap.containsKey(measurement);
  }

  @Override
  public void write(long insertTime, Object[] objectValue, List<IMeasurementSchema> schemaList) {
    int emptyColumnCount = 0;
    for (int i = 0; i < objectValue.length; i++) {
      if (objectValue[i] == null) {
        emptyColumnCount++;
        continue;
      }
      IWritableMemChunk memChunk =
          createMemChunkIfNotExistAndGet(schemaList.get(i - emptyColumnCount));
      memChunk.write(insertTime, objectValue[i]);
    }
  }

  @Override
  public Map<String, IWritableMemChunk> getMemChunkMap() {
    return memChunkMap;
  }

  @Override
  public int delete(
      PartialPath originalPath, PartialPath devicePath, long startTimestamp, long endTimestamp) {
    int deletedPointsNumber = 0;
    Iterator<Entry<String, IWritableMemChunk>> iter = memChunkMap.entrySet().iterator();
    while (iter.hasNext()) {
      Entry<String, IWritableMemChunk> entry = iter.next();
      IWritableMemChunk chunk = entry.getValue();
      // the key is measurement rather than component of multiMeasurement
      PartialPath fullPath = devicePath.concatNode(entry.getKey());
      if (originalPath.matchFullPath(fullPath)) {
        // matchFullPath ensures this branch could work on delete data of unary or multi measurement
        // and delete timeseries
        if (startTimestamp == Long.MIN_VALUE && endTimestamp == Long.MAX_VALUE) {
          iter.remove();
        }
        deletedPointsNumber += chunk.delete(startTimestamp, endTimestamp);
      }
    }
    return deletedPointsNumber;
  }

  @Override
  public long getCurrentChunkPointNum(String measurement) {
    return memChunkMap.get(measurement).count();
  }
}
