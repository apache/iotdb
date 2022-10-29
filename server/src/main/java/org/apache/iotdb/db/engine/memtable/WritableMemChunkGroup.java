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

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.wal.buffer.IWALByteBufferView;
import org.apache.iotdb.db.wal.utils.WALWriteUtils;
import org.apache.iotdb.tsfile.utils.BitMap;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static org.apache.iotdb.commons.conf.IoTDBConstant.MULTI_LEVEL_PATH_WILDCARD;
import static org.apache.iotdb.commons.conf.IoTDBConstant.ONE_LEVEL_PATH_WILDCARD;

public class WritableMemChunkGroup implements IWritableMemChunkGroup {

  private Map<String, IWritableMemChunk> memChunkMap;

  long latestTime;

  public WritableMemChunkGroup() {
    memChunkMap = new HashMap<>();
  }

  @Override
  public boolean writeValuesWithFlushCheck(
      long[] times,
      Object[] columns,
      BitMap[] bitMaps,
      List<IMeasurementSchema> schemaList,
      int start,
      int end) {
    boolean flushFlag = false;
    for (int i = 0; i < columns.length; i++) {
      if (columns[i] == null) {
        continue;
      }
      IWritableMemChunk memChunk = createMemChunkIfNotExistAndGet(schemaList.get(i));
      flushFlag |=
          memChunk.writeWithFlushCheck(
              times,
              columns[i],
              bitMaps == null ? null : bitMaps[i],
              schemaList.get(i).getType(),
              start,
              end);
    }
    if (latestTime < times[end - 1]) {
      latestTime = times[end - 1];
    }
    return flushFlag;
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
  public boolean writeWithFlushCheck(
      long insertTime, Object[] objectValue, List<IMeasurementSchema> schemaList) {
    boolean flushFlag = false;
    for (int i = 0; i < objectValue.length; i++) {
      if (objectValue[i] == null) {
        continue;
      }
      IWritableMemChunk memChunk = createMemChunkIfNotExistAndGet(schemaList.get(i));
      flushFlag |= memChunk.writeWithFlushCheck(insertTime, objectValue[i]);
    }
    if (latestTime < insertTime) {
      latestTime = insertTime;
    }
    return flushFlag;
  }

  @Override
  public Map<String, IWritableMemChunk> getMemChunkMap() {
    return memChunkMap;
  }

  @Override
  public int delete(
      PartialPath originalPath, PartialPath devicePath, long startTimestamp, long endTimestamp) {
    int deletedPointsNumber = 0;
    String targetMeasurement = originalPath.getMeasurement();
    if (targetMeasurement.equals(ONE_LEVEL_PATH_WILDCARD)
        || targetMeasurement.equals(MULTI_LEVEL_PATH_WILDCARD)) {
      Iterator<Entry<String, IWritableMemChunk>> iter = memChunkMap.entrySet().iterator();
      while (iter.hasNext()) {
        Entry<String, IWritableMemChunk> entry = iter.next();
        IWritableMemChunk chunk = entry.getValue();
        if (startTimestamp == Long.MIN_VALUE && endTimestamp == Long.MAX_VALUE) {
          iter.remove();
        }
        deletedPointsNumber += chunk.delete(startTimestamp, endTimestamp);
      }
    } else {
      IWritableMemChunk chunk = memChunkMap.get(targetMeasurement);
      if (chunk != null) {
        if (startTimestamp == Long.MIN_VALUE && endTimestamp == Long.MAX_VALUE) {
          memChunkMap.remove(targetMeasurement);
        }
        deletedPointsNumber += chunk.delete(startTimestamp, endTimestamp);
      }
    }

    return deletedPointsNumber;
  }

  @Override
  public long getCurrentTVListSize(String measurement) {
    return memChunkMap.get(measurement).getTVList().rowCount();
  }

  @Override
  public long getLatestTime() {
    return latestTime;
  }

  @Override
  public int serializedSize() {
    int size = 0;
    size += Integer.BYTES;
    size += Long.BYTES;
    for (Map.Entry<String, IWritableMemChunk> entry : memChunkMap.entrySet()) {
      size += ReadWriteIOUtils.sizeToWrite(entry.getKey());
      size += entry.getValue().serializedSize();
    }
    return size;
  }

  @Override
  public void serializeToWAL(IWALByteBufferView buffer) {
    buffer.putInt(memChunkMap.size());
    buffer.putLong(latestTime);
    for (Map.Entry<String, IWritableMemChunk> entry : memChunkMap.entrySet()) {
      WALWriteUtils.write(entry.getKey(), buffer);
      IWritableMemChunk memChunk = entry.getValue();
      memChunk.serializeToWAL(buffer);
    }
  }

  public static WritableMemChunkGroup deserialize(DataInputStream stream) throws IOException {
    WritableMemChunkGroup memChunkGroup = new WritableMemChunkGroup();
    int memChunkMapSize = stream.readInt();
    memChunkGroup.latestTime = stream.readLong();
    for (int i = 0; i < memChunkMapSize; ++i) {
      String measurement = ReadWriteIOUtils.readString(stream);
      IWritableMemChunk memChunk = WritableMemChunk.deserialize(stream);
      memChunkGroup.memChunkMap.put(measurement, memChunk);
    }
    return memChunkGroup;
  }
}
