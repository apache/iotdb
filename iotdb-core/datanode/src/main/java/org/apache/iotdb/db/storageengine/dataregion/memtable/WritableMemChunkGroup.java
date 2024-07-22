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
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternUtil;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.IWALByteBufferView;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALWriteUtils;

import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.apache.tsfile.write.schema.IMeasurementSchema;

import java.io.DataInputStream;
import java.io.IOException;
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
  public boolean writeValuesWithFlushCheck(
      long[] times,
      Object[] columns,
      BitMap[] bitMaps,
      List<IMeasurementSchema> schemaList,
      int start,
      int end,
      TSStatus[] results) {
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
    return flushFlag;
  }

  @Override
  public Map<String, IWritableMemChunk> getMemChunkMap() {
    return memChunkMap;
  }

  @SuppressWarnings("squid:S3776")
  @Override
  public int delete(
      PartialPath originalPath, PartialPath devicePath, long startTimestamp, long endTimestamp) {
    int deletedPointsNumber = 0;
    String targetMeasurement = originalPath.getMeasurement();
    if (PathPatternUtil.hasWildcard(targetMeasurement)) {
      Iterator<Entry<String, IWritableMemChunk>> iter = memChunkMap.entrySet().iterator();
      while (iter.hasNext()) {
        Entry<String, IWritableMemChunk> entry = iter.next();
        if (!PathPatternUtil.isNodeMatch(targetMeasurement, entry.getKey())) {
          continue;
        }
        IWritableMemChunk chunk = entry.getValue();
        if (startTimestamp == Long.MIN_VALUE && endTimestamp == Long.MAX_VALUE) {
          iter.remove();
          deletedPointsNumber += chunk.count();
          chunk.release();
        } else {
          deletedPointsNumber += chunk.delete(startTimestamp, endTimestamp);
          if (chunk.count() == 0) {
            iter.remove();
          }
        }
      }
    } else {
      IWritableMemChunk chunk = memChunkMap.get(targetMeasurement);
      if (chunk != null) {
        if (startTimestamp == Long.MIN_VALUE && endTimestamp == Long.MAX_VALUE) {
          memChunkMap.remove(targetMeasurement);
          deletedPointsNumber += chunk.count();
          chunk.release();
        } else {
          deletedPointsNumber += chunk.delete(startTimestamp, endTimestamp);
          if (chunk.count() == 0) {
            memChunkMap.remove(targetMeasurement);
          }
        }
      }
    }

    return deletedPointsNumber;
  }

  @Override
  public long getCurrentTVListSize(String measurement) {
    if (!memChunkMap.containsKey(measurement)) {
      return 0;
    }
    return memChunkMap.get(measurement).getTVList().rowCount();
  }

  @Override
  public long getMaxTime() {
    long maxTime = Long.MIN_VALUE;
    for (IWritableMemChunk memChunk : memChunkMap.values()) {
      maxTime = Math.max(maxTime, memChunk.getMaxTime());
    }
    return maxTime;
  }

  @Override
  public int serializedSize() {
    int size = 0;
    size += Integer.BYTES;
    for (Map.Entry<String, IWritableMemChunk> entry : memChunkMap.entrySet()) {
      size += ReadWriteIOUtils.sizeToWrite(entry.getKey());
      size += entry.getValue().serializedSize();
    }
    return size;
  }

  @Override
  public void serializeToWAL(IWALByteBufferView buffer) {
    buffer.putInt(memChunkMap.size());
    for (Map.Entry<String, IWritableMemChunk> entry : memChunkMap.entrySet()) {
      WALWriteUtils.write(entry.getKey(), buffer);
      IWritableMemChunk memChunk = entry.getValue();
      memChunk.serializeToWAL(buffer);
    }
  }

  public static WritableMemChunkGroup deserialize(DataInputStream stream) throws IOException {
    WritableMemChunkGroup memChunkGroup = new WritableMemChunkGroup();
    int memChunkMapSize = stream.readInt();
    for (int i = 0; i < memChunkMapSize; ++i) {
      String measurement = ReadWriteIOUtils.readString(stream);
      IWritableMemChunk memChunk = WritableMemChunk.deserialize(stream);
      memChunkGroup.memChunkMap.put(measurement, memChunk);
    }
    return memChunkGroup;
  }
}
