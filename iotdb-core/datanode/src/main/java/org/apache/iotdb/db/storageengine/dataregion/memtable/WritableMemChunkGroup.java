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
import org.apache.iotdb.db.exception.DataTypeInconsistentException;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertNode;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModEntry;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.IWALByteBufferView;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALWriteUtils;

import org.apache.tsfile.encrypt.EncryptParameter;
import org.apache.tsfile.encrypt.EncryptUtils;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class WritableMemChunkGroup implements IWritableMemChunkGroup {

  private Map<String, IWritableMemChunk> memChunkMap;
  private EncryptParameter encryptParameter;

  public WritableMemChunkGroup() {
    memChunkMap = new HashMap<>();
    encryptParameter = EncryptUtils.getEncryptParameter();
  }

  public WritableMemChunkGroup(EncryptParameter encryptParameter) {
    memChunkMap = new HashMap<>();
    this.encryptParameter = encryptParameter;
  }

  @Override
  public void writeTablet(
      long[] times,
      Object[] columns,
      BitMap[] bitMaps,
      List<IMeasurementSchema> schemaList,
      int start,
      int end,
      TSStatus[] results) {
    for (int i = 0; i < columns.length; i++) {
      if (columns[i] == null || schemaList.get(i) == null) {
        continue;
      }
      IWritableMemChunk memChunk = createMemChunkIfNotExistAndGet(schemaList.get(i));
      memChunk.writeNonAlignedTablet(
          times,
          columns[i],
          bitMaps == null ? null : bitMaps[i],
          schemaList.get(i).getType(),
          start,
          end);
    }
  }

  private IWritableMemChunk createMemChunkIfNotExistAndGet(IMeasurementSchema schema) {
    return memChunkMap.computeIfAbsent(
        schema.getMeasurementName(), k -> new WritableMemChunk(schema, encryptParameter));
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
  public void writeRow(long insertTime, Object[] objectValue, List<IMeasurementSchema> schemaList) {
    for (int i = 0; i < objectValue.length; i++) {
      if (objectValue[i] == null || schemaList.get(i) == null) {
        continue;
      }
      IWritableMemChunk memChunk = createMemChunkIfNotExistAndGet(schemaList.get(i));
      memChunk.writeNonAlignedPoint(insertTime, objectValue[i]);
    }
  }

  @Override
  public Map<String, IWritableMemChunk> getMemChunkMap() {
    return memChunkMap;
  }

  @Override
  public boolean isEmpty() {
    return memChunkMap.isEmpty() || count() == 0;
  }

  @Override
  public long delete(ModEntry modEntry) {
    Iterator<Entry<String, IWritableMemChunk>> iter = memChunkMap.entrySet().iterator();
    long deletedPointsNumber = 0;
    while (iter.hasNext()) {
      Entry<String, IWritableMemChunk> entry = iter.next();
      if (!modEntry.affects(entry.getKey())) {
        continue;
      }

      IWritableMemChunk chunk = entry.getValue();
      if (modEntry.getStartTime() == Long.MIN_VALUE && modEntry.getEndTime() == Long.MAX_VALUE) {
        iter.remove();
        deletedPointsNumber += chunk.count();
        chunk.release();
      } else {
        deletedPointsNumber += chunk.delete(modEntry.getStartTime(), modEntry.getEndTime());
        if (chunk.count() == 0) {
          iter.remove();
        }
      }
    }
    return deletedPointsNumber;
  }

  @Override
  public long deleteTime(ModEntry modEntry) {
    return delete(modEntry);
  }

  @Override
  public IWritableMemChunk getWritableMemChunk(String measurement) {
    if (!memChunkMap.containsKey(measurement)) {
      return null;
    }
    return memChunkMap.get(measurement);
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

  @Override
  public void setEncryptParameter(EncryptParameter encryptParameter) {
    this.encryptParameter = encryptParameter;
    for (IWritableMemChunk memChunk : memChunkMap.values()) {
      memChunk.setEncryptParameter(encryptParameter);
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

  @Override
  public void checkDataType(InsertNode node) throws DataTypeInconsistentException {
    for (MeasurementSchema incomingSchema : node.getMeasurementSchemas()) {
      if (incomingSchema == null) {
        continue;
      }

      IWritableMemChunk memChunk = memChunkMap.get(incomingSchema.getMeasurementName());
      if (memChunk != null && memChunk.getSchema().getType() != incomingSchema.getType()) {
        throw new DataTypeInconsistentException(
            memChunk.getWorkingTVList().getDataType(), incomingSchema.getType());
      }
    }
  }

  public static WritableMemChunkGroup deserializeSingleTVListMemChunks(DataInputStream stream)
      throws IOException {
    WritableMemChunkGroup memChunkGroup = new WritableMemChunkGroup();
    int memChunkMapSize = stream.readInt();
    for (int i = 0; i < memChunkMapSize; ++i) {
      String measurement = ReadWriteIOUtils.readString(stream);
      IWritableMemChunk memChunk = WritableMemChunk.deserializeSingleTVListMemChunks(stream);
      memChunkGroup.memChunkMap.put(measurement, memChunk);
    }
    return memChunkGroup;
  }
}
