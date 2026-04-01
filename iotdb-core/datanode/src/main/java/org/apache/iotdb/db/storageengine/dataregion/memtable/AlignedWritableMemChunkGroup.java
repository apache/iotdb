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
import org.apache.iotdb.commons.path.AlignedPath;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.exception.DataTypeInconsistentException;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertNode;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModEntry;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.IWALByteBufferView;

import org.apache.tsfile.encrypt.EncryptParameter;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.write.schema.IMeasurementSchema;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class AlignedWritableMemChunkGroup implements IWritableMemChunkGroup {

  private AlignedWritableMemChunk memChunk;

  private EncryptParameter encryptParameter;

  public AlignedWritableMemChunkGroup(List<IMeasurementSchema> schemaList, boolean isTableModel) {
    memChunk = new AlignedWritableMemChunk(schemaList, isTableModel);
  }

  @TestOnly
  public AlignedWritableMemChunkGroup(
      AlignedWritableMemChunk memChunk, List<IMeasurementSchema> schemaList, boolean isTableModel) {
    this.memChunk = memChunk;
  }

  private AlignedWritableMemChunkGroup() {
    // Empty constructor
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
    memChunk.writeAlignedTablet(times, columns, bitMaps, schemaList, start, end, results);
  }

  @Override
  public void release() {
    memChunk.release();
  }

  @Override
  public long count() {
    return memChunk.count();
  }

  /**
   * Check whether this MemChunkGroup contains a measurement. If a VECTOR_PLACEHOLDER passed from
   * outer, always return true because AlignedMemChunkGroup existing.
   */
  @Override
  public boolean contains(String measurement) {
    // used for calculate memtable size
    if (AlignedPath.VECTOR_PLACEHOLDER.equals(measurement)) {
      return true;
    }
    return memChunk.containsMeasurement(measurement);
  }

  @Override
  public void writeRow(long insertTime, Object[] objectValue, List<IMeasurementSchema> schemaList) {
    memChunk.writeAlignedPoints(insertTime, objectValue, schemaList);
  }

  @Override
  public Map<String, IWritableMemChunk> getMemChunkMap() {
    if (memChunk.count() == 0) {
      return Collections.emptyMap();
    }
    return Collections.singletonMap("", memChunk);
  }

  @Override
  public boolean isEmpty() {
    return memChunk.isEmpty() || memChunk.isAllDeleted();
  }

  @Override
  public long delete(ModEntry modEntry) {
    int deletedPointsNumber = 0;
    Set<String> measurements = memChunk.getAllMeasurements();
    List<String> columnsToBeRemoved = new ArrayList<>();
    for (String measurement : measurements) {
      if (!modEntry.affects(measurement)) {
        continue;
      }

      Pair<Integer, Boolean> deletedNumAndIsFullyDeleted =
          memChunk.deleteDataFromAColumn(
              modEntry.getStartTime(), modEntry.getEndTime(), measurement);
      deletedPointsNumber += deletedNumAndIsFullyDeleted.left;
      if (Boolean.TRUE.equals(deletedNumAndIsFullyDeleted.right)) {
        columnsToBeRemoved.add(measurement);
      }
    }

    for (String columnToBeRemoved : columnsToBeRemoved) {
      memChunk.removeColumn(columnToBeRemoved);
    }
    return deletedPointsNumber;
  }

  public long deleteTime(ModEntry modEntry) {
    return memChunk.deleteTime(modEntry.getStartTime(), modEntry.getEndTime());
  }

  @Override
  public IWritableMemChunk getWritableMemChunk(String measurement) {
    return memChunk;
  }

  @Override
  public long getMaxTime() {
    return memChunk.getMaxTime();
  }

  public AlignedWritableMemChunk getAlignedMemChunk() {
    return memChunk;
  }

  @Override
  public int serializedSize() {
    return memChunk.serializedSize();
  }

  @Override
  public void serializeToWAL(IWALByteBufferView buffer) {
    memChunk.serializeToWAL(buffer);
  }

  @Override
  public void setEncryptParameter(EncryptParameter encryptParameter) {
    this.encryptParameter = encryptParameter;
    memChunk.setEncryptParameter(encryptParameter);
  }

  protected static AlignedWritableMemChunkGroup deserialize(
      DataInputStream stream, boolean isTableModel) throws IOException {
    AlignedWritableMemChunkGroup memChunkGroup = new AlignedWritableMemChunkGroup();
    memChunkGroup.memChunk = AlignedWritableMemChunk.deserialize(stream, isTableModel);
    return memChunkGroup;
  }

  @Override
  public void checkDataType(InsertNode node) throws DataTypeInconsistentException {
    memChunk.checkDataType(node);
  }

  protected static AlignedWritableMemChunkGroup deserializeSingleTVListMemChunks(
      DataInputStream stream, boolean isTableModel) throws IOException {
    AlignedWritableMemChunkGroup memChunkGroup = new AlignedWritableMemChunkGroup();
    memChunkGroup.memChunk =
        AlignedWritableMemChunk.deserializeSingleTVListMemChunks(stream, isTableModel);
    return memChunkGroup;
  }
}
