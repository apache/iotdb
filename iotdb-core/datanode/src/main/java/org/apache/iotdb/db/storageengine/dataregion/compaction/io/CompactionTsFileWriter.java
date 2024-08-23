/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.storageengine.dataregion.compaction.io;

import org.apache.iotdb.db.service.metrics.CompactionMetrics;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.CompactionTaskManager;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.constant.CompactionIoDataType;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.constant.CompactionType;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.ChunkMetadata;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.read.common.Chunk;
import org.apache.tsfile.write.chunk.AlignedChunkWriterImpl;
import org.apache.tsfile.write.chunk.IChunkWriter;
import org.apache.tsfile.write.record.Tablet.ColumnType;
import org.apache.tsfile.write.writer.TsFileIOWriter;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class CompactionTsFileWriter extends TsFileIOWriter {
  CompactionType type;

  private volatile boolean isWritingAligned = false;
  private boolean isEmptyTargetFile = true;
  private IDeviceID currentDeviceId;

  public CompactionTsFileWriter(File file, long maxMetadataSize, CompactionType type)
      throws IOException {
    super(file, maxMetadataSize);
    this.type = type;
    super.out =
        new CompactionTsFileOutput(
            super.out, CompactionTaskManager.getInstance().getMergeWriteRateLimiter());
  }

  public void markStartingWritingAligned() {
    isWritingAligned = true;
  }

  public void markEndingWritingAligned() {
    isWritingAligned = false;
  }

  public void writeChunk(IChunkWriter chunkWriter) throws IOException {
    boolean isAligned = chunkWriter instanceof AlignedChunkWriterImpl;
    long beforeOffset = this.getPos();
    if (!chunkWriter.isEmpty()) {
      isEmptyTargetFile = false;
    }
    chunkWriter.writeToFileWriter(this);
    long writtenDataSize = this.getPos() - beforeOffset;
    CompactionMetrics.getInstance()
        .recordWriteInfo(
            type,
            isAligned ? CompactionIoDataType.ALIGNED : CompactionIoDataType.NOT_ALIGNED,
            writtenDataSize);
  }

  @Override
  public void writeChunk(Chunk chunk, ChunkMetadata chunkMetadata) throws IOException {
    long beforeOffset = this.getPos();
    if (chunkMetadata.getNumOfPoints() != 0) {
      isEmptyTargetFile = false;
    }
    super.writeChunk(chunk, chunkMetadata);
    long writtenDataSize = this.getPos() - beforeOffset;
    CompactionMetrics.getInstance()
        .recordWriteInfo(
            type,
            isWritingAligned ? CompactionIoDataType.ALIGNED : CompactionIoDataType.NOT_ALIGNED,
            writtenDataSize);
  }

  @Override
  public void writeEmptyValueChunk(
      String measurementId,
      CompressionType compressionType,
      TSDataType tsDataType,
      TSEncoding encodingType,
      Statistics<? extends Serializable> statistics)
      throws IOException {
    long beforeOffset = this.getPos();
    super.writeEmptyValueChunk(
        measurementId, compressionType, tsDataType, encodingType, statistics);
    long writtenDataSize = this.getPos() - beforeOffset;
    CompactionMetrics.getInstance()
        .recordWriteInfo(type, CompactionIoDataType.ALIGNED, writtenDataSize);
  }

  @Override
  public int checkMetadataSizeAndMayFlush() throws IOException {
    int size = super.checkMetadataSizeAndMayFlush();
    CompactionMetrics.getInstance().recordWriteInfo(type, CompactionIoDataType.METADATA, size);
    return size;
  }

  @Override
  public int startChunkGroup(IDeviceID deviceId) throws IOException {
    currentDeviceId = deviceId;
    return super.startChunkGroup(deviceId);
  }

  @Override
  public void endChunkGroup() throws IOException {
    if (currentDeviceId == null || chunkMetadataList.isEmpty()) {
      return;
    }
    String tableName = currentDeviceId.getTableName();
    TableSchema tableSchema = getSchema().getTableSchemaMap().get(tableName);
    boolean generateTableSchemaForCurrentChunkGroup = tableSchema != null;
    setGenerateTableSchema(generateTableSchemaForCurrentChunkGroup);
    super.endChunkGroup();
    currentDeviceId = null;
  }

  @Override
  public void endFile() throws IOException {
    removeUnusedTableSchema();
    long beforeSize = this.getPos();
    super.endFile();
    long writtenDataSize = this.getPos() - beforeSize;
    CompactionMetrics.getInstance()
        .recordWriteInfo(type, CompactionIoDataType.METADATA, writtenDataSize);
  }

  public boolean isEmptyTargetFile() {
    return isEmptyTargetFile;
  }

  private void removeUnusedTableSchema() {
    Map<String, TableSchema> tableSchemaMap = getSchema().getTableSchemaMap();
    Iterator<Map.Entry<String, TableSchema>> iterator = tableSchemaMap.entrySet().iterator();
    while (iterator.hasNext()) {
      Map.Entry<String, TableSchema> entry = iterator.next();
      List<ColumnType> columnTypes = entry.getValue().getColumnTypes();
      if (columnTypes.contains(ColumnType.MEASUREMENT)) {
        continue;
      }
      iterator.remove();
    }
  }
}
