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

package org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.readchunk.loader;

import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.ModifiedStatus;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.write.PageException;
import org.apache.tsfile.file.header.PageHeader;
import org.apache.tsfile.file.metadata.ChunkMetadata;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.write.chunk.AlignedChunkWriterImpl;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

public abstract class PageLoader {

  protected String file;
  protected PageHeader pageHeader;
  protected CompressionType compressionType;
  protected TSDataType dataType;
  protected TSEncoding encoding;
  protected List<TimeRange> deleteIntervalList;
  protected ModifiedStatus modifiedStatus;
  protected ChunkMetadata chunkMetadata;

  protected PageLoader() {}

  protected PageLoader(
      String file,
      PageHeader pageHeader,
      CompressionType compressionType,
      TSDataType dataType,
      TSEncoding encoding,
      ChunkMetadata chunkMetadata,
      ModifiedStatus modifiedStatus) {
    this.file = file;
    this.pageHeader = pageHeader;
    this.compressionType = compressionType;
    this.dataType = dataType;
    this.encoding = encoding;
    this.chunkMetadata = chunkMetadata;
    this.deleteIntervalList = chunkMetadata.getDeleteIntervalList();
    this.modifiedStatus = modifiedStatus;
  }

  public String getFile() {
    return file;
  }

  public PageHeader getHeader() {
    return pageHeader;
  }

  public TSDataType getDataType() {
    return dataType;
  }

  public CompressionType getCompressionType() {
    return compressionType;
  }

  public TSEncoding getEncoding() {
    return encoding;
  }

  public abstract ByteBuffer getCompressedData() throws IOException;

  public abstract ByteBuffer getUnCompressedData() throws IOException;

  public ChunkMetadata getChunkMetadata() {
    return chunkMetadata;
  }

  public ModifiedStatus getModifiedStatus() {
    return modifiedStatus;
  }

  public List<TimeRange> getDeleteIntervalList() {
    if (this.modifiedStatus == ModifiedStatus.PARTIAL_DELETED) {
      return this.deleteIntervalList;
    } else if (this.modifiedStatus == ModifiedStatus.ALL_DELETED) {
      return Collections.singletonList(
          new TimeRange(pageHeader.getStartTime(), pageHeader.getEndTime()));
    } else {
      return null;
    }
  }

  public abstract void flushToTimeChunkWriter(AlignedChunkWriterImpl alignedChunkWriter)
      throws PageException;

  public abstract void flushToValueChunkWriter(
      AlignedChunkWriterImpl alignedChunkWriter, int valueColumnIndex)
      throws IOException, PageException;

  public abstract boolean isEmpty();

  public abstract void clear();
}
