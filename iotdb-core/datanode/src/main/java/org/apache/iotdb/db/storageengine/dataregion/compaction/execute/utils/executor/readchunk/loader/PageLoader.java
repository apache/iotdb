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
import org.apache.iotdb.tsfile.exception.write.PageException;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.write.chunk.AlignedChunkWriterImpl;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

public abstract class PageLoader {

  protected PageHeader pageHeader;
  protected CompressionType compressionType;
  protected TSDataType dataType;
  protected TSEncoding encoding;
  protected List<TimeRange> deleteIntervalList;
  protected ModifiedStatus modifiedStatus;

  protected PageLoader() {}

  protected PageLoader(
      PageHeader pageHeader,
      CompressionType compressionType,
      TSDataType dataType,
      TSEncoding encoding,
      List<TimeRange> deleteIntervalList,
      ModifiedStatus modifiedStatus) {
    this.pageHeader = pageHeader;
    this.compressionType = compressionType;
    this.dataType = dataType;
    this.encoding = encoding;
    this.deleteIntervalList = deleteIntervalList;
    this.modifiedStatus = modifiedStatus;
  }

  public PageHeader getPageHeader() {
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

  public ModifiedStatus getModifiedStatus() {
    return modifiedStatus;
  }

  public List<TimeRange> getDeleteIntervalList() {
    return deleteIntervalList;
  }

  public abstract void flushToTimeChunkWriter(AlignedChunkWriterImpl alignedChunkWriter)
      throws PageException;

  public abstract void flushToValueChunkWriter(
      AlignedChunkWriterImpl alignedChunkWriter, int valueColumnIndex)
      throws IOException, PageException;

  public abstract boolean isEmpty();

  public abstract void clear();
}
