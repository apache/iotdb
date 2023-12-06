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
import org.apache.iotdb.tsfile.compress.IUnCompressor;
import org.apache.iotdb.tsfile.exception.write.PageException;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.write.chunk.AlignedChunkWriterImpl;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

public class InstantPageLoader implements PageLoader {

  private PageHeader pageHeader;
  private ByteBuffer pageData;
  private CompressionType compressionType;
  private TSDataType dataType;
  private TSEncoding encoding;
  private List<TimeRange> deleteIntervalList;
  private ModifiedStatus modifiedStatus;

  public InstantPageLoader() {
  }

  public InstantPageLoader(
      PageHeader pageHeader,
      ByteBuffer pageData,
      CompressionType compressionType,
      TSDataType dataType,
      TSEncoding encoding,
      List<TimeRange> deleteIntervalList,
      ModifiedStatus modifiedStatus) {
    this.pageHeader = pageHeader;
    this.pageData = pageData;
    this.compressionType = compressionType;
    this.dataType = dataType;
    this.encoding = encoding;
    this.deleteIntervalList = deleteIntervalList;
    this.modifiedStatus = modifiedStatus;
  }

  @Override
  public PageHeader getHeader() {
    return pageHeader;
  }

  @Override
  public TSDataType getDataType() {
    return dataType;
  }

  @Override
  public CompressionType getCompressionType() {
    return compressionType;
  }

  @Override
  public TSEncoding getEncoding() {
    return encoding;
  }

  @Override
  public ByteBuffer getCompressedData() {
    return pageData;
  }

  @Override
  public ByteBuffer getUnCompressedData() throws IOException {
    byte[] unCompressedData = new byte[pageHeader.getUncompressedSize()];
    IUnCompressor unCompressor = IUnCompressor.getUnCompressor(compressionType);
    unCompressor.uncompress(
        pageData.array(), 0, pageHeader.getCompressedSize(), unCompressedData, 0);
    return ByteBuffer.wrap(unCompressedData);
  }

  @Override
  public ModifiedStatus getModifiedStatus() {
    return this.modifiedStatus;
  }

  @Override
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

  @Override
  public void flushToTimeChunkWriter(AlignedChunkWriterImpl alignedChunkWriter)
      throws PageException {
    alignedChunkWriter.writePageHeaderAndDataIntoTimeBuff(pageData, pageHeader);
    clear();
  }

  @Override
  public void flushToValueChunkWriter(
      AlignedChunkWriterImpl alignedChunkWriter, int valueColumnIndex)
      throws IOException, PageException {
    if (isEmpty()) {
      alignedChunkWriter.getValueChunkWriterByIndex(valueColumnIndex).writeEmptyPageToPageBuffer();
    } else {
      alignedChunkWriter
          .getValueChunkWriterByIndex(valueColumnIndex)
          .writePageHeaderAndDataIntoBuff(pageData, pageHeader);
      clear();
    }
  }

  @Override
  public boolean isEmpty() {
    return pageHeader == null
        || pageData == null
        || pageHeader.getUncompressedSize() == 0
        || this.modifiedStatus == ModifiedStatus.ALL_DELETED;
  }

  @Override
  public void clear() {
    this.deleteIntervalList = null;
    this.pageHeader = null;
    this.pageData = null;
  }
}
