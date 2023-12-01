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
import org.apache.iotdb.db.storageengine.dataregion.compaction.io.CompactionTsFileReader;
import org.apache.iotdb.tsfile.compress.IUnCompressor;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.TimeRange;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

public class LazyPageLoader implements PageLoader {

  private CompactionTsFileReader reader;
  private PageHeader pageHeader;
  private long startOffset;
  private ByteBuffer pageData;
  private CompressionType compressionType;
  private TSDataType dataType;
  private TSEncoding encoding;
  private List<TimeRange> deleteIntervalList;
  private ModifiedStatus modifiedStatus;

  public LazyPageLoader(
      CompactionTsFileReader reader,
      PageHeader pageHeader,
      long startOffset,
      CompressionType compressionType,
      TSDataType dataType,
      TSEncoding encoding,
      List<TimeRange> deleteIntervalList,
      ModifiedStatus modifiedStatus) {
    this.reader = reader;
    this.pageHeader = pageHeader;
    this.startOffset = startOffset;
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
  public ByteBuffer getCompressedData() throws IOException {
    if (this.pageData != null) {
      return pageData;
    }
    pageData = reader.readPageWithoutUnCompressing(startOffset, pageHeader.getCompressedSize());
    return pageData;
  }

  @Override
  public ByteBuffer getUnCompressedData() throws IOException {
    this.pageData = getCompressedData();
    byte[] unCompressedData = new byte[pageHeader.getUncompressedSize()];
    IUnCompressor unCompressor = IUnCompressor.getUnCompressor(compressionType);
    unCompressor.uncompress(
        pageData.array(), 0, pageHeader.getCompressedSize(), unCompressedData, 0);
    return ByteBuffer.wrap(unCompressedData);
  }

  @Override
  public ModifiedStatus getModifiedStatus() {
    return modifiedStatus;
  }

  @Override
  public List<TimeRange> getDeleteIntervalList() {
    return deleteIntervalList;
  }

  @Override
  public boolean isEmpty() {
    return pageHeader == null;
  }

  @Override
  public void clear() {
    this.pageHeader = null;
    this.pageData = null;
  }
}
