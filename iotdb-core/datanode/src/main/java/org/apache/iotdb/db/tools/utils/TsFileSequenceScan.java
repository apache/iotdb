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
package org.apache.iotdb.db.tools.utils;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.file.MetaMarker;
import org.apache.tsfile.file.header.ChunkGroupHeader;
import org.apache.tsfile.file.header.ChunkHeader;
import org.apache.tsfile.file.header.PageHeader;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.utils.Pair;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.ByteBuffer;

public abstract class TsFileSequenceScan {

  protected PrintWriter printWriter = null;

  protected TsFileSequenceReader reader;
  protected byte marker;
  protected File file;

  protected IDeviceID currDeviceID;
  protected String currMeasurementID;
  protected Pair<IDeviceID, String> currTimeseriesID;
  protected boolean currChunkOnePage;

  public TsFileSequenceScan() {}

  /**
   * @return true if the file should be scanned
   */
  protected boolean onFileOpen(File file) throws IOException {
    // skip head magic string
    reader.position((long) TSFileConfig.MAGIC_STRING.getBytes().length + 1);
    this.file = file;
    return true;
  }

  protected void onFileEnd() throws IOException {}

  protected void onChunk(PageVisitor pageVisitor) throws IOException {
    ChunkHeader chunkHeader = reader.readChunkHeader(marker);
    if (chunkHeader.getDataSize() == 0) {
      // empty value chunk
      return;
    }
    currMeasurementID = chunkHeader.getMeasurementID();
    currTimeseriesID = new Pair<>(currDeviceID, currMeasurementID);

    int dataSize = chunkHeader.getDataSize();

    while (dataSize > 0) {
      PageHeader pageHeader =
          reader.readPageHeader(
              chunkHeader.getDataType(),
              (chunkHeader.getChunkType() & 0x3F) == MetaMarker.CHUNK_HEADER);
      ByteBuffer pageData = reader.readPage(pageHeader, chunkHeader.getCompressionType());
      pageVisitor.onPage(pageHeader, pageData, chunkHeader);
      dataSize -= pageHeader.getSerializedPageSize();
    }
  }

  protected void onChunkGroup() throws IOException {
    ChunkGroupHeader chunkGroupHeader = reader.readChunkGroupHeader();
    currDeviceID = chunkGroupHeader.getDeviceID();
  }

  protected abstract void onTimePage(
      PageHeader pageHeader, ByteBuffer pageData, ChunkHeader chunkHeader) throws IOException;

  protected abstract void onValuePage(
      PageHeader pageHeader, ByteBuffer pageData, ChunkHeader chunkHeader) throws IOException;

  protected abstract void onNonAlignedPage(
      PageHeader pageHeader, ByteBuffer pageData, ChunkHeader chunkHeader) throws IOException;

  protected interface PageVisitor {
    void onPage(PageHeader pageHeader, ByteBuffer pageData, ChunkHeader chunkHeader)
        throws IOException;
  }

  @SuppressWarnings("java:S106")
  protected void printBoth(String msg) {
    System.out.println(msg);
    if (printWriter != null) {
      printWriter.println(msg);
    }
  }

  protected abstract void onException(Throwable t);

  @SuppressWarnings("java:S1181")
  public void scanTsFile(File tsFile) {
    try (TsFileSequenceReader r = new TsFileSequenceReader(tsFile.getAbsolutePath())) {
      this.reader = r;
      boolean shouldScan = onFileOpen(tsFile);
      if (!shouldScan) {
        return;
      }
      // start reading data points in sequence
      while ((marker = reader.readMarker()) != MetaMarker.SEPARATOR) {
        switch (marker) {
          case MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER:
            currChunkOnePage = true;
            onChunk(this::onNonAlignedPage);
            break;
          case MetaMarker.CHUNK_HEADER:
            currChunkOnePage = false;
            onChunk(this::onNonAlignedPage);
            break;
          case MetaMarker.ONLY_ONE_PAGE_TIME_CHUNK_HEADER:
            currChunkOnePage = true;
            onChunk(this::onTimePage);
            break;
          case MetaMarker.TIME_CHUNK_HEADER:
            currChunkOnePage = false;
            onChunk(this::onTimePage);
            break;
          case MetaMarker.ONLY_ONE_PAGE_VALUE_CHUNK_HEADER:
            currChunkOnePage = true;
            onChunk(this::onValuePage);
            break;
          case MetaMarker.VALUE_CHUNK_HEADER:
            currChunkOnePage = false;
            onChunk(this::onValuePage);
            break;
          case MetaMarker.CHUNK_GROUP_HEADER:
            onChunkGroup();
            break;
          case MetaMarker.OPERATION_INDEX_RANGE:
            reader.readPlanIndex();
            break;
          default:
            MetaMarker.handleUnexpectedMarker(marker);
        }
      }

      onFileEnd();
    } catch (Throwable e) {
      onException(e);
    }
  }

  public void setPrintWriter(PrintWriter printWriter) {
    this.printWriter = printWriter;
  }
}
