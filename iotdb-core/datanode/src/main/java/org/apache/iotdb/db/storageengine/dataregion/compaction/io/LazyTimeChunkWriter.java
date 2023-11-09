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

package org.apache.iotdb.db.storageengine.dataregion.compaction.io;

import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.reader.LazyPageLoader;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.encoding.encoder.Encoder;
import org.apache.iotdb.tsfile.enums.TSDataType;
import org.apache.iotdb.tsfile.exception.write.PageException;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.file.metadata.statistics.TimeStatistics;
import org.apache.iotdb.tsfile.utils.ReadWriteForEncodingUtils;
import org.apache.iotdb.tsfile.write.chunk.TimeChunkWriter;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;

import java.io.IOException;
import java.util.LinkedList;

public class LazyTimeChunkWriter extends TimeChunkWriter {

  private LinkedList<Integer> insertPagePositions = new LinkedList<>();
  private LinkedList<LazyPageLoader> pageLoaders = new LinkedList<>();
  private long sizeOfUnLoadPage = 0;

  public LazyTimeChunkWriter(
      String measurementId,
      CompressionType compressionType,
      TSEncoding encodingType,
      Encoder timeEncoder) {
    super(measurementId, compressionType, encodingType, timeEncoder);
  }

  @Override
  public void writePageToPageBuffer() {
    try {
      if (numOfPages == 0) { // record the firstPageStatistics
        this.firstPageStatistics = pageWriter.getStatistics();
        this.sizeWithoutStatistic = pageWriter.writePageHeaderAndDataIntoBuff(pageBuffer, true);
      } else if (numOfPages == 1) { // put the firstPageStatistics into pageBuffer
        byte[] b = pageBuffer.toByteArray();
        pageBuffer.reset();
        pageBuffer.write(b, 0, this.sizeWithoutStatistic);
        firstPageStatistics.serialize(pageBuffer);
        pageBuffer.write(b, this.sizeWithoutStatistic, b.length - this.sizeWithoutStatistic);
        if (!insertPagePositions.isEmpty()) {
          insertPagePositions.set(0, pageBuffer.size());
        }
        pageWriter.writePageHeaderAndDataIntoBuff(pageBuffer, false);
        firstPageStatistics = null;
      } else {
        pageWriter.writePageHeaderAndDataIntoBuff(pageBuffer, false);
      }

      // update statistics of this chunk
      numOfPages++;
      this.statistics.mergeStatistics(pageWriter.getStatistics());
    } catch (IOException e) {
      logger.error("meet error in pageWriter.writePageHeaderAndDataIntoBuff,ignore this page:", e);
    } finally {
      // clear start time stamp for next initializing
      pageWriter.reset();
    }
  }

  public void writeLazyPageLoaderIntoBuff(LazyPageLoader lazyPageLoader) throws PageException {
    // write the page header to pageBuffer
    PageHeader header = lazyPageLoader.getPageHeader();
    try {
      logger.debug(
          "start to flush a page header into buffer, buffer position {} ", pageBuffer.size());
      // serialize pageHeader  see writePageToPageBuffer method
      if (numOfPages == 0) { // record the firstPageStatistics
        this.firstPageStatistics = header.getStatistics();
        this.sizeWithoutStatistic +=
            ReadWriteForEncodingUtils.writeUnsignedVarInt(header.getUncompressedSize(), pageBuffer);
        this.sizeWithoutStatistic +=
            ReadWriteForEncodingUtils.writeUnsignedVarInt(header.getCompressedSize(), pageBuffer);
      } else if (numOfPages == 1) { // put the firstPageStatistics into pageBuffer
        byte[] b = pageBuffer.toByteArray();
        pageBuffer.reset();
        pageBuffer.write(b, 0, this.sizeWithoutStatistic);
        firstPageStatistics.serialize(pageBuffer);
        pageBuffer.write(b, this.sizeWithoutStatistic, b.length - this.sizeWithoutStatistic);
        if (!insertPagePositions.isEmpty()) {
          insertPagePositions.set(0, pageBuffer.size());
        }
        ReadWriteForEncodingUtils.writeUnsignedVarInt(header.getUncompressedSize(), pageBuffer);
        ReadWriteForEncodingUtils.writeUnsignedVarInt(header.getCompressedSize(), pageBuffer);
        header.getStatistics().serialize(pageBuffer);
        firstPageStatistics = null;
      } else {
        ReadWriteForEncodingUtils.writeUnsignedVarInt(header.getUncompressedSize(), pageBuffer);
        ReadWriteForEncodingUtils.writeUnsignedVarInt(header.getCompressedSize(), pageBuffer);
        header.getStatistics().serialize(pageBuffer);
      }
      logger.debug(
          "finish to flush a page header {} of time page into buffer, buffer position {} ",
          header,
          pageBuffer.size());

      statistics.mergeStatistics(header.getStatistics());

    } catch (IOException e) {
      throw new PageException("IO Exception in writeDataPageHeader,ignore this page", e);
    }
    numOfPages++;
    insertPagePositions.add(pageBuffer.size());
    pageLoaders.add(lazyPageLoader);
    sizeOfUnLoadPage += header.getCompressedSize();
  }

  @Override
  public void writeAllPagesOfChunkToTsFile(TsFileIOWriter writer) throws IOException {
    if (statistics.getCount() == 0) {
      return;
    }

    // start to write this column chunk
    writer.startFlushChunk(
        measurementId,
        compressionType,
        TSDataType.VECTOR,
        encodingType,
        statistics,
        pageBuffer.size() + (int) sizeOfUnLoadPage,
        numOfPages,
        TsFileConstant.TIME_COLUMN_MASK);

    long dataOffset = writer.getPos();

    int writedSizeOfBuffer = 0;
    int bufferSize = pageBuffer.size();
    if (insertPagePositions.isEmpty()) {
      writer.writeBytesToStream(pageBuffer);
      writedSizeOfBuffer = bufferSize;
    }
    // write all pages of this column
    while (!insertPagePositions.isEmpty()) {
      Integer size = insertPagePositions.peek();
      if (writedSizeOfBuffer < size) {
        pageBuffer.writeTo(
            writer.getIOWriterOut().wrapAsStream(), writedSizeOfBuffer, size - writedSizeOfBuffer);
        writedSizeOfBuffer = size;
      } else {
        insertPagePositions.removeFirst();
        LazyPageLoader pageLoader = pageLoaders.removeFirst();
        if (pageLoader.isEmpty()) {
          continue;
        }
        writer.getIOWriterOut().write(pageLoader.loadPage());
      }
    }
    if (writedSizeOfBuffer != bufferSize) {
      pageBuffer.writeTo(
          writer.getIOWriterOut().wrapAsStream(),
          writedSizeOfBuffer,
          bufferSize - writedSizeOfBuffer);
    }

    int dataSize = (int) (writer.getPos() - dataOffset);
    if (dataSize != pageBuffer.size() + sizeOfUnLoadPage) {
      throw new IOException(
          "Bytes written is inconsistent with the size of data: "
              + dataSize
              + " !="
              + " "
              + pageBuffer.size()
              + sizeOfUnLoadPage);
    }

    writer.endCurrentChunk();
  }

  public void writeToFileWriter(TsFileIOWriter tsfileWriter) throws IOException {
    sealCurrentPage();
    writeAllPagesOfChunkToTsFile(tsfileWriter);

    // reinit this chunk writer
    pageBuffer.reset();
    numOfPages = 0;
    sizeWithoutStatistic = 0;
    firstPageStatistics = null;
    sizeOfUnLoadPage = 0;
    this.statistics = new TimeStatistics();
  }

  @Override
  public long estimateMaxSeriesMemSize() {
    return super.estimateMaxSeriesMemSize() + sizeOfUnLoadPage;
  }
}
