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
import org.apache.iotdb.tsfile.exception.write.PageException;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.ReadWriteForEncodingUtils;
import org.apache.iotdb.tsfile.write.chunk.ValueChunkWriter;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;

public class LazyValueChunkWriter extends ValueChunkWriter {

  private LinkedList<Integer> insertPagePositions = new LinkedList<>();
  private LinkedList<LazyPageLoader> pageLoaders = new LinkedList<>();

  private long sizeOfUnLoadPage = 0;

  public LazyValueChunkWriter(
      String measurementId,
      CompressionType compressionType,
      TSDataType dataType,
      TSEncoding encodingType,
      Encoder valueEncoder) {
    super(measurementId, compressionType, dataType, encodingType, valueEncoder);
  }

  @Override
  public void sealCurrentPage() {
    super.sealCurrentPage();
  }

  @Override
  public void writePageHeaderAndDataIntoBuff(ByteBuffer data, PageHeader header)
      throws PageException {
    super.writePageHeaderAndDataIntoBuff(data, header);
  }

  public void writeLazyPageLoaderIntoBuff(LazyPageLoader lazyPageLoader) throws PageException {
    PageHeader header = lazyPageLoader.getPageHeader();
    // write the page header to pageBuffer
    try {
      logger.debug(
          "start to flush a page header into buffer, buffer position {} ", pageBuffer.size());
      // serialize pageHeader  see writePageToPageBuffer method
      if (numOfPages == 0) { // record the firstPageStatistics

        this.sizeWithoutStatistic +=
            ReadWriteForEncodingUtils.writeUnsignedVarInt(header.getUncompressedSize(), pageBuffer);

        if (header.getStatistics() == null) {
          this.firstPageStatistics = null;
        } else {
          this.firstPageStatistics = header.getStatistics();
          this.sizeWithoutStatistic +=
              ReadWriteForEncodingUtils.writeUnsignedVarInt(header.getCompressedSize(), pageBuffer);
        }
      } else if (numOfPages == 1) { // put the firstPageStatistics into pageBuffer
        if (firstPageStatistics != null) {
          byte[] b = pageBuffer.toByteArray();
          pageBuffer.reset();
          pageBuffer.write(b, 0, this.sizeWithoutStatistic);
          firstPageStatistics.serialize(pageBuffer);
          pageBuffer.write(b, this.sizeWithoutStatistic, b.length - this.sizeWithoutStatistic);
        }
        if (!insertPagePositions.isEmpty()) {
          insertPagePositions.set(0, pageBuffer.size());
        }
        ReadWriteForEncodingUtils.writeUnsignedVarInt(header.getUncompressedSize(), pageBuffer);
        if (header.getUncompressedSize() != 0) {
          ReadWriteForEncodingUtils.writeUnsignedVarInt(header.getCompressedSize(), pageBuffer);
          header.getStatistics().serialize(pageBuffer);
        }

        firstPageStatistics = null;
      } else {
        ReadWriteForEncodingUtils.writeUnsignedVarInt(header.getUncompressedSize(), pageBuffer);
        if (header.getUncompressedSize() != 0) {
          ReadWriteForEncodingUtils.writeUnsignedVarInt(header.getCompressedSize(), pageBuffer);
          header.getStatistics().serialize(pageBuffer);
        }
      }
      logger.debug(
          "finish to flush a page header {} of time page into buffer, buffer position {} ",
          header,
          pageBuffer.size());

      if (header.getStatistics() != null) {
        statistics.mergeStatistics(header.getStatistics());
      }
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
      if (pageBuffer.size() == 0) {
        return;
      }
      // In order to ensure that different chunkgroups in a tsfile have the same chunks or if all
      // data of this timeseries has been deleted, it is possible to have an empty valueChunk in a
      // chunkGroup during compaction. To save the disk space, we only serialize chunkHeader for the
      // empty valueChunk, whose dataSize is 0.
      writer.startFlushChunk(
          measurementId,
          compressionType,
          dataType,
          encodingType,
          statistics,
          0,
          0,
          TsFileConstant.VALUE_COLUMN_MASK);
      writer.endCurrentChunk();
      return;
    }

    // start to write this column chunk
    writer.startFlushChunk(
        measurementId,
        compressionType,
        dataType,
        encodingType,
        statistics,
        pageBuffer.size() + (int) sizeOfUnLoadPage,
        numOfPages,
        TsFileConstant.VALUE_COLUMN_MASK);

    long dataOffset = writer.getPos();

    if (insertPagePositions.isEmpty()) {
      writer.writeBytesToStream(pageBuffer);
    }
    // write all pages of this column
    int writedSizeOfBuffer = 0;
    int bufferSize = pageBuffer.size();
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
              + pageBuffer.size());
    }
    writer.endCurrentChunk();
  }

  @Override
  public long estimateMaxSeriesMemSize() {
    return super.estimateMaxSeriesMemSize() + sizeOfUnLoadPage;
  }
}
