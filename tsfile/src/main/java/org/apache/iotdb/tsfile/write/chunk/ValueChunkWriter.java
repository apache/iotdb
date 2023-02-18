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
package org.apache.iotdb.tsfile.write.chunk;

import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.compress.ICompressor;
import org.apache.iotdb.tsfile.encoding.encoder.Encoder;
import org.apache.iotdb.tsfile.exception.write.PageException;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.apache.iotdb.tsfile.utils.ReadWriteForEncodingUtils;
import org.apache.iotdb.tsfile.write.page.ValuePageWriter;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;

public class ValueChunkWriter {

  private static final Logger logger = LoggerFactory.getLogger(ValueChunkWriter.class);

  private final String measurementId;

  private final TSEncoding encodingType;

  private final TSDataType dataType;

  private final CompressionType compressionType;

  /** all pages of this chunk. */
  private final PublicBAOS pageBuffer;

  private int numOfPages;

  /** write data into current page */
  private ValuePageWriter pageWriter;

  /** page size threshold. */
  private final long pageSizeThreshold;

  private final int maxNumberOfPointsInPage;

  /** value count in current page. */
  private int valueCountInOnePageForNextCheck;

  // initial value for valueCountInOnePageForNextCheck
  private static final int MINIMUM_RECORD_COUNT_FOR_CHECK = 1500;

  /** statistic of this chunk. */
  private Statistics<? extends Serializable> statistics;

  /** first page info */
  private int sizeWithoutStatistic;

  private Statistics<?> firstPageStatistics;

  public ValueChunkWriter(
      String measurementId,
      CompressionType compressionType,
      TSDataType dataType,
      TSEncoding encodingType,
      Encoder valueEncoder) {
    this.measurementId = measurementId;
    this.encodingType = encodingType;
    this.dataType = dataType;
    this.compressionType = compressionType;
    this.pageBuffer = new PublicBAOS();
    this.pageSizeThreshold = TSFileDescriptor.getInstance().getConfig().getPageSizeInByte();
    this.maxNumberOfPointsInPage =
        TSFileDescriptor.getInstance().getConfig().getMaxNumberOfPointsInPage();
    this.valueCountInOnePageForNextCheck = MINIMUM_RECORD_COUNT_FOR_CHECK;

    // init statistics for this chunk and page
    this.statistics = Statistics.getStatsByType(dataType);

    this.pageWriter =
        new ValuePageWriter(valueEncoder, ICompressor.getCompressor(compressionType), dataType);
  }

  public void write(long time, long value, boolean isNull) {
    pageWriter.write(time, value, isNull);
  }

  public void write(long time, int value, boolean isNull) {
    pageWriter.write(time, value, isNull);
  }

  public void write(long time, boolean value, boolean isNull) {
    pageWriter.write(time, value, isNull);
  }

  public void write(long time, float value, boolean isNull) {
    pageWriter.write(time, value, isNull);
  }

  public void write(long time, double value, boolean isNull) {
    pageWriter.write(time, value, isNull);
  }

  public void write(long time, Binary value, boolean isNull) {
    pageWriter.write(time, value, isNull);
  }

  public void write(long[] timestamps, int[] values, boolean[] isNull, int batchSize, int pos) {
    pageWriter.write(timestamps, values, isNull, batchSize, pos);
  }

  public void write(long[] timestamps, long[] values, boolean[] isNull, int batchSize, int pos) {
    pageWriter.write(timestamps, values, isNull, batchSize, pos);
  }

  public void write(long[] timestamps, boolean[] values, boolean[] isNull, int batchSize, int pos) {
    pageWriter.write(timestamps, values, isNull, batchSize, pos);
  }

  public void write(long[] timestamps, float[] values, boolean[] isNull, int batchSize, int pos) {
    pageWriter.write(timestamps, values, isNull, batchSize, pos);
  }

  public void write(long[] timestamps, double[] values, boolean[] isNull, int batchSize, int pos) {
    pageWriter.write(timestamps, values, isNull, batchSize, pos);
  }

  public void write(long[] timestamps, Binary[] values, boolean[] isNull, int batchSize, int pos) {
    pageWriter.write(timestamps, values, isNull, batchSize, pos);
  }

  public void writeEmptyPageToPageBuffer() throws IOException {
    if (numOfPages == 1 && firstPageStatistics != null) {
      // if the first page is not an empty page
      byte[] b = pageBuffer.toByteArray();
      pageBuffer.reset();
      pageBuffer.write(b, 0, this.sizeWithoutStatistic);
      firstPageStatistics.serialize(pageBuffer);
      pageBuffer.write(b, this.sizeWithoutStatistic, b.length - this.sizeWithoutStatistic);
      firstPageStatistics = null;
    }
    pageWriter.writeEmptyPageIntoBuff(pageBuffer);
    numOfPages++;
  }

  public void writePageToPageBuffer() {
    try {
      if (numOfPages == 0) {
        if (pageWriter.getStatistics().getCount() != 0) {
          // record the firstPageStatistics if it is not empty page
          this.firstPageStatistics = pageWriter.getStatistics();
        }
        this.sizeWithoutStatistic = pageWriter.writePageHeaderAndDataIntoBuff(pageBuffer, true);
      } else if (numOfPages == 1) { // put the firstPageStatistics into pageBuffer
        if (firstPageStatistics != null) { // Consider previous page is an empty page
          byte[] b = pageBuffer.toByteArray();
          pageBuffer.reset();
          pageBuffer.write(b, 0, this.sizeWithoutStatistic);
          firstPageStatistics.serialize(pageBuffer);
          pageBuffer.write(b, this.sizeWithoutStatistic, b.length - this.sizeWithoutStatistic);
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
      pageWriter.reset(dataType);
    }
  }

  public void writePageHeaderAndDataIntoBuff(ByteBuffer data, PageHeader header)
      throws PageException {
    // write the page header to pageBuffer
    try {
      logger.debug(
          "start to flush a page header into buffer, buffer position {} ", pageBuffer.size());
      // serialize pageHeader  see writePageToPageBuffer method
      if (numOfPages == 0) { // record the firstPageStatistics
        if (header.getStatistics() != null) {
          this.firstPageStatistics = header.getStatistics();
        }
        this.sizeWithoutStatistic +=
            ReadWriteForEncodingUtils.writeUnsignedVarInt(header.getUncompressedSize(), pageBuffer);
        this.sizeWithoutStatistic +=
            ReadWriteForEncodingUtils.writeUnsignedVarInt(header.getCompressedSize(), pageBuffer);
      } else if (numOfPages == 1) { // put the firstPageStatistics into pageBuffer
        if (firstPageStatistics != null) {
          byte[] b = pageBuffer.toByteArray();
          pageBuffer.reset();
          pageBuffer.write(b, 0, this.sizeWithoutStatistic);
          firstPageStatistics.serialize(pageBuffer);
          pageBuffer.write(b, this.sizeWithoutStatistic, b.length - this.sizeWithoutStatistic);
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
    // write page content to temp PBAOS
    try (WritableByteChannel channel = Channels.newChannel(pageBuffer)) {
      channel.write(data);
    } catch (IOException e) {
      throw new PageException(e);
    }
  }

  public void writeToFileWriter(TsFileIOWriter tsfileWriter) throws IOException {
    sealCurrentPage();
    writeAllPagesOfChunkToTsFile(tsfileWriter);

    // reinit this chunk writer
    pageBuffer.reset();
    numOfPages = 0;
    sizeWithoutStatistic = 0;
    firstPageStatistics = null;
    this.statistics = Statistics.getStatsByType(dataType);
  }

  public long estimateMaxSeriesMemSize() {
    return pageBuffer.size()
        + pageWriter.estimateMaxMemSize()
        + PageHeader.estimateMaxPageHeaderSizeWithoutStatistics()
        + pageWriter.getStatistics().getSerializedSize();
  }

  public long getCurrentChunkSize() {
    /**
     * It may happen if subsequent write operations are all out of order, then count of statistics
     * in this chunk will be 0 and this chunk will not be flushed.
     */
    if (pageBuffer.size() == 0) {
      return 0;
    }

    // Empty chunk, it may happen if pageBuffer stores empty bits and only chunk header will be
    // flushed.
    if (statistics.getCount() == 0) {
      return ChunkHeader.getSerializedSize(measurementId, 0);
    }

    // return the serialized size of the chunk header + all pages
    return ChunkHeader.getSerializedSize(measurementId, pageBuffer.size())
        + (long) pageBuffer.size();
  }

  public boolean checkPageSizeAndMayOpenANewPage() {
    if (pageWriter.getPointNumber() == maxNumberOfPointsInPage) {
      logger.debug("current line count reaches the upper bound, write page {}", measurementId);
      return true;
    } else if (pageWriter.getPointNumber()
        >= valueCountInOnePageForNextCheck) { // need to check memory size
      // not checking the memory used for every value
      long currentPageSize = pageWriter.estimateMaxMemSize();
      if (currentPageSize > pageSizeThreshold) { // memory size exceeds threshold
        // we will write the current page
        logger.debug(
            "enough size, write page {}, pageSizeThreshold:{}, currentPageSize:{}, valueCountInOnePage:{}",
            measurementId,
            pageSizeThreshold,
            currentPageSize,
            pageWriter.getPointNumber());
        valueCountInOnePageForNextCheck = MINIMUM_RECORD_COUNT_FOR_CHECK;
        return true;
      } else {
        // reset the valueCountInOnePageForNextCheck for the next page
        valueCountInOnePageForNextCheck =
            (int) (((float) pageSizeThreshold / currentPageSize) * pageWriter.getPointNumber());
      }
    }
    return false;
  }

  public void sealCurrentPage() {
    // if the page contains no points, we still need to serialize it
    if (pageWriter != null && pageWriter.getSize() != 0) {
      writePageToPageBuffer();
    }
  }

  public void clearPageWriter() {
    pageWriter = null;
  }

  public int getNumOfPages() {
    return numOfPages;
  }

  public TSDataType getDataType() {
    return dataType;
  }

  /**
   * write the page to specified IOWriter.
   *
   * @param writer the specified IOWriter
   * @throws IOException exception in IO
   */
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
        pageBuffer.size(),
        numOfPages,
        TsFileConstant.VALUE_COLUMN_MASK);

    long dataOffset = writer.getPos();

    // write all pages of this column
    writer.writeBytesToStream(pageBuffer);

    int dataSize = (int) (writer.getPos() - dataOffset);
    if (dataSize != pageBuffer.size()) {
      throw new IOException(
          "Bytes written is inconsistent with the size of data: "
              + dataSize
              + " !="
              + " "
              + pageBuffer.size());
    }

    writer.endCurrentChunk();
  }

  public String getMeasurementId() {
    return measurementId;
  }

  public TSEncoding getEncodingType() {
    return encodingType;
  }

  public CompressionType getCompressionType() {
    return compressionType;
  }

  public Statistics<? extends Serializable> getStatistics() {
    return statistics;
  }

  /** only used for test */
  public PublicBAOS getPageBuffer() {
    return pageBuffer;
  }

  public boolean checkIsUnsealedPageOverThreshold(long size) {
    return pageWriter.estimateMaxMemSize() >= size;
  }

  public ValuePageWriter getPageWriter() {
    return pageWriter;
  }
}
