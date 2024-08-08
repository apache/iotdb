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
import org.apache.iotdb.tsfile.compress.ICompressor;
import org.apache.iotdb.tsfile.encoding.encoder.SDTEncoder;
import org.apache.iotdb.tsfile.exception.write.PageException;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.apache.iotdb.tsfile.utils.ReadWriteForEncodingUtils;
import org.apache.iotdb.tsfile.write.page.PageWriter;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;

public class ChunkWriterImpl implements IChunkWriter {

  private static final Logger logger = LoggerFactory.getLogger(ChunkWriterImpl.class);

  private final IMeasurementSchema measurementSchema;

  private final ICompressor compressor;

  /** all pages of this chunk. */
  private final PublicBAOS pageBuffer;

  private int numOfPages;

  /** write data into current page */
  private PageWriter pageWriter;

  /** page size threshold. */
  private final long pageSizeThreshold;

  private final int maxNumberOfPointsInPage;

  /** value count in current page. */
  private int valueCountInOnePageForNextCheck;

  // initial value for valueCountInOnePageForNextCheck
  private static final int MINIMUM_RECORD_COUNT_FOR_CHECK = 1500;

  /** statistic of this chunk. */
  private Statistics<? extends Serializable> statistics;

  /** SDT parameters */
  private boolean isSdtEncoding;
  // When the ChunkWriter WILL write the last data point in the chunk, set it to true to tell SDT
  // saves the point.
  private boolean isLastPoint;
  // do not re-execute SDT compression when merging chunks
  private boolean isMerging;
  private SDTEncoder sdtEncoder;

  private static final String LOSS = "loss";
  private static final String SDT = "sdt";
  private static final String SDT_COMP_DEV = "compdev";
  private static final String SDT_COMP_MIN_TIME = "compmintime";
  private static final String SDT_COMP_MAX_TIME = "compmaxtime";

  /** first page info */
  private int sizeWithoutStatistic;

  private Statistics<?> firstPageStatistics;

  /** @param schema schema of this measurement */
  public ChunkWriterImpl(IMeasurementSchema schema) {
    this.measurementSchema = schema;
    this.compressor = ICompressor.getCompressor(schema.getCompressor());
    this.pageBuffer = new PublicBAOS();

    this.pageSizeThreshold = TSFileDescriptor.getInstance().getConfig().getPageSizeInByte();
    this.maxNumberOfPointsInPage =
        TSFileDescriptor.getInstance().getConfig().getMaxNumberOfPointsInPage();
    // initial check of memory usage. So that we have enough data to make an initial prediction
    this.valueCountInOnePageForNextCheck = MINIMUM_RECORD_COUNT_FOR_CHECK;

    // init statistics for this chunk and page
    this.statistics = Statistics.getStatsByType(measurementSchema.getType());

    this.pageWriter = new PageWriter(measurementSchema);

    this.pageWriter.setTimeEncoder(measurementSchema.getTimeEncoder());
    this.pageWriter.setValueEncoder(measurementSchema.getValueEncoder());

    // check if the measurement schema uses SDT
    checkSdtEncoding();
  }

  public ChunkWriterImpl(IMeasurementSchema schema, boolean isMerging) {
    this(schema);
    this.isMerging = isMerging;
  }

  private void checkSdtEncoding() {
    if (measurementSchema.getProps() != null && !isMerging) {
      if (measurementSchema.getProps().getOrDefault(LOSS, "").equals(SDT)) {
        isSdtEncoding = true;
        sdtEncoder = new SDTEncoder();
      }

      if (isSdtEncoding && measurementSchema.getProps().containsKey(SDT_COMP_DEV)) {
        sdtEncoder.setCompDeviation(
            Double.parseDouble(measurementSchema.getProps().get(SDT_COMP_DEV)));
      }

      if (isSdtEncoding && measurementSchema.getProps().containsKey(SDT_COMP_MIN_TIME)) {
        sdtEncoder.setCompMinTime(
            Long.parseLong(measurementSchema.getProps().get(SDT_COMP_MIN_TIME)));
      }

      if (isSdtEncoding && measurementSchema.getProps().containsKey(SDT_COMP_MAX_TIME)) {
        sdtEncoder.setCompMaxTime(
            Long.parseLong(measurementSchema.getProps().get(SDT_COMP_MAX_TIME)));
      }
    }
  }

  public void write(long time, long value) {
    // store last point for sdtEncoding, it still needs to go through encoding process
    // in case it exceeds compdev and needs to store second last point
    if (!isSdtEncoding || sdtEncoder.encodeLong(time, value)) {
      pageWriter.write(
          isSdtEncoding ? sdtEncoder.getTime() : time,
          isSdtEncoding ? sdtEncoder.getLongValue() : value);
    }
    if (isSdtEncoding && isLastPoint) {
      pageWriter.write(time, value);
    }
    checkPageSizeAndMayOpenANewPage();
  }

  public void write(long time, int value) {
    if (!isSdtEncoding || sdtEncoder.encodeInt(time, value)) {
      pageWriter.write(
          isSdtEncoding ? sdtEncoder.getTime() : time,
          isSdtEncoding ? sdtEncoder.getIntValue() : value);
    }
    if (isSdtEncoding && isLastPoint) {
      pageWriter.write(time, value);
    }
    checkPageSizeAndMayOpenANewPage();
  }

  public void write(long time, boolean value) {
    pageWriter.write(time, value);
    checkPageSizeAndMayOpenANewPage();
  }

  public void write(long time, float value) {
    if (!isSdtEncoding || sdtEncoder.encodeFloat(time, value)) {
      pageWriter.write(
          isSdtEncoding ? sdtEncoder.getTime() : time,
          isSdtEncoding ? sdtEncoder.getFloatValue() : value);
    }
    // store last point for sdt encoding
    if (isSdtEncoding && isLastPoint) {
      pageWriter.write(time, value);
    }
    checkPageSizeAndMayOpenANewPage();
  }

  public void write(long time, double value) {
    if (!isSdtEncoding || sdtEncoder.encodeDouble(time, value)) {
      pageWriter.write(
          isSdtEncoding ? sdtEncoder.getTime() : time,
          isSdtEncoding ? sdtEncoder.getDoubleValue() : value);
    }
    if (isSdtEncoding && isLastPoint) {
      pageWriter.write(time, value);
    }
    checkPageSizeAndMayOpenANewPage();
  }

  public void write(long time, Binary value) {
    pageWriter.write(time, value);
    checkPageSizeAndMayOpenANewPage();
  }

  public void write(long[] timestamps, int[] values, int batchSize) {
    if (isSdtEncoding) {
      batchSize = sdtEncoder.encode(timestamps, values, batchSize);
    }
    pageWriter.write(timestamps, values, batchSize);
    checkPageSizeAndMayOpenANewPage();
  }

  public void write(long[] timestamps, long[] values, int batchSize) {
    if (isSdtEncoding) {
      batchSize = sdtEncoder.encode(timestamps, values, batchSize);
    }
    pageWriter.write(timestamps, values, batchSize);
    checkPageSizeAndMayOpenANewPage();
  }

  public void write(long[] timestamps, boolean[] values, int batchSize) {
    pageWriter.write(timestamps, values, batchSize);
    checkPageSizeAndMayOpenANewPage();
  }

  public void write(long[] timestamps, float[] values, int batchSize) {
    if (isSdtEncoding) {
      batchSize = sdtEncoder.encode(timestamps, values, batchSize);
    }
    pageWriter.write(timestamps, values, batchSize);
    checkPageSizeAndMayOpenANewPage();
  }

  public void write(long[] timestamps, double[] values, int batchSize) {
    if (isSdtEncoding) {
      batchSize = sdtEncoder.encode(timestamps, values, batchSize);
    }
    pageWriter.write(timestamps, values, batchSize);
    checkPageSizeAndMayOpenANewPage();
  }

  public void write(long[] timestamps, Binary[] values, int batchSize) {
    pageWriter.write(timestamps, values, batchSize);
    checkPageSizeAndMayOpenANewPage();
  }

  /**
   * check occupied memory size, if it exceeds the PageSize threshold, construct a page and put it
   * to pageBuffer
   */
  private void checkPageSizeAndMayOpenANewPage() {
    if (pageWriter.getPointNumber() == maxNumberOfPointsInPage) {
      logger.debug("current line count reaches the upper bound, write page {}", measurementSchema);
      writePageToPageBuffer();
    } else if (pageWriter.getPointNumber()
        >= valueCountInOnePageForNextCheck) { // need to check memory size
      // not checking the memory used for every value
      long currentPageSize = pageWriter.estimateMaxMemSize();
      if (currentPageSize > pageSizeThreshold) { // memory size exceeds threshold
        // we will write the current page
        logger.debug(
            "enough size, write page {}, pageSizeThreshold:{}, currentPateSize:{}, valueCountInOnePage:{}",
            measurementSchema.getMeasurementId(),
            pageSizeThreshold,
            currentPageSize,
            pageWriter.getPointNumber());
        writePageToPageBuffer();
        valueCountInOnePageForNextCheck = MINIMUM_RECORD_COUNT_FOR_CHECK;
      } else {
        // reset the valueCountInOnePageForNextCheck for the next page
        valueCountInOnePageForNextCheck =
            (int) (((float) pageSizeThreshold / currentPageSize) * pageWriter.getPointNumber());
      }
    }
  }

  private void writePageToPageBuffer() {
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
      pageWriter.reset(measurementSchema);
    }
  }

  @Override
  public void writeToFileWriter(TsFileIOWriter tsfileWriter) throws IOException {
    sealCurrentPage();
    writeAllPagesOfChunkToTsFile(tsfileWriter, statistics);

    // reinit this chunk writer
    pageBuffer.reset();
    numOfPages = 0;
    sizeWithoutStatistic = 0;
    firstPageStatistics = null;
    this.statistics = Statistics.getStatsByType(measurementSchema.getType());
  }

  @Override
  public long estimateMaxSeriesMemSize() {
    return pageBuffer.size()
        + pageWriter.estimateMaxMemSize()
        + PageHeader.estimateMaxPageHeaderSizeWithoutStatistics()
        + pageWriter.getStatistics().getSerializedSize();
  }

  @Override
  public long getSerializedChunkSize() {
    if (pageBuffer.size() == 0) {
      return 0;
    }
    // return the serialized size of the chunk header + all pages
    return ChunkHeader.getSerializedSize(measurementSchema.getMeasurementId(), pageBuffer.size())
        + (long) pageBuffer.size();
  }

  @Override
  public void sealCurrentPage() {
    if (pageWriter != null && pageWriter.getPointNumber() > 0) {
      writePageToPageBuffer();
    }
  }

  @Override
  public void clearPageWriter() {
    pageWriter = null;
  }

  @Override
  public boolean checkIsUnsealedPageOverThreshold(
      long size, long pointNum, boolean returnTrueIfPageEmpty) {
    if (returnTrueIfPageEmpty && pageWriter.getPointNumber() == 0) {
      // return true if there is no unsealed page
      return true;
    }
    return pageWriter.getPointNumber() >= pointNum || pageWriter.estimateMaxMemSize() >= size;
  }

  @Override
  public boolean checkIsChunkSizeOverThreshold(
      long size, long pointNum, boolean returnTrueIfChunkEmpty) {
    if (returnTrueIfChunkEmpty && statistics.getCount() + pageWriter.getPointNumber() == 0) {
      // return true if there is no unsealed chunk
      return true;
    }
    return estimateMaxSeriesMemSize() >= size
        || statistics.getCount() + pageWriter.getPointNumber() >= pointNum;
  }

  public TSDataType getDataType() {
    return measurementSchema.getType();
  }

  /**
   * write the page header and data into the PageWriter's output stream. @NOTE: for upgrading
   * 0.11/v2 to 0.12/v3 TsFile
   */
  public void writePageHeaderAndDataIntoBuff(ByteBuffer data, PageHeader header)
      throws PageException {
    // write the page header to pageBuffer
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
          "finish to flush a page header {} of {} into buffer, buffer position {} ",
          header,
          measurementSchema.getMeasurementId(),
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

  /**
   * write the page to specified IOWriter.
   *
   * @param writer the specified IOWriter
   * @param statistics the chunk statistics
   * @throws IOException exception in IO
   */
  private void writeAllPagesOfChunkToTsFile(
      TsFileIOWriter writer, Statistics<? extends Serializable> statistics) throws IOException {
    if (statistics.getCount() == 0) {
      return;
    }

    // start to write this column chunk
    writer.startFlushChunk(
        measurementSchema.getMeasurementId(),
        compressor.getType(),
        measurementSchema.getType(),
        measurementSchema.getEncodingType(),
        statistics,
        pageBuffer.size(),
        numOfPages,
        0);

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

  public void setIsMerging(boolean isMerging) {
    this.isMerging = isMerging;
  }

  public boolean isMerging() {
    return isMerging;
  }

  public void setLastPoint(boolean isLastPoint) {
    this.isLastPoint = isLastPoint;
  }

  /** Only used for tests. */
  public PageWriter getPageWriter() {
    return pageWriter;
  }
}
