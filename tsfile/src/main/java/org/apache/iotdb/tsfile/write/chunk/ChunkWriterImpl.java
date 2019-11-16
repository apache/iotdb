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

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.compress.ICompressor;
import org.apache.iotdb.tsfile.exception.write.PageException;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.apache.iotdb.tsfile.write.page.PageWriter;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A implementation of {@code IChunkWriter}. {@code ChunkWriterImpl} consists of a {@code
 * ChunkBuffer}, a {@code PageWriter}, and two {@code Statistics}.
 *
 * @see IChunkWriter IChunkWriter
 */
public class ChunkWriterImpl implements IChunkWriter {

  private static final Logger LOG = LoggerFactory.getLogger(ChunkWriterImpl.class);

  private MeasurementSchema measurementSchema;

  /**
   * help to encode data of this series.
   */
  //private final ChunkBuffer chunkBuffer;
  private ICompressor compressor;
  /**
   * all pages of this column.
   */
  private PublicBAOS pageBuffer;

  private long totalValueCount;
  private long currentPageMaxTimestamp;
  private long currentPageMinTimestamp = Long.MIN_VALUE;
  private ByteBuffer compressedData;// DirectByteBuffer

  private int numOfPages;
  /**
   * value writer to encode data.
   */
  private PageWriter pageWriter;

  /**
   * page size threshold.
   */
  private final long pageSizeThreshold;

  private final int maxNumberOfPointsInPage;

  // initial value for this.valueCountInOnePageForNextCheck
  private static final int MINIMUM_RECORD_COUNT_FOR_CHECK = 1500;

  /**
   * value count in a page. It will be reset after calling {@code writePageHeaderAndDataIntoBuff()}
   */
  private int valueCountInOnePage;
  private int valueCountInOnePageForNextCheck;

  /**
   * statistic on a stage. It will be reset after calling {@code writeAllPagesOfSeriesToTsFile()}
   */
  private Statistics<?> chunkStatistics;

  /**
   * statistic on a page. It will be reset after calling {@code writePageHeaderAndDataIntoBuff()}
   */
  private Statistics<?> pageStatistics;

  // time of the latest written time value pair, we assume data is written in time order
  private long maxTimestamp;
  private long minTimestamp = Long.MIN_VALUE;

  /**
   * @param schema schema of this measurement
   */
  public ChunkWriterImpl(MeasurementSchema schema) {
    this.measurementSchema = schema;
    this.compressor = ICompressor.getCompressor(schema.getCompressor());
    this.pageBuffer = new PublicBAOS();

    this.pageSizeThreshold = TSFileDescriptor.getInstance().getConfig().getPageSizeInByte();
    this.maxNumberOfPointsInPage = TSFileDescriptor.getInstance().getConfig().getMaxNumberOfPointsInPage();
    // initial check of memory usage. So that we have enough data to make an initial prediction
    this.valueCountInOnePageForNextCheck = MINIMUM_RECORD_COUNT_FOR_CHECK;

    // init statistics for this series and page
    this.chunkStatistics = Statistics.getStatsByType(measurementSchema.getType());
    this.pageStatistics = Statistics.getStatsByType(measurementSchema.getType());

    this.pageWriter = new PageWriter();

    this.pageWriter.setTimeEncoder(measurementSchema.getTimeEncoder());
    this.pageWriter.setValueEncoder(measurementSchema.getValueEncoder());
  }

  @Override
  public void write(long time, long value) {
    this.maxTimestamp = time;
    ++valueCountInOnePage;
    pageWriter.write(time, value);
    pageStatistics.updateStats(value);
    if (minTimestamp == Long.MIN_VALUE) {
      minTimestamp = time;
    }
    checkPageSizeAndMayOpenANewPage();
  }

  @Override
  public void write(long time, int value) {
    this.maxTimestamp = time;
    ++valueCountInOnePage;
    pageWriter.write(time, value);
    pageStatistics.updateStats(value);
    if (minTimestamp == Long.MIN_VALUE) {
      minTimestamp = time;
    }
    checkPageSizeAndMayOpenANewPage();
  }

  @Override
  public void write(long time, boolean value) {
    this.maxTimestamp = time;
    ++valueCountInOnePage;
    pageWriter.write(time, value);
    pageStatistics.updateStats(value);
    if (minTimestamp == Long.MIN_VALUE) {
      minTimestamp = time;
    }
    checkPageSizeAndMayOpenANewPage();
  }

  @Override
  public void write(long time, float value) {
    this.maxTimestamp = time;
    ++valueCountInOnePage;
    pageWriter.write(time, value);
    pageStatistics.updateStats(value);
    if (minTimestamp == Long.MIN_VALUE) {
      minTimestamp = time;
    }
    checkPageSizeAndMayOpenANewPage();
  }

  @Override
  public void write(long time, double value) {
    this.maxTimestamp = time;
    ++valueCountInOnePage;
    pageWriter.write(time, value);
    pageStatistics.updateStats(value);
    if (minTimestamp == Long.MIN_VALUE) {
      minTimestamp = time;
    }
    checkPageSizeAndMayOpenANewPage();
  }

  @Override
  public void write(long time, BigDecimal value) {
    this.maxTimestamp = time;
    ++valueCountInOnePage;
    pageWriter.write(time, value);
    pageStatistics.updateStats(value);
    if (minTimestamp == Long.MIN_VALUE) {
      minTimestamp = time;
    }
    checkPageSizeAndMayOpenANewPage();
  }

  @Override
  public void write(long time, Binary value) {
    this.maxTimestamp = time;
    ++valueCountInOnePage;
    pageWriter.write(time, value);
    pageStatistics.updateStats(value);
    if (minTimestamp == Long.MIN_VALUE) {
      minTimestamp = time;
    }
    checkPageSizeAndMayOpenANewPage();
  }

  @Override
  public void write(long[] timestamps, int[] values, int batchSize) {
    this.maxTimestamp = timestamps[batchSize - 1];
    valueCountInOnePage += batchSize;
    if (minTimestamp == Long.MIN_VALUE) {
      minTimestamp = timestamps[0];
    }
    pageWriter.write(timestamps, values, batchSize);
    pageStatistics.updateStats(values);
    checkPageSizeAndMayOpenANewPage();
  }

  @Override
  public void write(long[] timestamps, long[] values, int batchSize) {
    this.maxTimestamp = timestamps[batchSize - 1];
    valueCountInOnePage += batchSize;
    if (minTimestamp == Long.MIN_VALUE) {
      minTimestamp = timestamps[0];
    }
    pageWriter.write(timestamps, values, batchSize);
    pageStatistics.updateStats(values);
    checkPageSizeAndMayOpenANewPage();
  }

  @Override
  public void write(long[] timestamps, boolean[] values, int batchSize) {
    this.maxTimestamp = timestamps[batchSize - 1];
    valueCountInOnePage += batchSize;
    if (minTimestamp == Long.MIN_VALUE) {
      minTimestamp = timestamps[0];
    }
    pageWriter.write(timestamps, values, batchSize);
    pageStatistics.updateStats(values);
    checkPageSizeAndMayOpenANewPage();
  }

  @Override
  public void write(long[] timestamps, float[] values, int batchSize) {
    this.maxTimestamp = timestamps[batchSize - 1];
    valueCountInOnePage += batchSize;
    if (minTimestamp == Long.MIN_VALUE) {
      minTimestamp = timestamps[0];
    }
    pageWriter.write(timestamps, values, batchSize);
    pageStatistics.updateStats(values);
    checkPageSizeAndMayOpenANewPage();
  }

  @Override
  public void write(long[] timestamps, double[] values, int batchSize) {
    this.maxTimestamp = timestamps[batchSize - 1];
    valueCountInOnePage += batchSize;
    if (minTimestamp == Long.MIN_VALUE) {
      minTimestamp = timestamps[0];
    }
    pageWriter.write(timestamps, values, batchSize);
    pageStatistics.updateStats(values);
    checkPageSizeAndMayOpenANewPage();
  }

  @Override
  public void write(long[] timestamps, BigDecimal[] values, int batchSize) {
    this.maxTimestamp = timestamps[batchSize - 1];
    valueCountInOnePage += batchSize;
    if (minTimestamp == Long.MIN_VALUE) {
      minTimestamp = timestamps[0];
    }
    pageWriter.write(timestamps, values, batchSize);
    pageStatistics.updateStats(values);
    checkPageSizeAndMayOpenANewPage();
  }

  @Override
  public void write(long[] timestamps, Binary[] values, int batchSize) {
    this.maxTimestamp = timestamps[batchSize - 1];
    valueCountInOnePage += batchSize;
    if (minTimestamp == Long.MIN_VALUE) {
      minTimestamp = timestamps[0];
    }
    pageWriter.write(timestamps, values, batchSize);
    pageStatistics.updateStats(values);
    checkPageSizeAndMayOpenANewPage();
  }

  /**
   * check occupied memory size, if it exceeds the PageSize threshold, flush them to given
   * OutputStream.
   */
  private void checkPageSizeAndMayOpenANewPage() {
    if (valueCountInOnePage == maxNumberOfPointsInPage) {
      LOG.debug("current line count reaches the upper bound, write page {}", measurementSchema);
      writePage();
    } else if (valueCountInOnePage >= valueCountInOnePageForNextCheck) { // need to check memory size
      // not checking the memory used for every value
      long currentPageSize = pageWriter.estimateMaxMemSize();
      if (currentPageSize > pageSizeThreshold) { // memory size exceeds threshold
        // we will write the current page
        LOG.debug(
            "enough size, write page {}, pageSizeThreshold:{}, currentPateSize:{}, valueCountInOnePage:{}",
            measurementSchema.getMeasurementId(), pageSizeThreshold, currentPageSize, valueCountInOnePage);
        writePage();
        valueCountInOnePageForNextCheck = MINIMUM_RECORD_COUNT_FOR_CHECK;
      } else {
        // reset the valueCountInOnePageForNextCheck for the next page
        valueCountInOnePageForNextCheck = (int) (((float) pageSizeThreshold / currentPageSize)
            * valueCountInOnePage);
      }
    }
  }

  /**
   * flush data into {@code IChunkWriter}.
   */
  private void writePage() {
    try {
      this.writePageHeaderAndDataIntoBuff(pageWriter.getUncompressedBytes(),
          valueCountInOnePage, pageStatistics, maxTimestamp, minTimestamp);

      // update statistics of this series
      this.chunkStatistics.mergeStatistics(this.pageStatistics);
    } catch (IOException e) {
      LOG.error("meet error in pageWriter.getUncompressedBytes(),ignore this page:", e);
    } catch (PageException e) {
      LOG.error(
          "meet error in chunkBuffer.writePageHeaderAndDataIntoBuff, ignore this page:", e);
    } finally {
      // clear start time stamp for next initializing
      minTimestamp = Long.MIN_VALUE;
      valueCountInOnePage = 0;
      pageWriter.reset();
      this.pageStatistics = Statistics.getStatsByType(measurementSchema.getType());
    }
  }

  @Override
  public void writeToFileWriter(TsFileIOWriter tsfileWriter) throws IOException {
    sealCurrentPage();
    this.writeAllPagesOfSeriesToTsFile(tsfileWriter, chunkStatistics);
    this.reset();
    // reset series_statistics
    this.chunkStatistics = Statistics.getStatsByType(measurementSchema.getType());
  }

  @Override
  public long estimateMaxSeriesMemSize() {
    return pageWriter.estimateMaxMemSize() + this.estimateMaxPageMemSize();
  }

  @Override
  public long getCurrentChunkSize() {
    // return the serialized size of the chunk header + all pages
    return ChunkHeader.getSerializedSize(measurementSchema.getMeasurementId()) + this
        .getCurrentDataSize();
  }

  @Override
  public void sealCurrentPage() {
    if (valueCountInOnePage > 0) {
      writePage();
    }
  }

  @Override
  public int getNumOfPages() {
    return numOfPages;
  }


  @Override
  public TSDataType getDataType() {
    return measurementSchema.getType();
  }

  /**
   * write the page header and data into the PageWriter's output stream.
   *
   * @param data the data of the page
   * @param valueCount - the amount of values in that page
   * @param statistics - page statistics
   * @param maxTimestamp - timestamp maximum in given data
   * @param minTimestamp - timestamp minimum in given data
   * @return byte size of the page header and uncompressed data in the page body.
   */
  public int writePageHeaderAndDataIntoBuff(ByteBuffer data, int valueCount,
      Statistics<?> statistics, long maxTimestamp, long minTimestamp) throws PageException {
    numOfPages++;

    // 1. update time statistics
    if (this.currentPageMinTimestamp == Long.MIN_VALUE) {
      this.currentPageMinTimestamp = minTimestamp;
    }
    if (this.currentPageMinTimestamp == Long.MIN_VALUE) {
      throw new PageException("No valid data point in this page");
    }
    this.currentPageMaxTimestamp = maxTimestamp;
    int uncompressedSize = data.remaining();
    int compressedSize;
    int compressedPosition = 0;
    byte[] compressedBytes = null;

    if (compressor.getType().equals(CompressionType.UNCOMPRESSED)) {
      compressedSize = data.remaining();
    } else {
      compressedBytes = new byte[compressor.getMaxBytesForCompression(uncompressedSize)];
      try {
        compressedPosition = 0;
        // data is never a directByteBuffer now, so we can use data.array()
        compressedSize = compressor
            .compress(data.array(), data.position(), data.remaining(), compressedBytes);
      } catch (IOException e) {
        throw new PageException(e);
      }
    }

    int headerSize;

    // write the page header to IOWriter
    try {
      PageHeader header = new PageHeader(uncompressedSize, compressedSize, valueCount, statistics,
          maxTimestamp, minTimestamp);
      headerSize = header.getSerializedSize();
      LOG.debug("start to flush a page header into buffer, buffer position {} ", pageBuffer.size());
      header.serializeTo(pageBuffer);
      LOG.debug("finish to flush a page header {} of {} into buffer, buffer position {} ", header,
          measurementSchema.getMeasurementId(), pageBuffer.size());

    } catch (IOException e) {
      resetTimeStamp();
      throw new PageException(
          "IO Exception in writeDataPageHeader,ignore this page", e);
    }

    // update data point num
    this.totalValueCount += valueCount;

    // write page content to temp PBAOS
    try (WritableByteChannel channel = Channels.newChannel(pageBuffer)) {
      LOG.debug("start to flush a page data into buffer, buffer position {} ", pageBuffer.size());
      if (compressor.getType().equals(CompressionType.UNCOMPRESSED)) {
        channel.write(data);
      } else {
        if (data.isDirect()) {
          channel.write(compressedData);
        } else {
          pageBuffer.write(compressedBytes, compressedPosition, compressedSize);
        }
      }
      LOG.debug("start to flush a page data into buffer, buffer position {} ", pageBuffer.size());
    } catch (IOException e) {
      throw new PageException(e);
    }
    return headerSize + uncompressedSize;
  }

  /**
   * write the page header and data into the PageWriter's output stream.
   */
  public void writePageHeaderAndDataIntoBuff(ByteBuffer data, PageHeader header)
      throws PageException {
    numOfPages++;

    // 1. update time statistics
    if (this.currentPageMinTimestamp == Long.MIN_VALUE) {
      this.currentPageMinTimestamp = header.getMinTimestamp();
    }
    if (this.currentPageMinTimestamp == Long.MIN_VALUE) {
      throw new PageException("No valid data point in this page");
    }
    this.currentPageMaxTimestamp = header.getMaxTimestamp();

    // write the page header to pageBuffer
    try {
      LOG.debug("start to flush a page header into buffer, buffer position {} ", pageBuffer.size());
      header.serializeTo(pageBuffer);
      LOG.debug("finish to flush a page header {} of {} into buffer, buffer position {} ", header,
          measurementSchema.getMeasurementId(), pageBuffer.size());

    } catch (IOException e) {
      resetTimeStamp();
      throw new PageException(
          "IO Exception in writeDataPageHeader,ignore this page", e);
    }

    // update data point num
    this.totalValueCount += header.getNumOfValues();

    // write page content to temp PBAOS
    try (WritableByteChannel channel = Channels.newChannel(pageBuffer)) {
      channel.write(data);
    } catch (IOException e) {
      throw new PageException(e);
    }
  }

  private void resetTimeStamp() {
    if (totalValueCount == 0) {
      currentPageMinTimestamp = Long.MIN_VALUE;
    }
  }

  /**
   * write the page to specified IOWriter.
   *
   * @param writer the specified IOWriter
   * @param statistics the statistic information provided by series writer
   * @return the data size of this chunk
   * @throws IOException exception in IO
   */
  public long writeAllPagesOfSeriesToTsFile(TsFileIOWriter writer, Statistics<?> statistics)
      throws IOException {
    if (totalValueCount == 0) {
      return 0;
    }

    // start to write this column chunk
    int headerSize = writer.startFlushChunk(measurementSchema, compressor.getType(), measurementSchema.getType(),
        measurementSchema.getEncodingType(), statistics, currentPageMaxTimestamp, currentPageMinTimestamp, pageBuffer.size(),
        numOfPages);

    long dataOffset = writer.getPos();
    LOG.debug("start writing pages of {} into file, position {}", measurementSchema.getMeasurementId(),
        writer.getPos());

    // write all pages of this column
    writer.writeBytesToStream(pageBuffer);
    LOG.debug("finish writing pages of {} into file, position {}", measurementSchema.getMeasurementId(),
        writer.getPos());

    long dataSize = writer.getPos() - dataOffset;
    if (dataSize != pageBuffer.size()) {
      throw new IOException(
          "Bytes written is inconsistent with the size of data: " + dataSize + " !="
              + " " + pageBuffer.size());
    }

    writer.endChunk(totalValueCount);
    return headerSize + dataSize;
  }

  /**
   * reset exist data in page for next stage.
   */
  public void reset() {
    currentPageMinTimestamp = Long.MIN_VALUE;
    pageBuffer.reset();
    totalValueCount = 0;
  }

  /**
   * reset exist data in page for next stage.
   */
  public void reInit(MeasurementSchema schema) {
    reset();
    this.measurementSchema = schema;
    this.compressor = ICompressor.getCompressor(schema.getCompressor());
    numOfPages = 0;
    currentPageMaxTimestamp = 0;
  }

  /**
   * estimate max page memory size.
   *
   * @return the max possible allocated size currently
   */
  public long estimateMaxPageMemSize() {
    // return the sum of size of buffer and page max size
    return (long) (pageBuffer.size() + estimateMaxPageHeaderSize());
  }

  private int estimateMaxPageHeaderSize() {
    return PageHeader.calculatePageHeaderSize(measurementSchema.getType());
  }

  /**
   * get current data size.
   *
   * @return current data size that the writer has serialized.
   */
  public long getCurrentDataSize() {
    return pageBuffer.size();
  }
}
