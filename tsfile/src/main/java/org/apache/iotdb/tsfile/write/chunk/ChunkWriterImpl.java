/**
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
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.exception.write.PageException;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.write.page.PageWriter;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A implementation of {@code IChunkWriter}. {@code ChunkWriterImpl} consists of a {@code
 * ChunkBuffer}, a {@code PageWriter}, and two {@code Statistics}.
 *
 * @author kangrong
 * @see IChunkWriter IChunkWriter
 */
public class ChunkWriterImpl implements IChunkWriter {

  private static final Logger LOG = LoggerFactory.getLogger(ChunkWriterImpl.class);

  // initial value for this.valueCountInOnePageForNextCheck
  private static final int MINIMUM_RECORD_COUNT_FOR_CHECK = 1500;

  private final TSDataType dataType;
  /**
   * help to encode data of this series.
   */
  private final ChunkBuffer chunkBuffer;
  /**
   * page size threshold.
   */
  private final long psThres;
  private final int pageCountUpperBound;
  /**
   * value writer to encode data.
   */
  private PageWriter dataPageWriter;

  /**
   * value count in a page. It will be reset after calling {@code writePageHeaderAndDataIntoBuff()}
   */
  private int valueCountInOnePage;
  private int valueCountInOnePageForNextCheck;
  /**
   * statistic on a page. It will be reset after calling {@code writePageHeaderAndDataIntoBuff()}
   */
  private Statistics<?> pageStatistics;
  /**
   * statistic on a stage. It will be reset after calling {@code writeAllPagesOfSeriesToTsFile()}
   */
  private Statistics<?> chunkStatistics;
  // time of the latest written time value pair
  private long time;
  private long minTimestamp = -1;

  private MeasurementSchema measurementSchema;

  /**
   * constructor of ChunkWriterImpl.
   *
   * @param measurementSchema measurement schema
   * @param chunkBuffer chunk in buffer
   * @param pageSizeThreshold page size threshold
   */
  public ChunkWriterImpl(MeasurementSchema measurementSchema, ChunkBuffer chunkBuffer,
      int pageSizeThreshold) {
    this.measurementSchema = measurementSchema;
    this.dataType = measurementSchema.getType();
    this.chunkBuffer = chunkBuffer;
    this.psThres = pageSizeThreshold;

    // initial check of memory usage. So that we have enough data to make an initial prediction
    this.valueCountInOnePageForNextCheck = MINIMUM_RECORD_COUNT_FOR_CHECK;

    // init statistics for this series and page
    this.chunkStatistics = Statistics.getStatsByType(dataType);
    resetPageStatistics();

    this.dataPageWriter = new PageWriter();
    this.pageCountUpperBound = TSFileDescriptor.getInstance().getConfig().maxNumberOfPointsInPage;

    this.dataPageWriter.setTimeEncoder(measurementSchema.getTimeEncoder());
    this.dataPageWriter.setValueEncoder(measurementSchema.getValueEncoder());
  }

  /**
   * reset statistics of page by dataType of this measurement.
   */
  private void resetPageStatistics() {
    this.pageStatistics = Statistics.getStatsByType(dataType);
  }

  @Override
  public void write(long time, long value) throws IOException {
    this.time = time;
    ++valueCountInOnePage;
    dataPageWriter.write(time, value);
    pageStatistics.updateStats(value);
    if (minTimestamp == -1) {
      minTimestamp = time;
    }
    checkPageSizeAndMayOpenANewPage();
  }

  @Override
  public void write(long time, int value) throws IOException {
    this.time = time;
    ++valueCountInOnePage;
    dataPageWriter.write(time, value);
    pageStatistics.updateStats(value);
    if (minTimestamp == -1) {
      minTimestamp = time;
    }
    checkPageSizeAndMayOpenANewPage();
  }

  @Override
  public void write(long time, boolean value) throws IOException {
    this.time = time;
    ++valueCountInOnePage;
    dataPageWriter.write(time, value);
    pageStatistics.updateStats(value);
    if (minTimestamp == -1) {
      minTimestamp = time;
    }
    checkPageSizeAndMayOpenANewPage();
  }

  @Override
  public void write(long time, float value) throws IOException {
    this.time = time;
    ++valueCountInOnePage;
    dataPageWriter.write(time, value);
    pageStatistics.updateStats(value);
    if (minTimestamp == -1) {
      minTimestamp = time;
    }
    checkPageSizeAndMayOpenANewPage();
  }

  @Override
  public void write(long time, double value) throws IOException {
    this.time = time;
    ++valueCountInOnePage;
    dataPageWriter.write(time, value);
    pageStatistics.updateStats(value);
    if (minTimestamp == -1) {
      minTimestamp = time;
    }
    checkPageSizeAndMayOpenANewPage();
  }

  @Override
  public void write(long time, BigDecimal value) throws IOException {
    this.time = time;
    ++valueCountInOnePage;
    dataPageWriter.write(time, value);
    pageStatistics.updateStats(value);
    if (minTimestamp == -1) {
      minTimestamp = time;
    }
    checkPageSizeAndMayOpenANewPage();
  }

  @Override
  public void write(long time, Binary value) throws IOException {
    this.time = time;
    ++valueCountInOnePage;
    dataPageWriter.write(time, value);
    pageStatistics.updateStats(value);
    if (minTimestamp == -1) {
      minTimestamp = time;
    }
    checkPageSizeAndMayOpenANewPage();
  }

  /**
   * check occupied memory size, if it exceeds the PageSize threshold, flush them to given
   * OutputStream.
   */
  private void checkPageSizeAndMayOpenANewPage() {
    if (valueCountInOnePage == pageCountUpperBound) {
      LOG.debug("current line count reaches the upper bound, write page {}", measurementSchema);
      writePage();
    } else if (valueCountInOnePage
        >= valueCountInOnePageForNextCheck) { // need to check memory size
      // not checking the memory used for every value
      long currentColumnSize = dataPageWriter.estimateMaxMemSize();
      if (currentColumnSize > psThres) { // memory size exceeds threshold
        // we will write the current page
        LOG.debug(
            "enough size, write page {}, psThres:{}, currentColumnSize:{}, valueCountInOnePage:{}",
            measurementSchema.getMeasurementId(), psThres, currentColumnSize, valueCountInOnePage);
        writePage();
        valueCountInOnePageForNextCheck = MINIMUM_RECORD_COUNT_FOR_CHECK;
      } else {
        // reset the valueCountInOnePageForNextCheck for the next page
        valueCountInOnePageForNextCheck = (int) (((float) psThres / currentColumnSize)
            * valueCountInOnePage);
        LOG.debug(
            "not enough size. {}, psThres:{}, currentColumnSize:{},  now valueCountInOnePage: {}, "
                + "change to {}",
            measurementSchema.getMeasurementId(), psThres, currentColumnSize, valueCountInOnePage,
            valueCountInOnePageForNextCheck);
      }

    }
  }

  /**
   * flush data into {@code IChunkWriter}.
   */
  private void writePage() {
    try {
      chunkBuffer.writePageHeaderAndDataIntoBuff(dataPageWriter.getUncompressedBytes(),
          valueCountInOnePage,
          pageStatistics, time, minTimestamp);

      // update statistics of this series
      this.chunkStatistics.mergeStatistics(this.pageStatistics);
    } catch (IOException e) {
      LOG.error("meet error in dataPageWriter.getUncompressedBytes(),ignore this page, {}",
          e.getMessage());
    } catch (PageException e) {
      LOG.error(
          "meet error in chunkBuffer.writePageHeaderAndDataIntoBuff, ignore this page, "
              + "error message:{}",
          e.getMessage());
    } finally {
      // clear start time stamp for next initializing
      minTimestamp = -1;
      valueCountInOnePage = 0;
      dataPageWriter.reset();
      resetPageStatistics();
    }
  }

  @Override
  public void writeToFileWriter(TsFileIOWriter tsfileWriter) throws IOException {
    sealCurrentPage();
    chunkBuffer.writeAllPagesOfSeriesToTsFile(tsfileWriter, chunkStatistics);
    chunkBuffer.reset();
    // reset series_statistics
    this.chunkStatistics = Statistics.getStatsByType(dataType);
  }

  @Override
  public long estimateMaxSeriesMemSize() {
    return dataPageWriter.estimateMaxMemSize() + chunkBuffer.estimateMaxPageMemSize();
  }

  @Override
  public long getCurrentChunkSize() {
    // return the serialized size of the chunk header + all pages
    return ChunkHeader.getSerializedSize(measurementSchema.getMeasurementId()) + chunkBuffer
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
    return chunkBuffer.getNumOfPages();
  }
}
