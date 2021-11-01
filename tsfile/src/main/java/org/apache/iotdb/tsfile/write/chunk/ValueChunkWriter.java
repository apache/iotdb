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

import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.compress.ICompressor;
import org.apache.iotdb.tsfile.encoding.encoder.Encoder;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.apache.iotdb.tsfile.write.page.ValuePageWriter;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;

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

  public void write(long[] timestamps, int[] values, int batchSize) {
    pageWriter.write(timestamps, values, batchSize);
  }

  public void write(long[] timestamps, long[] values, int batchSize) {
    pageWriter.write(timestamps, values, batchSize);
  }

  public void write(long[] timestamps, boolean[] values, int batchSize) {
    pageWriter.write(timestamps, values, batchSize);
  }

  public void write(long[] timestamps, float[] values, int batchSize) {
    pageWriter.write(timestamps, values, batchSize);
  }

  public void write(long[] timestamps, double[] values, int batchSize) {
    pageWriter.write(timestamps, values, batchSize);
  }

  public void write(long[] timestamps, Binary[] values, int batchSize) {
    pageWriter.write(timestamps, values, batchSize);
  }

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

  public void writeToFileWriter(TsFileIOWriter tsfileWriter) throws IOException {
    sealCurrentPage();
    writeAllPagesOfChunkToTsFile(tsfileWriter);

    // reinit this chunk writer
    pageBuffer.reset();
    numOfPages = 0;
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
    if (pageBuffer.size() == 0) {
      return 0;
    }
    // return the serialized size of the chunk header + all pages
    return ChunkHeader.getSerializedSize(measurementId, pageBuffer.size())
        + (long) pageBuffer.size();
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

  /** only used for test */
  public PublicBAOS getPageBuffer() {
    return pageBuffer;
  }
}
