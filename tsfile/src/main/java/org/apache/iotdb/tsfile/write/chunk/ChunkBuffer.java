/**
 * Copyright © 2019 Apache IoTDB(incubating) (dev@iotdb.apache.org)
 *
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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.tsfile.write.chunk;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import org.apache.iotdb.tsfile.compress.Compressor;
import org.apache.iotdb.tsfile.exception.write.PageException;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Store current pages in this chunk.
 *
 * @author kangrong
 */
public class ChunkBuffer {

  private static Logger LOG = LoggerFactory.getLogger(ChunkBuffer.class);
  private final Compressor compressor;
  private final MeasurementSchema schema;

  private int numOfPages;

  /**
   * all pages of this column.
   */
  private PublicBAOS pageBuffer;

  private long totalValueCount;
  private long maxTimestamp;
  private long minTimestamp = -1;
  private ByteBuffer compressedData;// DirectByteBuffer

  /**
   * constructor of ChunkBuffer.
   *
   * @param schema measurement schema
   */
  public ChunkBuffer(MeasurementSchema schema) {
    this.schema = schema;
    this.compressor = schema.getCompressor();
    this.pageBuffer = new PublicBAOS();
  }

  public int getNumOfPages() {
    return numOfPages;
  }

  public void setNumOfPages(int numOfPages) {
    this.numOfPages = numOfPages;
  }

  /**
   * write the page header and data into the PageWriter's output stream.
   *
   * @param data the data of the page
   * @param valueCount - the amount of values in that page
   * @param statistics - the statistics for that page
   * @param maxTimestamp - timestamp maximum in given data
   * @param minTimestamp - timestamp minimum in given data
   * @return byte size of the page header and uncompressed data in the page body.
   */
  public int writePageHeaderAndDataIntoBuff(ByteBuffer data, int valueCount,
      Statistics<?> statistics,
      long maxTimestamp, long minTimestamp) throws PageException {
    numOfPages++;

    // 1. update time statistics
    if (this.minTimestamp == -1) {
      this.minTimestamp = minTimestamp;
    }
    if (this.minTimestamp == -1) {
      throw new PageException("minTimestamp of this page is -1, no valid data point in this page");
    }
    this.maxTimestamp = maxTimestamp;
    int uncompressedSize = data.remaining();
    int maxSize = compressor.getMaxBytesForCompression(uncompressedSize);
    int compressedSize = 0;
    int compressedPosition = 0;
    byte[] compressedBytes = null;

    if (compressor.getType().equals(CompressionType.UNCOMPRESSED)) {
      compressedSize = data.remaining();
    } else {
      // data is never a directByteBuffer now.
      if (compressedBytes == null
          || compressedBytes.length < compressor.getMaxBytesForCompression(uncompressedSize)) {
        compressedBytes = new byte[compressor.getMaxBytesForCompression(uncompressedSize)];
      }
      try {
        compressedPosition = 0;
        compressedSize = compressor
            .compress(data.array(), data.position(), data.remaining(), compressedBytes);
      } catch (IOException e) {
        throw new PageException("Error when writing a page, " + e.getMessage());
      }
    }

    int headerSize = 0;

    // write the page header to IOWriter
    try {
      PageHeader header = new PageHeader(uncompressedSize, compressedSize, valueCount, statistics,
          maxTimestamp,
          minTimestamp);
      headerSize = header.getSerializedSize();
      LOG.debug("start to flush a page header into buffer, buffer position {} ", pageBuffer.size());
      header.serializeTo(pageBuffer);
      LOG.debug("finish to flush a page header {} of {} into buffer, buffer position {} ", header,
          schema.getMeasurementId(), pageBuffer.size());

    } catch (IOException e) {
      resetTimeStamp();
      throw new PageException(
          "IO Exception in writeDataPageHeader,ignore this page,error message:" + e.getMessage());
    }

    // update data point num
    this.totalValueCount += valueCount;

    // write page content to temp PBAOS
    try {
      LOG.debug("start to flush a page data into buffer, buffer position {} ", pageBuffer.size());
      if (compressor.getType().equals(CompressionType.UNCOMPRESSED)) {
        WritableByteChannel channel = Channels.newChannel(pageBuffer);
        channel.write(data);
      } else {
        if (data.isDirect()) {
          WritableByteChannel channel = Channels.newChannel(pageBuffer);
          channel.write(compressedData);
        } else {
          pageBuffer.write(compressedBytes, compressedPosition, compressedSize);
        }
      }
      LOG.debug("start to flush a page data into buffer, buffer position {} ", pageBuffer.size());
    } catch (IOException e) {
      throw new PageException(
          "meet IO Exception in buffer append,but we cannot understand it:" + e.getMessage());
    }
    return headerSize + uncompressedSize;
  }

  private void resetTimeStamp() {
    if (totalValueCount == 0) {
      minTimestamp = -1;
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
    if (minTimestamp == -1) {
      LOG.error("Write page error, {}, minTime:{}, maxTime:{}", schema, minTimestamp, maxTimestamp);
    }

    // start to write this column chunk
    int headerSize = writer.startFlushChunk(schema, compressor.getType(), schema.getType(),
        schema.getEncodingType(), statistics, maxTimestamp, minTimestamp, pageBuffer.size(),
        numOfPages);

    long totalByteSize = writer.getPos();
    LOG.debug("start writing pages of {} into file, position {}", schema.getMeasurementId(),
        writer.getPos());

    // write all pages of this column
    writer.writeBytesToStream(pageBuffer);
    LOG.debug("finish writing pages of {} into file, position {}", schema.getMeasurementId(),
        writer.getPos());

    long size = writer.getPos() - totalByteSize;
    assert size == pageBuffer.size();

    writer.endChunk(totalValueCount);
    return headerSize + size;
  }

  /**
   * reset exist data in page for next stage.
   */
  public void reset() {
    minTimestamp = -1;
    pageBuffer.reset();
    totalValueCount = 0;
  }

  /**
   * estimate max page memory size.
   *
   * @return the max possible allocated size currently
   */
  public long estimateMaxPageMemSize() {
    // return size of buffer + page max size;
    return pageBuffer.size() + estimateMaxPageHeaderSize();
  }

  private int estimateMaxPageHeaderSize() {
    int digestSize = (totalValueCount == 0) ? 0 : schema.getTypeLength() * 2;
    return PageHeader.calculatePageHeaderSize(schema.getType());
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
