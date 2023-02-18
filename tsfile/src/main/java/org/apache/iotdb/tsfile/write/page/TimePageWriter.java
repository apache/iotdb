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
package org.apache.iotdb.tsfile.write.page;

import org.apache.iotdb.tsfile.compress.ICompressor;
import org.apache.iotdb.tsfile.encoding.encoder.Encoder;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.statistics.TimeStatistics;
import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.apache.iotdb.tsfile.utils.ReadWriteForEncodingUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;

/**
 * This writer is used to write time into a page. It consists of a time encoder and respective
 * OutputStream.
 */
public class TimePageWriter {

  private static final Logger logger = LoggerFactory.getLogger(TimePageWriter.class);

  private final ICompressor compressor;

  // time
  private Encoder timeEncoder;
  private final PublicBAOS timeOut;

  /**
   * statistic of current page. It will be reset after calling {@code
   * writePageHeaderAndDataIntoBuff()}
   */
  private TimeStatistics statistics;

  public TimePageWriter(Encoder timeEncoder, ICompressor compressor) {
    this.timeOut = new PublicBAOS();
    this.timeEncoder = timeEncoder;
    this.statistics = new TimeStatistics();
    this.compressor = compressor;
  }

  /** write a time into encoder */
  public void write(long time) {
    timeEncoder.encode(time, timeOut);
    statistics.update(time);
  }

  /** write time series into encoder */
  public void write(long[] timestamps, int batchSize, int arrayOffset) {
    for (int i = arrayOffset; i < batchSize + arrayOffset; i++) {
      timeEncoder.encode(timestamps[i], timeOut);
    }
    statistics.update(timestamps, batchSize, arrayOffset);
  }

  /** flush all data remained in encoders. */
  private void prepareEndWriteOnePage() throws IOException {
    timeEncoder.flush(timeOut);
  }

  /**
   * getUncompressedBytes return data what it has been written in form of <code>
   * size of time list, time list, value list</code>
   *
   * @return a new readable ByteBuffer whose position is 0.
   */
  public ByteBuffer getUncompressedBytes() throws IOException {
    prepareEndWriteOnePage();
    ByteBuffer buffer = ByteBuffer.allocate(timeOut.size());
    buffer.put(timeOut.getBuf(), 0, timeOut.size());
    buffer.flip();
    return buffer;
  }

  /** write the page header and data into the PageWriter's output stream. */
  public int writePageHeaderAndDataIntoBuff(PublicBAOS pageBuffer, boolean first)
      throws IOException {
    if (statistics.getCount() == 0) {
      return 0;
    }

    ByteBuffer pageData = getUncompressedBytes();
    int uncompressedSize = pageData.remaining();
    int compressedSize;
    byte[] compressedBytes = null;

    if (compressor.getType().equals(CompressionType.UNCOMPRESSED)) {
      compressedSize = uncompressedSize;
    } else if (compressor.getType().equals(CompressionType.GZIP)) {
      compressedBytes =
          compressor.compress(pageData.array(), pageData.position(), uncompressedSize);
      compressedSize = compressedBytes.length;
    } else {
      compressedBytes = new byte[compressor.getMaxBytesForCompression(uncompressedSize)];
      // data is never a directByteBuffer now, so we can use data.array()
      compressedSize =
          compressor.compress(
              pageData.array(), pageData.position(), uncompressedSize, compressedBytes);
    }

    // write the page header to IOWriter
    int sizeWithoutStatistic = 0;
    if (first) {
      sizeWithoutStatistic +=
          ReadWriteForEncodingUtils.writeUnsignedVarInt(uncompressedSize, pageBuffer);
      sizeWithoutStatistic +=
          ReadWriteForEncodingUtils.writeUnsignedVarInt(compressedSize, pageBuffer);
    } else {
      ReadWriteForEncodingUtils.writeUnsignedVarInt(uncompressedSize, pageBuffer);
      ReadWriteForEncodingUtils.writeUnsignedVarInt(compressedSize, pageBuffer);
      statistics.serialize(pageBuffer);
    }

    // write page content to temp PBAOS
    logger.trace(
        "start to flush a time page data into buffer, buffer position {} ", pageBuffer.size());
    if (compressor.getType().equals(CompressionType.UNCOMPRESSED)) {
      try (WritableByteChannel channel = Channels.newChannel(pageBuffer)) {
        channel.write(pageData);
      }
    } else {
      pageBuffer.write(compressedBytes, 0, compressedSize);
    }
    logger.trace(
        "finish flushing a time page data into buffer, buffer position {} ", pageBuffer.size());
    return sizeWithoutStatistic;
  }

  /**
   * calculate max possible memory size it occupies, including time outputStream and value
   * outputStream, because size outputStream is never used until flushing.
   *
   * @return allocated size in time, value and outputStream
   */
  public long estimateMaxMemSize() {
    return timeOut.size() + timeEncoder.getMaxByteSize();
  }

  /** reset this page */
  public void reset() {
    timeOut.reset();
    statistics = new TimeStatistics();
  }

  public void setTimeEncoder(Encoder encoder) {
    this.timeEncoder = encoder;
  }

  public void initStatistics() {
    statistics = new TimeStatistics();
  }

  public long getPointNumber() {
    return statistics.getCount();
  }

  public TimeStatistics getStatistics() {
    return statistics;
  }
}
