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

import org.apache.iotdb.db.service.metrics.CompactionMetrics;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.CompactionTaskManager;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.constant.CompactionIoDataType;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.constant.CompactionType;

import org.apache.tsfile.exception.StopReadTsFileByInterruptException;
import org.apache.tsfile.read.reader.TsFileInput;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

public class CompactionTsFileInput implements TsFileInput {
  private final TsFileInput tsFileInput;

  private long metadataOffset = -1;

  /** The type of compaction running. */
  private final CompactionType compactionType;

  /** A flag that indicates if an aligned series is being read. */
  private volatile boolean readingAlignedSeries = false;

  public CompactionTsFileInput(CompactionType compactionType, TsFileInput tsFileInput) {
    this.compactionType = compactionType;
    this.tsFileInput = tsFileInput;
  }

  public void setMetadataOffset(long metadataOffset) {
    this.metadataOffset = metadataOffset;
  }

  /** Marks the start of reading an aligned series. */
  public void markStartOfAlignedSeries() {
    readingAlignedSeries = true;
  }

  /** Marks the end of reading an aligned series. */
  public void markEndOfAlignedSeries() {
    readingAlignedSeries = false;
  }

  @Override
  public long size() throws IOException {
    try {
      return tsFileInput.size();
    } catch (Exception e) {
      if (Thread.currentThread().isInterrupted()) {
        throw new StopReadTsFileByInterruptException();
      }
      throw e;
    }
  }

  @Override
  public long position() throws IOException {
    try {
      return tsFileInput.position();
    } catch (Exception e) {
      if (Thread.currentThread().isInterrupted()) {
        throw new StopReadTsFileByInterruptException();
      }
      throw e;
    }
  }

  @Override
  public TsFileInput position(long newPosition) throws IOException {
    try {
      return tsFileInput.position(newPosition);
    } catch (Exception e) {
      if (Thread.currentThread().isInterrupted()) {
        throw new StopReadTsFileByInterruptException();
      }
      throw e;
    }
  }

  @Override
  public int read(ByteBuffer dst) throws IOException {
    acquireReadDataSizeWithCompactionReadRateLimiter(dst.remaining());
    int readSize = tsFileInput.read(dst);
    updateMetrics(position(), readSize);
    if (Thread.currentThread().isInterrupted()) {
      throw new StopReadTsFileByInterruptException();
    }
    return readSize;
  }

  @Override
  public int read(ByteBuffer dst, long position) throws IOException {
    acquireReadDataSizeWithCompactionReadRateLimiter(dst.remaining());
    int readSize = tsFileInput.read(dst, position);
    updateMetrics(position, readSize);
    if (Thread.currentThread().isInterrupted()) {
      throw new StopReadTsFileByInterruptException();
    }
    return readSize;
  }

  @Override
  public InputStream wrapAsInputStream() throws IOException {
    return new CompactionTsFileInputStreamWrapper(tsFileInput.wrapAsInputStream());
  }

  @Override
  public void close() throws IOException {
    tsFileInput.close();
  }

  @Override
  public String getFilePath() {
    return tsFileInput.getFilePath();
  }

  private void acquireReadDataSizeWithCompactionReadRateLimiter(int readDataSize) {
    CompactionTaskManager.getInstance().getCompactionReadOperationRateLimiter().acquire(1);
    CompactionTaskManager.getInstance().getCompactionReadRateLimiter().acquire(readDataSize);
  }

  private void updateMetrics(long position, long totalSize) {
    if (position >= metadataOffset) {
      CompactionMetrics.getInstance()
          .recordReadInfo(compactionType, CompactionIoDataType.METADATA, totalSize);
    } else {
      CompactionMetrics.getInstance()
          .recordReadInfo(
              compactionType,
              readingAlignedSeries
                  ? CompactionIoDataType.ALIGNED
                  : CompactionIoDataType.NOT_ALIGNED,
              totalSize);
    }
  }

  private class CompactionTsFileInputStreamWrapper extends InputStream {

    private final InputStream inputStream;

    public CompactionTsFileInputStreamWrapper(InputStream inputStream) {
      this.inputStream = inputStream;
    }

    @Override
    public int read() throws IOException {
      acquireReadDataSizeWithCompactionReadRateLimiter(1);
      long position = position();
      int readSize = inputStream.read();
      updateMetrics(position, readSize);
      return readSize;
    }

    @Override
    public int read(byte[] b) throws IOException {
      acquireReadDataSizeWithCompactionReadRateLimiter(b.length);
      long position = position();
      int readSize = inputStream.read(b);
      updateMetrics(position, readSize);
      return readSize;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      acquireReadDataSizeWithCompactionReadRateLimiter(len);
      long position = position();
      int readSize = inputStream.read(b, off, len);
      updateMetrics(position, readSize);
      return readSize;
    }

    @Override
    public long skip(long n) throws IOException {
      return inputStream.skip(n);
    }

    @Override
    public int available() throws IOException {
      return inputStream.available();
    }

    @Override
    public void close() throws IOException {
      inputStream.close();
    }

    @Override
    public synchronized void mark(int readlimit) {
      inputStream.mark(readlimit);
    }

    @Override
    public synchronized void reset() throws IOException {
      inputStream.reset();
    }

    @Override
    public boolean markSupported() {
      return inputStream.markSupported();
    }
  }
}
