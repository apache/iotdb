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

import com.google.common.util.concurrent.RateLimiter;
import org.apache.tsfile.write.writer.TsFileOutput;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class CompactionTsFileOutput extends OutputStream implements TsFileOutput {

  private TsFileOutput output;
  private RateLimiter rateLimiter;
  private final int maxSizePerWrite;

  public CompactionTsFileOutput(TsFileOutput output, RateLimiter rateLimiter) {
    this.output = output;
    this.rateLimiter = rateLimiter;
    this.maxSizePerWrite = (int) Math.min((long) rateLimiter.getRate(), Integer.MAX_VALUE);
  }

  @Override
  public void write(int b) throws IOException {
    rateLimiter.acquire(1);
    output.wrapAsStream().write(b);
  }

  @Override
  public void write(byte[] b) throws IOException {
    write(b, 0, b.length);
  }

  @Override
  public void write(byte b) throws IOException {
    rateLimiter.acquire(1);
    output.write(b);
  }

  @Override
  public void write(ByteBuffer b) throws IOException {
    write(b.array());
  }

  @Override
  public long getPosition() throws IOException {
    return output.getPosition();
  }

  @Override
  public void close() throws IOException {
    output.close();
  }

  @Override
  public OutputStream wrapAsStream() {
    return this;
  }

  @Override
  public void flush() throws IOException {
    output.flush();
  }

  @Override
  public void truncate(long size) throws IOException {
    output.truncate(size);
  }

  @Override
  public void force() throws IOException {
    output.force();
  }

  @Override
  public void write(byte[] buf, int start, int length) throws IOException {
    while (length > 0) {
      int writeSize = Math.min(length, maxSizePerWrite);
      rateLimiter.acquire(writeSize);
      output.wrapAsStream().write(buf, start, writeSize);
      start += writeSize;
      length -= writeSize;
    }
  }
}
