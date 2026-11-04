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

package org.apache.iotdb.db.pipe.sink.protocol.thrift.async.handler;

import org.apache.iotdb.db.pipe.sink.protocol.thrift.async.IoTDBDataRegionAsyncSink;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class PipeTransferTsFileHandlerRateLimitTest {

  @Test
  public void testRateLimitUsesActualReadLengthAndSkipsEndOfFile() throws Exception {
    final File file = Files.createTempFile("pipe-transfer-rate-limit", ".tsfile").toFile();
    Files.write(file.toPath(), new byte[7]);

    final RecordingPipeTransferTsFileHandler handler = new RecordingPipeTransferTsFileHandler(file);
    try (final RandomAccessFile reader = new RandomAccessFile(file, "r")) {
      final byte[] readBuffer = new byte[4];

      Assert.assertEquals(4, handler.readNextFilePiece(reader, readBuffer));
      Assert.assertEquals(3, handler.readNextFilePiece(reader, readBuffer));
      Assert.assertEquals(-1, handler.readNextFilePiece(reader, readBuffer));

      Assert.assertEquals(file.length(), handler.rateLimitedBytes.get());
      Assert.assertEquals(2, handler.rateLimitInvocationCount.get());
    } finally {
      handler.close();
      Assert.assertTrue(file.delete());
    }
  }

  private static class RecordingPipeTransferTsFileHandler extends PipeTransferTsFileHandler {

    private final AtomicLong rateLimitedBytes = new AtomicLong(0);
    private final AtomicInteger rateLimitInvocationCount = new AtomicInteger(0);

    private RecordingPipeTransferTsFileHandler(final File file) throws InterruptedException {
      super(
          Mockito.mock(IoTDBDataRegionAsyncSink.class),
          Collections.emptyMap(),
          Collections.emptyList(),
          new AtomicInteger(1),
          new AtomicBoolean(false),
          file,
          null,
          false,
          null);
    }

    @Override
    protected void mayLimitRateAndRecordIO(final long requiredBytes) {
      rateLimitedBytes.addAndGet(requiredBytes);
      rateLimitInvocationCount.incrementAndGet();
    }
  }
}
