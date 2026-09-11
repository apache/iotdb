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

package org.apache.iotdb.commons.snapshot;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Random;

public class SnapshotStreamFactoryTest {

  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private long originalBufferSizeMax;

  @Before
  public void setUp() {
    originalBufferSizeMax = SnapshotStreamFactory.getBufferSizeMax();
  }

  @After
  public void tearDown() {
    // The buffer size cap is process-global; restore it so tests do not leak state into each
    // other.
    SnapshotStreamFactory.setBufferSizeMax(originalBufferSizeMax);
  }

  @Test
  public void testDisabledBufferingReturnsRawStreams() {
    SnapshotStreamFactory.setBufferSizeMax(0);

    final OutputStream rawOut = new NullOutputStream();
    final InputStream rawIn = new NullInputStream();
    Assert.assertSame(rawOut, SnapshotStreamFactory.createOutputStream(rawOut));
    Assert.assertSame(rawIn, SnapshotStreamFactory.createInputStream(rawIn, 1024));
  }

  @Test
  public void testBufferSizeMaximumBoundary() {
    SnapshotStreamFactory.setBufferSizeMax(Integer.MAX_VALUE);
    Assert.assertEquals(Integer.MAX_VALUE, SnapshotStreamFactory.getBufferSizeMax());

    Assert.assertThrows(
        IllegalArgumentException.class,
        () -> SnapshotStreamFactory.setBufferSizeMax((long) Integer.MAX_VALUE + 1));
    Assert.assertEquals(Integer.MAX_VALUE, SnapshotStreamFactory.getBufferSizeMax());
  }

  @Test
  public void testRoundTripThroughBufferedStreams() throws IOException {
    // 100KB of random data with a 64KB buffer: writes must wrap the buffer several times.
    SnapshotStreamFactory.setBufferSizeMax(64 * 1024);
    final byte[] data = new byte[100 * 1024];
    new Random(42).nextBytes(data);

    final File file = temporaryFolder.newFile();
    try (FileOutputStream fileOutputStream = new FileOutputStream(file);
        OutputStream outputStream = SnapshotStreamFactory.createOutputStream(fileOutputStream)) {
      // Write in 4KB chunks so the 64KB buffer is actually filled, flushed and wrapped around.
      for (int offset = 0; offset < data.length; offset += 4096) {
        outputStream.write(data, offset, Math.min(4096, data.length - offset));
      }
    }

    final byte[] readBack = new byte[data.length];
    try (FileInputStream fileInputStream = new FileInputStream(file);
        InputStream inputStream =
            SnapshotStreamFactory.createInputStream(fileInputStream, file.length())) {
      int offset = 0;
      while (offset < readBack.length) {
        final int read = inputStream.read(readBack, offset, readBack.length - offset);
        if (read < 0) {
          break;
        }
        offset += read;
      }
      Assert.assertEquals(readBack.length, offset);
    }
    Assert.assertArrayEquals(data, readBack);
  }

  @Test
  public void testLargeWriteBypassesBuffer() throws IOException {
    // A single write larger than the buffer must bypass it and still land correctly on the
    // underlying stream.
    SnapshotStreamFactory.setBufferSizeMax(64 * 1024);
    final byte[] data = new byte[1024 * 1024];
    new Random(7).nextBytes(data);

    final File file = temporaryFolder.newFile();
    try (FileOutputStream fileOutputStream = new FileOutputStream(file);
        OutputStream outputStream = SnapshotStreamFactory.createOutputStream(fileOutputStream)) {
      outputStream.write(data);
    }
    Assert.assertEquals(data.length, file.length());

    final byte[] readBack = new byte[data.length];
    try (InputStream inputStream = new FileInputStream(file)) {
      int offset = 0;
      while (offset < readBack.length) {
        final int read = inputStream.read(readBack, offset, readBack.length - offset);
        if (read < 0) {
          break;
        }
        offset += read;
      }
      Assert.assertEquals(readBack.length, offset);
    }
    Assert.assertTrue(Arrays.equals(data, readBack));
  }

  @Test
  public void testWriteBufferReuse() {
    // Use an explicit cap so the pool semantics do not depend on state left by other tests.
    SnapshotStreamFactory.setBufferSizeMax(128 * 1024);
    final byte[] first = SnapshotStreamFactory.acquireBuffer(64 * 1024);
    SnapshotStreamFactory.releaseBuffer(first);

    // The released buffer is handed out again for a request that fits.
    final byte[] second = SnapshotStreamFactory.acquireBuffer(64 * 1024);
    Assert.assertSame(first, second);
    SnapshotStreamFactory.releaseBuffer(second);

    // A borrowed buffer is removed from the pool: a concurrent borrow must not share it.
    final byte[] borrowed = SnapshotStreamFactory.acquireBuffer(64 * 1024);
    final byte[] other = SnapshotStreamFactory.acquireBuffer(64 * 1024);
    Assert.assertNotSame(borrowed, other);
    SnapshotStreamFactory.releaseBuffer(other);

    // A smaller request reuses the larger cached buffer while it still fits the cap.
    final byte[] smaller = SnapshotStreamFactory.acquireBuffer(1024);
    Assert.assertSame(other, smaller);
    SnapshotStreamFactory.releaseBuffer(smaller);

    // A request larger than the cached buffer allocates a fresh one.
    final byte[] tiny = new byte[32 * 1024];
    SnapshotStreamFactory.releaseBuffer(tiny);
    final byte[] larger = SnapshotStreamFactory.acquireBuffer(64 * 1024);
    Assert.assertNotSame(tiny, larger);
    SnapshotStreamFactory.releaseBuffer(borrowed);
    SnapshotStreamFactory.releaseBuffer(larger);
  }

  @Test
  public void testBufferNotReusedAfterCapDecrease() {
    // Prime the pool with a large write buffer under a large cap.
    SnapshotStreamFactory.setBufferSizeMax(128 * 1024);
    final byte[] large = SnapshotStreamFactory.acquireBuffer(128 * 1024);
    SnapshotStreamFactory.releaseBuffer(large);

    // Lower the cap: the pooled buffer now exceeds it and must not be handed out again.
    SnapshotStreamFactory.setBufferSizeMax(8192);
    final byte[] borrowed = SnapshotStreamFactory.acquireBuffer(8192);
    Assert.assertNotSame(large, borrowed);
    Assert.assertTrue(borrowed.length <= 8192);
    SnapshotStreamFactory.releaseBuffer(borrowed);

    // The buffer allocated under the new cap is still reused for requests that fit it.
    final byte[] again = SnapshotStreamFactory.acquireBuffer(8192);
    Assert.assertSame(borrowed, again);
    SnapshotStreamFactory.releaseBuffer(again);
  }

  @Test
  public void testCloseIsIdempotentAndWriteAfterCloseFailsOnFlush() throws IOException {
    final File file = temporaryFolder.newFile();
    final OutputStream outputStream =
        SnapshotStreamFactory.createOutputStream(new FileOutputStream(file));
    outputStream.write(1);
    outputStream.close();
    // Double close must not throw.
    outputStream.close();

    // Writes and flushes after close must fail instead of silently losing data.
    Assert.assertThrows(IOException.class, () -> outputStream.write(2));
    Assert.assertThrows(IOException.class, outputStream::flush);
    // Closing an already closed stream remains idempotent.
    outputStream.close();
  }

  @Test
  public void testClosePropagatesFlushFailureAndClosesUnderlyingStream() throws IOException {
    SnapshotStreamFactory.setBufferSizeMax(64);
    final FailingOutputStream rawOut = new FailingOutputStream(false);
    final OutputStream outputStream = SnapshotStreamFactory.createOutputStream(rawOut);
    outputStream.write(1);

    final IOException exception = Assert.assertThrows(IOException.class, outputStream::close);
    Assert.assertEquals("write failure", exception.getMessage());
    Assert.assertTrue(rawOut.closed);
  }

  @Test
  public void testCloseSuppressesUnderlyingCloseFailureAfterFlushFailure() throws IOException {
    SnapshotStreamFactory.setBufferSizeMax(64);
    final FailingOutputStream rawOut = new FailingOutputStream(true);
    final OutputStream outputStream = SnapshotStreamFactory.createOutputStream(rawOut);
    outputStream.write(1);

    final IOException exception = Assert.assertThrows(IOException.class, outputStream::close);
    Assert.assertEquals("write failure", exception.getMessage());
    Assert.assertEquals(1, exception.getSuppressed().length);
    Assert.assertEquals("close failure", exception.getSuppressed()[0].getMessage());
    Assert.assertTrue(rawOut.closed);
  }

  @Test
  public void testInputBufferNeverExceedsFileSizeOrCap() throws IOException {
    // The read buffer of a stream created for a small file must be capped, and reading must
    // still see the whole content.
    final byte[] data = new byte[100];
    new Random(3).nextBytes(data);
    final File file = temporaryFolder.newFile();
    try (FileOutputStream fileOutputStream = new FileOutputStream(file)) {
      fileOutputStream.write(data);
    }

    SnapshotStreamFactory.setBufferSizeMax(64);
    final byte[] readBack = new byte[data.length];
    try (FileInputStream fileInputStream = new FileInputStream(file);
        InputStream inputStream =
            SnapshotStreamFactory.createInputStream(fileInputStream, file.length())) {
      int offset = 0;
      while (offset < readBack.length) {
        final int read = inputStream.read(readBack, offset, readBack.length - offset);
        if (read < 0) {
          break;
        }
        offset += read;
      }
      Assert.assertEquals(readBack.length, offset);
    }
    Assert.assertArrayEquals(data, readBack);
  }

  /** OutputStream that discards everything, used to test the disabled-buffering fast path. */
  private static final class NullOutputStream extends OutputStream {
    @Override
    public void write(final int b) {
      // discard
    }
  }

  /** InputStream that is always at EOF, used to test the disabled-buffering fast path. */
  private static final class NullInputStream extends InputStream {
    @Override
    public int read() {
      return -1;
    }
  }

  private static final class FailingOutputStream extends OutputStream {

    private final boolean failOnClose;
    private boolean closed;

    private FailingOutputStream(final boolean failOnClose) {
      this.failOnClose = failOnClose;
    }

    @Override
    public void write(final int b) throws IOException {
      throw new IOException("write failure");
    }

    @Override
    public void write(final byte[] b, final int off, final int len) throws IOException {
      throw new IOException("write failure");
    }

    @Override
    public void close() throws IOException {
      closed = true;
      if (failOnClose) {
        throw new IOException("close failure");
      }
    }
  }
}
