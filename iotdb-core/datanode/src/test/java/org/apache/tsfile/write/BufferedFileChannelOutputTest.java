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

package org.apache.tsfile.write;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;

/** Writes are buffered until the buffer is full, and the position counts the buffered bytes. */
public class BufferedFileChannelOutputTest {

  @Rule public final TemporaryFolder folder = new TemporaryFolder();

  @Test
  public void testSmallWritesStayInTheBufferUntilFlushed() throws Exception {
    final File file = folder.newFile();
    try (final FileChannel channel = open(file);
        final BufferedFileChannelOutput out = new BufferedFileChannelOutput(channel, 64)) {
      out.write("hello".getBytes(StandardCharsets.UTF_8));

      assertEquals(5L, out.getPosition());
      assertEquals("nothing is on disk yet", 0L, channel.size());

      out.flush();
      assertEquals(5L, channel.size());
    }
    assertArrayEquals("hello".getBytes(StandardCharsets.UTF_8), Files.readAllBytes(file.toPath()));
  }

  @Test
  public void testWritesAreFlushedWhenTheBufferWouldOverflow() throws Exception {
    final File file = folder.newFile();
    try (final FileChannel channel = open(file);
        final BufferedFileChannelOutput out = new BufferedFileChannelOutput(channel, 8)) {
      out.write(new byte[] {1, 2, 3, 4, 5});
      out.write(new byte[] {6, 7, 8, 9, 10});

      assertEquals("the first block had to leave the buffer", 5L, channel.size());
      assertEquals(10L, out.getPosition());

      out.force();
      assertEquals(10L, channel.size());
    }
  }

  @Test
  public void testAWriteAtLeastAsLargeAsTheBufferBypassesIt() throws Exception {
    final File file = folder.newFile();
    final byte[] block = new byte[100];
    try (final FileChannel channel = open(file);
        final BufferedFileChannelOutput out = new BufferedFileChannelOutput(channel, 8)) {
      out.write(block);

      assertEquals("a large block is written directly", 100L, channel.size());
      assertEquals(100L, out.getPosition());
    }
    assertArrayEquals(block, Files.readAllBytes(file.toPath()));
  }

  @Test
  public void testByteBufferWritesAreBuffered() throws Exception {
    final File file = folder.newFile();
    final byte[] block = {1, 2, 3};
    try (final FileChannel channel = open(file);
        final BufferedFileChannelOutput out = new BufferedFileChannelOutput(channel, 8)) {
      final ByteBuffer src = ByteBuffer.wrap(block);
      out.write(src);

      assertEquals(0, src.remaining());
      assertEquals(3L, out.getPosition());
      assertEquals(0L, channel.size());

      out.force();
      assertEquals(3L, channel.size());
    }
    assertArrayEquals(block, Files.readAllBytes(file.toPath()));
  }

  @Test
  public void testSeekingToAnotherPositionFlushesFirst() throws Exception {
    final File file = folder.newFile();
    try (final FileChannel channel = open(file);
        final BufferedFileChannelOutput out = new BufferedFileChannelOutput(channel, 64)) {
      out.write("abc".getBytes(StandardCharsets.UTF_8));
      out.position(1L);
      out.write("Z".getBytes(StandardCharsets.UTF_8));
      out.force();
    }
    assertEquals("aZc", new String(Files.readAllBytes(file.toPath()), StandardCharsets.UTF_8));
  }

  @Test
  public void testSeekingToTheCurrentPositionDoesNotFlush() throws Exception {
    final File file = folder.newFile();
    try (final FileChannel channel = open(file);
        final BufferedFileChannelOutput out = new BufferedFileChannelOutput(channel, 64)) {
      out.write("abc".getBytes(StandardCharsets.UTF_8));
      out.position(out.getPosition());

      assertEquals(0L, channel.size());
    }
  }

  @Test
  public void testTheOutputIsItsOwnStream() throws Exception {
    final File file = folder.newFile();
    try (final FileChannel channel = open(file);
        final BufferedFileChannelOutput out = new BufferedFileChannelOutput(channel, 64)) {
      assertSame(out, out.wrapAsStream());
    }
  }

  @Test
  public void testANonPositiveBufferSizeIsRejected() throws Exception {
    final File file = folder.newFile();
    try (final FileChannel channel = open(file)) {
      assertThrows(IllegalArgumentException.class, () -> new BufferedFileChannelOutput(channel, 0));
      assertThrows(
          IllegalArgumentException.class, () -> new BufferedFileChannelOutput(channel, -1));
    }
  }

  private static FileChannel open(final File file) throws IOException {
    return FileChannel.open(
        file.toPath(),
        StandardOpenOption.CREATE,
        StandardOpenOption.WRITE,
        StandardOpenOption.READ,
        StandardOpenOption.TRUNCATE_EXISTING);
  }
}
