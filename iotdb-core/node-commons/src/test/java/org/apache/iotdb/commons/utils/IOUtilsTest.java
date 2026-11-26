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
package org.apache.iotdb.commons.utils;

import org.apache.iotdb.commons.i18n.UtilMessages;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.atomic.AtomicInteger;

public class IOUtilsTest {

  private static final String ENCODING = "UTF-8";

  @Test
  public void writeFullyCompletesShortPositionalWrites() throws IOException {
    ByteBuffer buffer = ByteBuffer.wrap(new byte[] {9, 1, 2, 3, 8});
    buffer.position(1);
    buffer.limit(4);
    ByteArrayOutputStream persisted = new ByteArrayOutputStream();
    FileChannel channel = Mockito.mock(FileChannel.class);
    long start = Integer.MAX_VALUE + 5L;
    Mockito.when(channel.write(Mockito.any(ByteBuffer.class), Mockito.anyLong()))
        .thenAnswer(
            invocation -> {
              Assert.assertEquals(start + persisted.size(), (long) invocation.getArgument(1));
              persisted.write(((ByteBuffer) invocation.getArgument(0)).get());
              return 1;
            });

    Assert.assertEquals(3, IOUtils.writeFully(channel, buffer, start));

    Assert.assertArrayEquals(new byte[] {1, 2, 3}, persisted.toByteArray());
    Assert.assertEquals(4, buffer.position());
    Assert.assertEquals(4, buffer.limit());
  }

  @Test
  public void writeFullyCompletesShortSequentialWrites() throws IOException {
    ByteBuffer buffer = ByteBuffer.wrap(new byte[] {1, 2, 3});
    ByteArrayOutputStream persisted = new ByteArrayOutputStream();
    FileChannel channel = Mockito.mock(FileChannel.class);
    Mockito.when(channel.write(Mockito.any(ByteBuffer.class)))
        .thenAnswer(
            invocation -> {
              persisted.write(((ByteBuffer) invocation.getArgument(0)).get());
              return 1;
            });

    Assert.assertEquals(3, IOUtils.writeFully(channel, buffer));

    Assert.assertArrayEquals(new byte[] {1, 2, 3}, persisted.toByteArray());
    Assert.assertFalse(buffer.hasRemaining());
  }

  @Test
  public void writeFullyDoesNotWriteEmptyBuffers() throws IOException {
    FileChannel channel = Mockito.mock(FileChannel.class);
    ByteBuffer buffer = ByteBuffer.allocate(0);

    Assert.assertEquals(0, IOUtils.writeFully(channel, buffer, 7));
    Assert.assertEquals(0, IOUtils.writeFully(channel, buffer));

    Mockito.verifyZeroInteractions(channel);
  }

  @Test
  public void writeFullyFailsImmediatelyWhenNoProgressIsMade() throws IOException {
    for (boolean positional : new boolean[] {false, true}) {
      for (int noProgress : new int[] {0, -1}) {
        FileChannel channel = Mockito.mock(FileChannel.class);
        IOException unexpectedRetry = new IOException("Unexpected retry after no progress");
        if (positional) {
          Mockito.when(channel.write(Mockito.any(ByteBuffer.class), Mockito.anyLong()))
              .thenAnswer(
                  invocation -> {
                    ((ByteBuffer) invocation.getArgument(0)).get();
                    return 1;
                  })
              .thenReturn(noProgress)
              .thenThrow(unexpectedRetry);
        } else {
          Mockito.when(channel.write(Mockito.any(ByteBuffer.class)))
              .thenAnswer(
                  invocation -> {
                    ((ByteBuffer) invocation.getArgument(0)).get();
                    return 1;
                  })
              .thenReturn(noProgress)
              .thenThrow(unexpectedRetry);
        }
        ByteBuffer buffer = ByteBuffer.wrap(new byte[] {1, 2, 3});

        IOException failure =
            Assert.assertThrows(
                IOException.class,
                () -> {
                  if (positional) {
                    IOUtils.writeFully(channel, buffer, 5);
                  } else {
                    IOUtils.writeFully(channel, buffer);
                  }
                });

        Assert.assertEquals(
            UtilMessages.EXCEPTION_FILE_CHANNEL_WRITE_MADE_NO_PROGRESS_B0F24ACD,
            failure.getMessage());
        Assert.assertEquals(1, buffer.position());
        if (positional) {
          Mockito.verify(channel, Mockito.times(2))
              .write(Mockito.any(ByteBuffer.class), Mockito.anyLong());
        } else {
          Mockito.verify(channel, Mockito.times(2)).write(Mockito.any(ByteBuffer.class));
        }
      }
    }
  }

  @Test
  public void writeFullyPropagatesOriginalIOException() throws IOException {
    for (boolean positional : new boolean[] {false, true}) {
      FileChannel channel = Mockito.mock(FileChannel.class);
      IOException failure = new IOException("Injected write failure");
      if (positional) {
        Mockito.when(channel.write(Mockito.any(ByteBuffer.class), Mockito.anyLong()))
            .thenThrow(failure);
      } else {
        Mockito.when(channel.write(Mockito.any(ByteBuffer.class))).thenThrow(failure);
      }

      Assert.assertSame(
          failure,
          Assert.assertThrows(
              IOException.class,
              () -> {
                if (positional) {
                  IOUtils.writeFully(channel, ByteBuffer.allocate(1), 0);
                } else {
                  IOUtils.writeFully(channel, ByteBuffer.allocate(1));
                }
              }));
    }
  }

  @Test
  public void writeFullyPreservesPositionalAndSequentialChannelSemantics() throws IOException {
    Path path = Files.createTempFile("iotdb-write-fully", ".bin");
    try (FileChannel channel =
        FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
      channel.position(2);

      Assert.assertEquals(2, IOUtils.writeFully(channel, ByteBuffer.wrap(new byte[] {1, 2}), 0));
      Assert.assertEquals(2, channel.position());
      Assert.assertEquals(1, IOUtils.writeFully(channel, ByteBuffer.wrap(new byte[] {3})));
      Assert.assertEquals(3, channel.position());
      Assert.assertArrayEquals(new byte[] {1, 2, 3}, Files.readAllBytes(path));
    } finally {
      Files.delete(path);
    }
  }

  @Test
  public void readStringReadsCompletePayloadAfterShortRead() throws IOException {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    IOUtils.writeString(outputStream, "abcdefg", ENCODING, null);

    try (DataInputStream inputStream =
        new DataInputStream(new OneByteAtATimeInputStream(outputStream.toByteArray()))) {
      Assert.assertEquals("abcdefg", IOUtils.readString(inputStream, ENCODING, null));
    }
  }

  @Test
  public void readStringThrowsWhenPayloadIsTruncated() throws IOException {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    IOUtils.writeInt(outputStream, 7, null);
    outputStream.write(new byte[] {'a', 'b', 'c'});

    try (DataInputStream inputStream =
        new DataInputStream(new OneByteAtATimeInputStream(outputStream.toByteArray()))) {
      Assert.assertThrows(
          EOFException.class, () -> IOUtils.readString(inputStream, ENCODING, null));
    }
  }

  @Test
  public void readFullyReadsCompleteByteBufferAfterShortChannelRead() throws IOException {
    byte[] bytes = new byte[] {1, 2, 3};
    FileChannel channel = mockOneByteAtATimeChannel(bytes);
    ByteBuffer buffer = ByteBuffer.allocate(bytes.length);

    IOUtils.readFully(channel, buffer);

    Assert.assertArrayEquals(bytes, buffer.array());
  }

  @Test
  public void readFullyReadsCompleteByteBufferFromPositionAfterShortChannelRead()
      throws IOException {
    byte[] bytes = new byte[] {1, 2, 3, 4, 5};
    FileChannel channel = mockOneByteAtATimeChannel(bytes);
    ByteBuffer buffer = ByteBuffer.allocate(3);

    IOUtils.readFully(channel, buffer, 2);

    Assert.assertArrayEquals(new byte[] {3, 4, 5}, buffer.array());
  }

  @Test
  public void readFullyThrowsWhenChannelIsTruncated() throws IOException {
    FileChannel channel = mockOneByteAtATimeChannel(new byte[] {1, 2});
    ByteBuffer buffer = ByteBuffer.allocate(3);

    Assert.assertThrows(EOFException.class, () -> IOUtils.readFully(channel, buffer));
  }

  private static FileChannel mockOneByteAtATimeChannel(byte[] bytes) throws IOException {
    FileChannel channel = Mockito.mock(FileChannel.class);
    AtomicInteger index = new AtomicInteger();
    Mockito.when(channel.read(Mockito.any(ByteBuffer.class)))
        .thenAnswer(
            invocation -> {
              ByteBuffer buffer = invocation.getArgument(0);
              int currentIndex = index.getAndIncrement();
              if (currentIndex >= bytes.length) {
                return -1;
              }
              buffer.put(bytes[currentIndex]);
              return 1;
            });
    Mockito.when(channel.read(Mockito.any(ByteBuffer.class), Mockito.anyLong()))
        .thenAnswer(
            invocation -> {
              ByteBuffer buffer = invocation.getArgument(0);
              long position = invocation.getArgument(1);
              if (position >= bytes.length) {
                return -1;
              }
              buffer.put(bytes[(int) position]);
              return 1;
            });
    return channel;
  }

  private static class OneByteAtATimeInputStream extends InputStream {

    private final byte[] bytes;
    private int index;

    private OneByteAtATimeInputStream(byte[] bytes) {
      this.bytes = bytes;
    }

    @Override
    public int read() {
      return index < bytes.length ? bytes[index++] & 0xFF : -1;
    }

    @Override
    public int read(byte[] b, int off, int len) {
      if (len == 0) {
        return 0;
      }
      if (index >= bytes.length) {
        return -1;
      }
      b[off] = bytes[index++];
      return 1;
    }
  }
}
