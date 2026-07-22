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
import java.util.concurrent.atomic.AtomicInteger;

public class IOUtilsTest {

  private static final String ENCODING = "UTF-8";

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
