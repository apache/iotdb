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

import org.apache.iotdb.db.i18n.DataNodeMiscMessages;
import org.apache.iotdb.db.i18n.StorageEngineMessages;

import org.apache.tsfile.write.writer.TsFileOutput;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Objects;

/**
 * A {@link TsFileOutput} that buffers writes and hands them to the {@link FileChannel} once the
 * buffer size the caller passes in is reached. The buffer is allocated at its full size when the
 * output is created, and it is flushed by {@link #force()}, {@link #flush()}, {@link
 * #truncate(long)}, a seek to another position, or {@link #close()}.
 *
 * <p>This class is not thread-safe; callers must serialize access to one instance.
 */
public final class BufferedFileChannelOutput extends OutputStream implements TsFileOutput {

  private final FileChannel channel;
  private final int bufferSize;
  private final byte[] buffer;

  /** Reused wrapper over {@link #buffer} so each flush does not allocate a new ByteBuffer. */
  private final ByteBuffer bufferView;

  private int bufferLength;

  /**
   * Creates an output that buffers the given number of bytes.
   *
   * @param bufferSize the size of the buffer in bytes, must be positive
   */
  public BufferedFileChannelOutput(final FileChannel channel, final int bufferSize) {
    this.channel =
        Objects.requireNonNull(
            channel, StorageEngineMessages.EXCEPTION_CHANNEL_CANNOT_BE_NULL_0F79E0FB);
    if (bufferSize <= 0) {
      throw new IllegalArgumentException(
          DataNodeMiscMessages.EXCEPTION_BUFFER_SIZE_SHOULD_BE_GREATER_THAN_0_BB68B5C4);
    }
    this.bufferSize = bufferSize;
    this.buffer = new byte[bufferSize];
    this.bufferView = ByteBuffer.wrap(buffer);
  }

  @Override
  public void write(final int b) throws IOException {
    write((byte) b);
  }

  @Override
  public void write(final byte b) throws IOException {
    if (bufferLength == bufferSize) {
      flushBuffer();
    }
    buffer[bufferLength++] = b;
  }

  @Override
  public void write(final byte[] bytes) throws IOException {
    write(bytes, 0, bytes.length);
  }

  @Override
  public void write(final byte[] bytes, final int offset, final int length) throws IOException {
    Objects.checkFromIndexSize(offset, length, bytes.length);
    if (length == 0) {
      return;
    }
    if (length >= bufferSize) {
      // A block at least as large as the buffer is not worth a copy.
      flushBuffer();
      writeDirect(bytes, offset, length);
      return;
    }
    if (length > bufferSize - bufferLength) {
      flushBuffer();
    }
    System.arraycopy(bytes, offset, buffer, bufferLength, length);
    bufferLength += length;
  }

  @Override
  public void write(final ByteBuffer src) throws IOException {
    final int length = src.remaining();
    if (length == 0) {
      return;
    }
    if (length >= bufferSize) {
      flushBuffer();
      writeFully(src);
      return;
    }
    if (length > bufferSize - bufferLength) {
      flushBuffer();
    }
    src.get(buffer, bufferLength, length);
    bufferLength += length;
  }

  private void writeDirect(final byte[] bytes, final int offset, final int length)
      throws IOException {
    writeFully(ByteBuffer.wrap(bytes, offset, length));
  }

  /** Hands the buffered bytes to the channel, keeping the previous writes in order. */
  private void flushBuffer() throws IOException {
    if (bufferLength == 0) {
      return;
    }
    bufferView.position(0).limit(bufferLength);
    writeFully(bufferView);
    bufferLength = 0;
  }

  private void writeFully(final ByteBuffer src) throws IOException {
    while (src.hasRemaining()) {
      channel.write(src);
    }
  }

  @Override
  public long getPosition() throws IOException {
    // The buffered bytes are already logically written, so a position query must not flush them.
    return channel.position() + bufferLength;
  }

  /** Moves the output to the given absolute position, flushing first when the position changes. */
  void position(final long offset) throws IOException {
    if (offset == channel.position() + bufferLength) {
      return;
    }
    flushBuffer();
    channel.position(offset);
  }

  @Override
  public OutputStream wrapAsStream() {
    return this;
  }

  @Override
  public void flush() throws IOException {
    flushBuffer();
  }

  @Override
  public void force() throws IOException {
    flushBuffer();
    // Pass false to fsync file content without blocking on OS metadata modifications
    channel.force(false);
  }

  @Override
  public void truncate(final long size) throws IOException {
    flushBuffer();
    channel.truncate(size);
  }

  @Override
  public void close() throws IOException {
    try {
      flushBuffer();
    } finally {
      channel.close();
    }
  }
}
