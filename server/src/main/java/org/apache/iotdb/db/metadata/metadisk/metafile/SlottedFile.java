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
package org.apache.iotdb.db.metadata.metadisk.metafile;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class SlottedFile implements ISlottedFileAccess {

  private final RandomAccessFile file;
  private final FileChannel channel;

  private final int headerLength;

  private volatile long currentBlockPosition = -1;
  private final byte[] blockData;
  private final int blockScale;
  private final int blockSize;
  private final long blockMask;

  public SlottedFile(String filepath, int headerLength, int blockSize) throws IOException {
    File metaFile = new File(filepath);
    file = new RandomAccessFile(metaFile, "rw");
    channel = file.getChannel();

    this.headerLength = headerLength;

    blockScale = (int) Math.ceil(Math.log(blockSize));
    this.blockSize = 1 << blockScale;
    blockMask = 0xffffffffffffffffL << blockScale;
    blockData = new byte[this.blockSize];
  }

  @Override
  public long getFileLength() throws IOException {
    return file.length();
  }

  @Override
  public int getHeaderLength() {
    return headerLength;
  }

  @Override
  public int getBlockSize() {
    return blockSize;
  }

  @Override
  public synchronized ByteBuffer readHeader() throws IOException {
    ByteBuffer buffer = ByteBuffer.allocate(headerLength);
    fileRead(0, buffer);
    buffer.flip();
    return buffer;
  }

  @Override
  public void writeHeader(ByteBuffer buffer) throws IOException {
    if (buffer.limit() - buffer.position() != headerLength) {
      throw new IOException("wrong format header");
    }
    fileWrite(0, buffer);
  }

  @Override
  public ByteBuffer readBytes(long position, int length) throws IOException {
    ByteBuffer buffer = ByteBuffer.allocate(length);
    readBytes(position, buffer);
    return buffer;
  }

  @Override
  public void readBytes(long position, ByteBuffer byteBuffer) throws IOException {
    ByteBuffer blockBuffer = ByteBuffer.wrap(blockData);
    synchronized (blockData) {
      long endPosition = position + (byteBuffer.limit() - byteBuffer.position());

      // if the blockBuffer is empty or the target position is not in current block, read the
      // according block
      if (currentBlockPosition == -1
          || position < currentBlockPosition
          || position >= currentBlockPosition + blockSize) {
        // get target block position
        currentBlockPosition = ((position - headerLength) & blockMask) + headerLength;
        // prepare blockBuffer for file read, set the position to 0 and limit to bufferSize
        blockBuffer.position(0);
        blockBuffer.limit(blockSize);
        // read data from file
        fileRead(currentBlockPosition, blockBuffer);
      }

      // read data from blockBuffer to buffer
      blockBuffer.position((int) (position - currentBlockPosition));
      blockBuffer.limit(
          Math.min(byteBuffer.limit() - byteBuffer.position(), blockSize - blockBuffer.position()));
      byteBuffer.put(blockBuffer);

      // case the target data exist in several block
      while (endPosition >= currentBlockPosition + blockSize) {
        // read next block from file
        currentBlockPosition += blockSize;
        blockBuffer.position(0);
        blockBuffer.limit(blockSize);
        fileRead(currentBlockPosition, blockBuffer);

        // read the rest target data to buffer
        blockBuffer.position(0);
        blockBuffer.limit(Math.min(byteBuffer.limit() - byteBuffer.position(), blockSize));
        byteBuffer.put(blockBuffer);
      }
      blockBuffer.position(0);
      blockBuffer.limit(blockSize);
      byteBuffer.flip();
    }
  }

  @Override
  public void writeBytes(long position, ByteBuffer byteBuffer) throws IOException {
    synchronized (blockData) {
      if (currentBlockPosition != -1 // initial state, empty blockBuffer
          && position >= currentBlockPosition
          && position < currentBlockPosition + blockSize) {
        ByteBuffer blockBuffer = ByteBuffer.wrap(blockData);
        // update the blockBuffer to the updated data
        blockBuffer.position((int) (position - currentBlockPosition));

        // record the origin state of byteBuffer
        int limit = byteBuffer.limit();
        byteBuffer.mark();

        // write data
        byteBuffer.limit(Math.min(byteBuffer.limit(), blockSize - blockBuffer.position()));
        blockBuffer.put(byteBuffer);

        // recover the byteBuffer attribute for file write
        byteBuffer.reset();
        byteBuffer.limit(limit);
      }
      fileWrite(position, byteBuffer);
    }
  }

  private void fileRead(long position, ByteBuffer byteBuffer) throws IOException {
    channel.position(position);
    channel.read(byteBuffer);
  }

  private void fileWrite(long position, ByteBuffer byteBuffer) throws IOException {
    channel.position(position);
    while (byteBuffer.hasRemaining()) {
      channel.write(byteBuffer);
    }
  }

  @Override
  public void sync() throws IOException {
    channel.force(false);
  }

  @Override
  public void close() throws IOException {
    sync();
    channel.close();
    file.close();
  }
}
