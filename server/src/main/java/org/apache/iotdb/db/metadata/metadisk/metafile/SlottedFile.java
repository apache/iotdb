package org.apache.iotdb.db.metadata.metadisk.metafile;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class SlottedFile implements SlottedFileAccess {

  private final RandomAccessFile file;
  private final FileChannel channel;

  private final int headerLength;

  private long currentBlockPosition = -1;
  private final ByteBuffer blockBuffer;
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
    blockBuffer = ByteBuffer.allocate(this.blockSize);
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
  public ByteBuffer readHeader() throws IOException {
    ByteBuffer buffer = ByteBuffer.allocate(headerLength);
    channel.position(0);
    channel.read(buffer);
    buffer.flip();
    return buffer;
  }

  @Override
  public void writeHeader(ByteBuffer buffer) throws IOException {
    if (buffer.limit() - buffer.position() != headerLength) {
      throw new IOException("wrong format header");
    }
    channel.position(0);
    channel.write(buffer);
  }

  @Override
  public ByteBuffer readBytes(long position, int length) throws IOException {
    ByteBuffer buffer = ByteBuffer.allocate(length);
    readBytes(position, buffer);
    return buffer;
  }

  @Override
  public void readBytes(long position, ByteBuffer byteBuffer) throws IOException {
    long endPosition = position + (byteBuffer.limit() - byteBuffer.position());

    // if the blockBuffer is empty or the target position is not in current block, read the
    // according block
    if (currentBlockPosition == -1
        || position < currentBlockPosition
        || position >= currentBlockPosition + blockSize) {
      // get target block position
      currentBlockPosition = ((position - headerLength) & blockMask) + headerLength;
      channel.position(currentBlockPosition);
      // prepare blockBuffer for file read, set the position to 0 and limit to bufferSize
      blockBuffer.position(0);
      blockBuffer.limit(blockSize);
      // read data from file
      channel.read(blockBuffer);
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
      channel.position(currentBlockPosition);
      blockBuffer.position(0);
      blockBuffer.limit(blockSize);
      channel.read(blockBuffer);

      // read the rest target data to buffer
      blockBuffer.position(0);
      blockBuffer.limit(Math.min(byteBuffer.limit() - byteBuffer.position(), blockSize));
      byteBuffer.put(blockBuffer);
    }
    blockBuffer.position(0);
    blockBuffer.limit(blockSize);
    byteBuffer.flip();
  }

  @Override
  public void writeBytes(long position, ByteBuffer byteBuffer) throws IOException {
    if (currentBlockPosition != -1 // initial state, empty blockBuffer
        && position >= currentBlockPosition
        && position < currentBlockPosition + blockSize) {
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
