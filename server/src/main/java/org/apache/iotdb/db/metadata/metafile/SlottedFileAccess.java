package org.apache.iotdb.db.metadata.metafile;

import java.io.IOException;
import java.nio.ByteBuffer;

public interface SlottedFileAccess {

  long getFileLength() throws IOException;

  int getHeaderLength();

  int getBlockSize();

  ByteBuffer readHeader() throws IOException;

  void writeHeader(ByteBuffer buffer) throws IOException;

  ByteBuffer readBytes(long position, int length) throws IOException;

  void readBytes(long position, ByteBuffer buffer) throws IOException;

  void writeBytes(long position, ByteBuffer buffer) throws IOException;

  void sync() throws IOException;

  void close() throws IOException;
}
