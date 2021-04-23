package org.apache.iotdb.db.metadata.metadisk.metafile;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * this interface provides operations on a certain file which is consisted of a header and a series
 * of slot/record with fixed length
 *
 * <p>the data is read in form of block which consists of several continuous records
 */
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
