package org.apache.iotdb.tsfile.exception.compress;

public class GZIPCompressOverflowException extends RuntimeException {

  public GZIPCompressOverflowException() {
    super("compressed data is larger than the given byte container.");
  }
}
