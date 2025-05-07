package org.apache.iotdb.session.compress;

import org.apache.tsfile.compress.ICompressor;
import org.apache.tsfile.file.metadata.enums.CompressionType;

import java.io.IOException;
import java.nio.ByteBuffer;

public class RpcCompressor {

  public static ICompressor compressor;

  public RpcCompressor(CompressionType name) {
    compressor = ICompressor.getCompressor(name);
  }

  public byte[] compress(byte[] data) throws IOException {
    return compressor.compress(data);
  }

  public byte[] compress(byte[] data, int offset, int length) throws IOException {
    return compressor.compress(data, offset, length);
  }

  public int compress(byte[] data, int offset, int length, byte[] compressed) throws IOException {
    return compressor.compress(data, offset, length, compressed);
  }

  /**
   * Compress ByteBuffer, this method is better for longer data
   *
   * @return byte length of compressed data.
   */
  public int compress(ByteBuffer data, ByteBuffer compressed) throws IOException {
    return compressor.compress(data, compressed);
  }

  public int getMaxBytesForCompression(int uncompressedDataSize) {
    return compressor.getMaxBytesForCompression(uncompressedDataSize);
  }

  public CompressionType getType() {
    return compressor.getType();
  }
}
