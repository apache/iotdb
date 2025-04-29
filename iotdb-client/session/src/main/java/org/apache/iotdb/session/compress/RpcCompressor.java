package org.apache.iotdb.session.compress;

import org.apache.tsfile.compress.ICompressor;
import org.apache.tsfile.file.metadata.enums.CompressionType;

import java.io.IOException;
import java.nio.ByteBuffer;

// 为后续的压缩器提供一个基础的实现
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
   * 压缩 ByteBuffer, 较长的数据使用这种方式更好
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
