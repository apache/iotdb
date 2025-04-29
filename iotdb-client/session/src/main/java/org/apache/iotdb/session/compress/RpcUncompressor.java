package org.apache.iotdb.session.compress;

import org.apache.tsfile.compress.IUnCompressor;
import org.apache.tsfile.file.metadata.enums.CompressionType;

import java.io.IOException;
import java.nio.ByteBuffer;

public class RpcUncompressor {
  public static IUnCompressor unCompressor;

  public RpcUncompressor(CompressionType name) {
    unCompressor = IUnCompressor.getUnCompressor(name);
  }

  public int getUncompressedLength(byte[] array, int offset, int length) throws IOException {
    return unCompressor.getUncompressedLength(array, offset, length);
  }

  public int getUncompressedLength(ByteBuffer buffer) throws IOException {
    return unCompressor.getUncompressedLength(buffer);
  }

  public byte[] uncompress(byte[] byteArray) throws IOException {
    return unCompressor.uncompress(byteArray);
  }

  public int uncompress(byte[] byteArray, int offset, int length, byte[] output, int outOffset)
      throws IOException {
    return unCompressor.uncompress(byteArray, offset, length, output, outOffset);
  }

  public int uncompress(ByteBuffer compressed, ByteBuffer uncompressed) throws IOException {
    return unCompressor.uncompress(compressed, uncompressed);
  }

  public CompressionType getCodecName() {
    return unCompressor.getCodecName();
  }
}
