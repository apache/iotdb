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

package org.apache.iotdb.tsfile.compress.auto;

import org.apache.iotdb.tsfile.compress.IUnCompressor;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;

import java.io.IOException;
import java.nio.ByteBuffer;

public class AutoUncompressor implements IUnCompressor {

  @Override
  public int getUncompressedLength(byte[] array, int offset, int length) throws IOException {
    byte realType = array[offset + length - 1];
    IUnCompressor unCompressor =
        IUnCompressor.getUnCompressor(CompressionType.deserialize(realType));
    return unCompressor.getUncompressedLength(array, offset, length);
  }

  @Override
  public int getUncompressedLength(ByteBuffer buffer) throws IOException {
    byte realType = buffer.array()[buffer.position() + buffer.remaining() - 1];
    IUnCompressor unCompressor =
        IUnCompressor.getUnCompressor(CompressionType.deserialize(realType));
    return unCompressor.getUncompressedLength(buffer);
  }

  @Override
  public byte[] uncompress(byte[] byteArray) throws IOException {
    byte realType = byteArray[byteArray.length - 1];
    IUnCompressor unCompressor =
        IUnCompressor.getUnCompressor(CompressionType.deserialize(realType));
    return unCompressor.uncompress(byteArray);
  }

  @Override
  public int uncompress(byte[] byteArray, int offset, int length, byte[] output, int outOffset)
      throws IOException {
    byte realType = byteArray[offset + length - 1];
    IUnCompressor unCompressor =
        IUnCompressor.getUnCompressor(CompressionType.deserialize(realType));
    return unCompressor.uncompress(byteArray, offset, length, output, outOffset);
  }

  @Override
  public int uncompress(ByteBuffer compressed, ByteBuffer uncompressed) throws IOException {
    byte realType = compressed.array()[compressed.position() + compressed.remaining() - 1];
    IUnCompressor unCompressor =
        IUnCompressor.getUnCompressor(CompressionType.deserialize(realType));
    return unCompressor.uncompress(compressed, uncompressed);
  }

  @Override
  public CompressionType getCodecName() {
    return null;
  }
}
