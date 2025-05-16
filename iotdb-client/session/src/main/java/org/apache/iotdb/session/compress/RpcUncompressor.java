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
