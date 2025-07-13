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

package org.apache.iotdb.rpc;

import org.apache.tsfile.compress.ICompressor;
import org.apache.tsfile.compress.IUnCompressor;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

public class SerdeUtils {
  private static final long COMPRESS_THRESHOLD = 128L * 1024;

  private SerdeUtils() {}

  public static byte[] serialize(List<float[]> obj, CompressionType compressionType)
      throws IOException {
    long count = 0;
    for (float[] array : obj) {
      count += array.length;
    }
    // compress type + uncompress size + content
    long valueSize = count * Float.BYTES;
    long size = Byte.BYTES + Integer.BYTES + valueSize;
    if (size > Integer.MAX_VALUE) {
      throw new IllegalArgumentException("Read object size is too large (size > 2G)");
    }

    if (valueSize < COMPRESS_THRESHOLD) {
      compressionType = CompressionType.UNCOMPRESSED;
      ByteBuffer res = ByteBuffer.allocate((int) size);
      ReadWriteIOUtils.write(compressionType.serialize(), res);
      ReadWriteIOUtils.write((int) valueSize, res);
      writeFloatArray(res, obj);
      return res.array();
    } else {
      ByteBuffer byteBuffer = ByteBuffer.allocate((int) valueSize);
      writeFloatArray(byteBuffer, obj);
      ICompressor compressor = ICompressor.getCompressor(compressionType);
      byte[] compressedValue = compressor.compress(byteBuffer.array());

      long compressedSize = (long) Byte.BYTES + Integer.BYTES + compressedValue.length;
      if (compressedSize > Integer.MAX_VALUE) {
        throw new IllegalArgumentException("Read object size is too large (size > 2G)");
      }
      ByteBuffer res = ByteBuffer.allocate((int) compressedSize);

      ReadWriteIOUtils.write(compressionType.serialize(), res);
      ReadWriteIOUtils.write((int) valueSize, res);
      res.put(compressedValue);
      return res.array();
    }
  }

  private static void writeFloatArray(ByteBuffer byteBuffer, List<float[]> values) {
    for (float[] array : values) {
      for (float value : array) {
        ReadWriteIOUtils.write(value, byteBuffer);
      }
    }
  }

  public static float[] deserialize(byte[] bytes) {
    CompressionType compressionType = CompressionType.deserialize(bytes[0]);
    int unCompressedSize = ByteBuffer.wrap(bytes, 1, 4).getInt();
    byte[] unCompressedBytes = new byte[unCompressedSize];
    IUnCompressor unCompressor = IUnCompressor.getUnCompressor(compressionType);
    try {
      unCompressor.uncompress(bytes, 5, bytes.length - 5, unCompressedBytes, 0);
      ByteBuffer byteBuffer = ByteBuffer.wrap(unCompressedBytes);
      float[] floats = new float[byteBuffer.limit() / Float.BYTES];
      for (int i = 0; i < floats.length; i++) {
        floats[i] = byteBuffer.getFloat();
      }
      return floats;
    } catch (Exception e) {
      throw new IllegalStateException("Failed to uncompress.", e);
    }
  }
}
