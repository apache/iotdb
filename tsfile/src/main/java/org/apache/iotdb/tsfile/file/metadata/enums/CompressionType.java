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
package org.apache.iotdb.tsfile.file.metadata.enums;

public enum CompressionType {
  /** Do not comprocess */
  UNCOMPRESSED("", (byte) 0),

  /** SNAPPY */
  SNAPPY(".snappy", (byte) 1),

  /** GZIP */
  GZIP(".gzip", (byte) 2),

  /** LZ4 */
  // NOTICE: To ensure the compatibility of existing files, do not change the byte LZ4 binds to.
  LZ4(".lz4", (byte) 7),
  /** ZSTD */
  ZSTD(".zstd", (byte) 8);

  private final String extensionName;
  private final byte index;

  CompressionType(String extensionName, byte index) {
    this.extensionName = extensionName;
    this.index = index;
  }

  /**
   * deserialize byte number.
   *
   * @param compressor byte number
   * @return CompressionType
   */
  public static CompressionType deserialize(byte compressor) {
    switch (compressor) {
      case 0:
        return CompressionType.UNCOMPRESSED;
      case 1:
        return CompressionType.SNAPPY;
      case 2:
        return CompressionType.GZIP;
      case 7:
        return CompressionType.LZ4;
      case 8:
        return CompressionType.ZSTD;
      default:
        throw new IllegalArgumentException("Invalid input: " + compressor);
    }
  }

  public static int getSerializedSize() {
    return Byte.BYTES;
  }

  /** @return byte number */
  public byte serialize() {
    return this.index;
  }

  /**
   * get extension.
   *
   * @return extension (string type), for example: .snappy, .gz, .lzo
   */
  public String getExtension() {
    return extensionName;
  }
}
