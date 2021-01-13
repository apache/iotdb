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
  /**
   * Do not comprocess
   */
  UNCOMPRESSED("", 0),

  /**
   * SNAPPY
   */
  SNAPPY(".snappy", 1),

  /**
   * GZIP
   */
  GZIP(".gzip", 2),

  /**
   * LZO
   */
  LZO(".lzo", 3),

  /**
   * SDT
   */
  SDT(".sdt", 4),

  /**
   * PAA
   */
  PAA(".paa", 5),

  /**
   * PLA
   */
  PLA(".pla", 6),

  /**
   * LZ4
   */
  LZ4(".lz4", 7);

  private final String extensionName;
  private final int index;

  CompressionType(String extensionName, int index) {
    this.extensionName = extensionName;
    this.index = index;
  }

  /**
   * deserialize short number.
   *
   * @param compressor short number
   * @return CompressionType
   */
  public static CompressionType deserialize(short compressor) {
    return getCompressionType(compressor);
  }

  public static byte deserializeToByte(short compressor) {
    //check compressor is valid
    getCompressionType(compressor);
    return (byte) compressor;
  }


  private static CompressionType getCompressionType(short compressor) {
    for (CompressionType compressionType : CompressionType.values()) {
      if (compressor == compressionType.index) {
        return compressionType;
      }
    }

    throw new IllegalArgumentException("Invalid input: " + compressor);
  }

  /**
   * give an byte to return a compression type.
   *
   * @param compressor byte number
   * @return CompressionType
   */
  public static CompressionType byteToEnum(byte compressor) {
    return getCompressionType(compressor);
  }

  public static int getSerializedSize() {
    return Short.BYTES;
  }

  /**
   * serialize.
   *
   * @return short number
   */
  public short serialize() {
    return enumToByte();
  }

  /**
   * @return byte number
   */
  public byte enumToByte() {
    return (byte) index;
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
