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
  UNCOMPRESSED, SNAPPY, GZIP, LZO, SDT, PAA, PLA, LZ4;

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
    if (compressor >= 8 || compressor < 0) {
      throw new IllegalArgumentException("Invalid input: " + compressor);
    }
    return (byte) compressor;
  }


  private static CompressionType getCompressionType(short compressor) {
    if (compressor >= 8 || compressor < 0) {
      throw new IllegalArgumentException("Invalid input: " + compressor);
    }
    switch (compressor) {
      case 1:
        return SNAPPY;
      case 2:
        return GZIP;
      case 3:
        return LZO;
      case 4:
        return SDT;
      case 5:
        return PAA;
      case 6:
        return PLA;
      case 7:
        return LZ4;
      default:
        return UNCOMPRESSED;
    }
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
    switch (this) {
      case SNAPPY:
        return 1;
      case GZIP:
        return 2;
      case LZO:
        return 3;
      case SDT:
        return 4;
      case PAA:
        return 5;
      case PLA:
        return 6;
      case LZ4:
        return 7;
      default:
        return 0;
    }
  }

  /**
   * get extension.
   *
   * @return extension (string type), for example: .snappy, .gz, .lzo
   */
  public String getExtension() {
    switch (this) {
      case SNAPPY:
        return ".snappy";
      case GZIP:
        return ".gz";
      case LZO:
        return ".lzo";
      case SDT:
        return ".sdt";
      case PAA:
        return ".paa";
      case PLA:
        return ".pla";
      case LZ4:
        return ".lz4";
      default:
        return "";
    }
  }

}
