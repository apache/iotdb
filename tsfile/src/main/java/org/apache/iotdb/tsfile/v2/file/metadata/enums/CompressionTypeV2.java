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
package org.apache.iotdb.tsfile.v2.file.metadata.enums;

import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;

public class CompressionTypeV2 {

  /**
   * deserialize short number.
   *
   * @param compressor short number
   * @return CompressionType
   */
  public static CompressionType deserialize(short compressor) {
    return getCompressionType(compressor);
  }

  private static CompressionType getCompressionType(short compressor) {
    if (compressor >= 8 || compressor < 0) {
      throw new IllegalArgumentException("Invalid input: " + compressor);
    }
    switch (compressor) {
      case 1:
        return CompressionType.SNAPPY;
      case 2:
        return CompressionType.GZIP;
      case 3:
        return CompressionType.LZO;
      case 4:
        return CompressionType.SDT;
      case 5:
        return CompressionType.PAA;
      case 6:
        return CompressionType.PLA;
      case 7:
        return CompressionType.LZ4;
      default:
        return CompressionType.UNCOMPRESSED;
    }
  }
}
