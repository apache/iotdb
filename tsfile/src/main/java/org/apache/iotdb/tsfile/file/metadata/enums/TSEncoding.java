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

public enum TSEncoding {
  PLAIN((byte) 0),
  DICTIONARY((byte) 1),
  RLE((byte) 2),
  DIFF((byte) 3),
  TS_2DIFF((byte) 4),
  BITMAP((byte) 5),
  GORILLA_V1((byte) 6),
  REGULAR((byte) 7),
  GORILLA((byte) 8),
  ZIGZAG((byte) 9),
  FREQ((byte) 10),
  CHIMP((byte) 11);

  private final byte type;

  TSEncoding(byte type) {
    this.type = type;
  }

  /**
   * judge the encoding deserialize type.
   *
   * @param encoding -use to determine encoding type
   * @return -encoding type
   */
  public static TSEncoding deserialize(byte encoding) {
    return getTsEncoding(encoding);
  }

  private static TSEncoding getTsEncoding(byte encoding) {
    switch (encoding) {
      case 0:
        return TSEncoding.PLAIN;
      case 1:
        return TSEncoding.DICTIONARY;
      case 2:
        return TSEncoding.RLE;
      case 3:
        return TSEncoding.DIFF;
      case 4:
        return TSEncoding.TS_2DIFF;
      case 5:
        return TSEncoding.BITMAP;
      case 6:
        return TSEncoding.GORILLA_V1;
      case 7:
        return TSEncoding.REGULAR;
      case 8:
        return TSEncoding.GORILLA;
      case 9:
        return TSEncoding.ZIGZAG;
      case 10:
        return TSEncoding.FREQ;
      case 11:
        return TSEncoding.CHIMP;
      default:
        throw new IllegalArgumentException("Invalid input: " + encoding);
    }
  }

  public static int getSerializedSize() {
    return Byte.BYTES;
  }

  /**
   * judge the encoding deserialize type.
   *
   * @return -encoding type
   */
  public byte serialize() {
    return type;
  }
}
