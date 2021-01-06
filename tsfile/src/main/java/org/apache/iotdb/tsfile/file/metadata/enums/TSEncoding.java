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

  PLAIN(0),
  PLAIN_DICTIONARY(1),
  RLE(2),
  DIFF(3),
  TS_2DIFF(4),
  BITMAP(5),
  GORILLA_V1(6),
  REGULAR(7),
  GORILLA(8);

  private final int type;

  TSEncoding(int type) {
    this.type = type;
  }

  /**
   * judge the encoding deserialize type.
   *
   * @param encoding -use to determine encoding type
   * @return -encoding type
   */
  public static TSEncoding deserialize(short encoding) {
    return getTsEncoding(encoding);
  }

  public static byte deserializeToByte(short encoding) {
    getTsEncoding(encoding);
    return (byte) encoding;
  }

  private static TSEncoding getTsEncoding(short encoding) {


    for (TSEncoding tsEncoding : TSEncoding.values()) {
      if (encoding == tsEncoding.type) {
        return tsEncoding;
      }
    }

    throw new IllegalArgumentException("Invalid input: " + encoding);
  }

  /**
   * give an byte to return a encoding type.
   *
   * @param encoding byte number
   * @return encoding type
   */
  public static TSEncoding byteToEnum(byte encoding) {
    return getTsEncoding(encoding);
  }

  public static int getSerializedSize() {
    return Short.BYTES;
  }

  /**
   * judge the encoding deserialize type.
   *
   * @return -encoding type
   */
  public short serialize() {
    return enumToByte();
  }

  /**
   * @return byte number
   */
  public byte enumToByte() {
    return (byte) type;
  }
}
