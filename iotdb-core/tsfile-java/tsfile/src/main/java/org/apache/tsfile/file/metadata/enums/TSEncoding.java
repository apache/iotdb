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

package org.apache.tsfile.file.metadata.enums;

import org.apache.tsfile.enums.TSDataType;

import java.util.EnumMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

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
  @Deprecated
  FREQ((byte) 10),
  CHIMP((byte) 11),
  SPRINTZ((byte) 12),
  RLBE((byte) 13);
  private final byte type;

  @SuppressWarnings("java:S2386") // used by other projects
  public static final Map<TSDataType, Set<TSEncoding>> TYPE_SUPPORTED_ENCODINGS =
      new EnumMap<>(TSDataType.class);

  static {
    Set<TSEncoding> booleanSet = new HashSet<>();
    booleanSet.add(TSEncoding.PLAIN);
    booleanSet.add(TSEncoding.RLE);
    TYPE_SUPPORTED_ENCODINGS.put(TSDataType.BOOLEAN, booleanSet);

    Set<TSEncoding> intSet = new HashSet<>();
    intSet.add(TSEncoding.PLAIN);
    intSet.add(TSEncoding.RLE);
    intSet.add(TSEncoding.TS_2DIFF);
    intSet.add(TSEncoding.GORILLA);
    intSet.add(TSEncoding.ZIGZAG);
    intSet.add(TSEncoding.CHIMP);
    intSet.add(TSEncoding.SPRINTZ);
    intSet.add(TSEncoding.RLBE);

    TYPE_SUPPORTED_ENCODINGS.put(TSDataType.INT32, intSet);
    TYPE_SUPPORTED_ENCODINGS.put(TSDataType.INT64, intSet);
    TYPE_SUPPORTED_ENCODINGS.put(TSDataType.TIMESTAMP, intSet);
    TYPE_SUPPORTED_ENCODINGS.put(TSDataType.DATE, intSet);

    Set<TSEncoding> floatSet = new HashSet<>();
    floatSet.add(TSEncoding.PLAIN);
    floatSet.add(TSEncoding.RLE);
    floatSet.add(TSEncoding.TS_2DIFF);
    floatSet.add(TSEncoding.GORILLA_V1);
    floatSet.add(TSEncoding.GORILLA);
    floatSet.add(TSEncoding.CHIMP);
    floatSet.add(TSEncoding.SPRINTZ);
    floatSet.add(TSEncoding.RLBE);

    TYPE_SUPPORTED_ENCODINGS.put(TSDataType.FLOAT, floatSet);
    TYPE_SUPPORTED_ENCODINGS.put(TSDataType.DOUBLE, floatSet);

    Set<TSEncoding> textSet = new HashSet<>();
    textSet.add(TSEncoding.PLAIN);
    textSet.add(TSEncoding.DICTIONARY);
    TYPE_SUPPORTED_ENCODINGS.put(TSDataType.TEXT, textSet);
    TYPE_SUPPORTED_ENCODINGS.put(TSDataType.STRING, textSet);

    Set<TSEncoding> blobSet = new HashSet<>();
    blobSet.add(TSEncoding.PLAIN);
    TYPE_SUPPORTED_ENCODINGS.put(TSDataType.BLOB, blobSet);
  }

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
      case 11:
        return TSEncoding.CHIMP;
      case 12:
        return TSEncoding.SPRINTZ;
      case 13:
        return TSEncoding.RLBE;
      default:
        throw new IllegalArgumentException("Invalid input: " + encoding);
    }
  }

  public static boolean isSupported(TSDataType type, TSEncoding encoding) {
    return TYPE_SUPPORTED_ENCODINGS.get(type).contains(encoding);
  }

  public boolean isSupported(TSDataType type) {
    return TYPE_SUPPORTED_ENCODINGS.get(type).contains(this);
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
