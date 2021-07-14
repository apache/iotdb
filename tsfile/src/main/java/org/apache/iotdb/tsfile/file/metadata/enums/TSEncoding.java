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

import java.util.HashMap;
import java.util.Map;

public enum TSEncoding {
  PLAIN((byte) 0),
  DICTIONARY((byte) 1),
  RLE((byte) 2),
  DIFF((byte) 3),
  TS_2DIFF((byte) 4),
  BITMAP((byte) 5),
  GORILLA_V1((byte) 6),
  REGULAR((byte) 7),
  GORILLA((byte) 8);

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

  private static final Map<Byte, TSEncoding> map = new HashMap<>();

  static {
    TSEncoding[] array = TSEncoding.values();
    for (TSEncoding e : array) {
      map.put(e.type, e);
    }
  }

  private static TSEncoding getTsEncoding(byte encoding) {
    TSEncoding ret = map.get(encoding);
    if (ret == null) {
      throw new IllegalArgumentException("Invalid input: " + encoding);
    }
    return ret;
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
