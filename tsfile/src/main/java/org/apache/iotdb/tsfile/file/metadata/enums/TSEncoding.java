/**
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

  PLAIN, PLAIN_DICTIONARY, RLE, DIFF, TS_2DIFF, BITMAP, GORILLA;

  /**
   * judge the encoding deserialize type.
   *
   * @param i -use to determine encoding type
   * @return -encoding type
   */
  public static TSEncoding deserialize(short i) {
    switch (i) {
      case 0:
        return PLAIN;
      case 1:
        return PLAIN_DICTIONARY;
      case 2:
        return RLE;
      case 3:
        return DIFF;
      case 4:
        return TS_2DIFF;
      case 5:
        return BITMAP;
      case 6:
        return GORILLA;
      default:
        return PLAIN;
    }
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
    switch (this) {
      case PLAIN:
        return 0;
      case PLAIN_DICTIONARY:
        return 1;
      case RLE:
        return 2;
      case DIFF:
        return 3;
      case TS_2DIFF:
        return 4;
      case BITMAP:
        return 5;
      case GORILLA:
        return 6;
      default:
        return 0;
    }
  }
}
