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

import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

public class TSEncodingV2 {

  private TSEncodingV2() {
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

  private static TSEncoding getTsEncoding(short encoding) {
    if (encoding < 0 || 8 < encoding) {
      throw new IllegalArgumentException("Invalid input: " + encoding);
    }
    switch (encoding) {
      case 1:
        return TSEncoding.PLAIN_DICTIONARY;
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
      default:
        return TSEncoding.PLAIN;
    }
  }
}
