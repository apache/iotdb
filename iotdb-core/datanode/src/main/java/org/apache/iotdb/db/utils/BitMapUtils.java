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

package org.apache.iotdb.db.utils;

import org.apache.tsfile.utils.BitMap;

import java.util.Objects;

public final class BitMapUtils {

  private BitMapUtils() {}

  public static BitMap[] compactBitMaps(final BitMap[] bitMaps, final int rowCount) {
    if (Objects.isNull(bitMaps)) {
      return null;
    }

    boolean hasMarkedBitMap = false;
    for (int i = 0; i < bitMaps.length; ++i) {
      if (Objects.nonNull(bitMaps[i]) && isAllUnmarked(bitMaps[i], rowCount)) {
        bitMaps[i] = null;
      }
      if (Objects.nonNull(bitMaps[i])) {
        hasMarkedBitMap = true;
      }
    }
    return hasMarkedBitMap ? bitMaps : null;
  }

  private static boolean isAllUnmarked(final BitMap bitMap, final int rowCount) {
    final int checkedSize = Math.min(rowCount, bitMap.getSize());
    if (checkedSize <= 0) {
      return true;
    }

    final byte[] bytes = bitMap.getByteArray();
    final int fullByteCount = checkedSize / Byte.SIZE;
    for (int i = 0; i < fullByteCount; ++i) {
      if (bytes[i] != 0) {
        return false;
      }
    }

    final int remainingBitCount = checkedSize % Byte.SIZE;
    return remainingBitCount == 0
        || (bytes[fullByteCount] & (0xFF << (Byte.SIZE - remainingBitCount))) == 0;
  }
}
