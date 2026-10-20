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

package org.apache.iotdb.db.storageengine.load.splitter;

import org.apache.tsfile.utils.ReadWriteIOUtils;

/**
 * The sizes of the parts of the staged data wire format, counted rather than measured: the WAL asks
 * a command how large it is, and that answer must not cost a buffer.
 */
public final class SerializedSizes {

  /**
   * The bytes {@link ReadWriteIOUtils#sizeToWrite(String)} counts for {@code value}: the four byte
   * length that precedes it and then its UTF-8 form, or the length alone when it is null.
   */
  public static int utf8Size(final String value) {
    return value == null ? Integer.BYTES : Integer.BYTES + encodedLength(value);
  }

  /**
   * The bytes the UTF-8 form of {@code sequence} takes, counted without encoding it. A character
   * the encoder cannot map, such as a surrogate that is missing its pair, counts as the single
   * replacement byte the encoder writes for it.
   */
  public static int encodedLength(final CharSequence sequence) {
    if (sequence == null) {
      return 0;
    }
    final int length = sequence.length();

    // A staged path or an id is ASCII, and its length is then the number of its characters. Only
    // this leading run is scanned: an ASCII character behind a wider one is counted below.
    int i = 0;
    while (i < length && sequence.charAt(i) < 0x80) {
      i++;
    }
    int utf8Length = i;

    for (; i < length; i++) {
      final char c = sequence.charAt(i);
      if (c < 0x80) {
        utf8Length += 1;
      } else if (c < 0x800) {
        utf8Length += 2;
      } else if (Character.isHighSurrogate(c)
          && i + 1 < length
          && Character.isLowSurrogate(sequence.charAt(i + 1))) {
        utf8Length += 4;
        i++; // the low surrogate is part of the same character
      } else if (Character.isSurrogate(c)) {
        utf8Length += 1;
      } else {
        utf8Length += 3;
      }
    }
    return utf8Length;
  }

  private SerializedSizes() {}
}
