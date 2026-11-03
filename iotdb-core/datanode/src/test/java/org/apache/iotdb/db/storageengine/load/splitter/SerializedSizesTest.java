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
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

/** The counted size of a string is compared with the bytes its encoder writes. */
public class SerializedSizesTest {

  @Test
  public void testTheCountedSizeIsWhatTheEncoderWrites() {
    for (final String value : values()) {
      Assert.assertEquals(
          "the counted size of \"" + value + "\" must be the size its encoder writes",
          value.getBytes(StandardCharsets.UTF_8).length,
          SerializedSizes.encodedLength(value));
      Assert.assertEquals(
          "the counted size of \"" + value + "\" must be its header and encoded value",
          ReadWriteIOUtils.sizeToWrite(value),
          SerializedSizes.utf8Size(value));
    }
  }

  @Test
  public void testANullValueOnlyTakesItsLengthHeader() {
    Assert.assertEquals(Integer.BYTES, SerializedSizes.utf8Size(null));
    Assert.assertEquals(0, SerializedSizes.encodedLength(null));
  }

  /** ASCII, every length of an encoded character, and the characters an encoder has to replace. */
  private static List<String> values() {
    return Arrays.asList(
        "",
        "a",
        "file.tsfile",
        "task-1/root.sg-0-0.tsfile",
        "\u00e9", // two bytes
        "\u03c4\u03b1",
        "\u4efb\u52a1/\u5206\u533a.tsfile", // three bytes
        "\uD83D\uDE00", // four bytes, a surrogate pair
        "a\u4e2d\uD83D\uDE00b",
        "a\uD83D", // a surrogate without its pair
        "\uDE00",
        "\uD83D\uD83D",
        "a\uDE00b");
  }
}
