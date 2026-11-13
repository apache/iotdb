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

package org.apache.iotdb.pipe.api.type;

import org.junit.Assert;
import org.junit.Test;

public class BinaryTest {

  @Test
  public void compareToTreatsBytesAsUnsigned() {
    // 0x41 ('A') is 65, 0x80 is 128 when read as an unsigned byte. The one with the larger
    // unsigned value must sort after the smaller one.
    final Binary low = new Binary(new byte[] {(byte) 0x41});
    final Binary high = new Binary(new byte[] {(byte) 0x80});

    Assert.assertTrue(low.compareTo(high) < 0);
    Assert.assertTrue(high.compareTo(low) > 0);
  }

  @Test
  public void compareToMatchesStringOrderingForNonAsciiText() {
    // "é" is encoded as the two bytes 0xC3 0xA9 in UTF-8, so its first byte (195 unsigned) is
    // greater than 'A' (65). The ordering must agree with java.lang.String ordering.
    final Binary a = new Binary("A");
    final Binary e = new Binary("é");

    Assert.assertEquals("A".compareTo("é") < 0, a.compareTo(e) < 0);
    Assert.assertTrue(a.compareTo(e) < 0);
  }

  @Test
  public void compareToKeepsAsciiOrdering() {
    Assert.assertTrue(new Binary("A").compareTo(new Binary("B")) < 0);
    Assert.assertTrue(new Binary("abc").compareTo(new Binary("abd")) < 0);
    Assert.assertEquals(0, new Binary("abc").compareTo(new Binary("abc")));
  }

  @Test
  public void compareToUsesLengthWhenOneIsPrefixOfTheOther() {
    Assert.assertTrue(new Binary("ab").compareTo(new Binary("abc")) < 0);
    Assert.assertTrue(new Binary("abc").compareTo(new Binary("ab")) > 0);
  }
}
