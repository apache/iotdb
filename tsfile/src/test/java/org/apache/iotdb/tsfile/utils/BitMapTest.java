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
package org.apache.iotdb.tsfile.utils;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class BitMapTest {

  @Test
  public void testMarkAndUnMark() {
    BitMap bitmap = new BitMap(100);
    assertEquals(100, bitmap.getSize());
    assertTrue(bitmap.isAllUnmarked());
    assertFalse(bitmap.isAllMarked());
    for (int i = 0; i < 100; i++) {
      bitmap.mark(i);
      assertTrue(bitmap.isMarked(i));
      if (i == 50) {
        assertFalse(bitmap.isAllMarked());
        assertFalse(bitmap.isAllUnmarked());
      }
    }
    assertTrue(bitmap.isAllMarked());
    assertFalse(bitmap.isAllUnmarked());
    for (int i = 0; i < 100; i++) {
      bitmap.unmark(i);
      assertFalse(bitmap.isMarked(i));
    }
    assertTrue(bitmap.isAllUnmarked());
    assertFalse(bitmap.isAllMarked());
  }

  @Test
  public void testInitFromBytes() {
    BitMap bitmap1 = new BitMap(100);
    for (int i = 0; i < 100; i++) {
      if (i % 2 == 0) {
        bitmap1.mark(i);
      }
    }
    BitMap bitmap2 = new BitMap(bitmap1.getSize(), bitmap1.getByteArray());
    assertEquals(100, bitmap2.getSize());
    for (int i = 0; i < 100; i++) {
      assertEquals(bitmap1.isMarked(i), bitmap2.isMarked(i));
    }
  }
}
