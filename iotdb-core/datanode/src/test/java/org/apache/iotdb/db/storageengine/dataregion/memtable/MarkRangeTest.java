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

package org.apache.iotdb.db.storageengine.dataregion.memtable;

import org.apache.iotdb.db.utils.datastructure.AlignedTVList;

import org.apache.tsfile.utils.BitMap;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class MarkRangeTest {

  @Test
  public void emptyRange() {
    byte[] map = new byte[1];
    AlignedTVList.markRange(map, 0, 0);
    assertEquals((byte) 0x00, map[0]);
  }

  @Test
  public void singleByteAllBits() {
    for (int i = 0; i < 8; i++) {
      for (int j = 0; j <= 8 - i; j++) {
        doTest(8, i, j);
      }
    }
  }

  @Test
  public void twoBytesHeadTail() {
    for (int i = 0; i < 64; i++) {
      for (int j = 0; j <= 64 - i; j += 8) {
        doTest(64, i, j);
      }
    }
  }

  @Test
  public void twoBytesPartialHead() {
    for (int i = 0; i < 64; i += 8) {
      for (int j = 0; j <= 64 - i; j += 8) {
        doTest(64, i, j);
      }
    }
  }

  @Test
  public void twoBytesPartialTail() {
    int size = 64;
    for (int i = 0; i < size; i += 8) {
      for (int j = 1; j <= size - i; j++) {
        doTest(size, i, j);
      }
    }
  }

  private void doTest(int size, int start, int length) {
    BitMap map = new BitMap(size);
    BitMap bitMap = new BitMap(size);
    AlignedTVList.markRange(map.getByteArray(), start, length);
    for (int i = start; i < start + length; i++) {
      bitMap.mark(i);
    }
    assertArrayEquals(map.getByteArray(), bitMap.getByteArray());
  }
}
