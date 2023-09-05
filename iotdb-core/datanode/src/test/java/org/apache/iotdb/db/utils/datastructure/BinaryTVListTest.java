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
package org.apache.iotdb.db.utils.datastructure;

import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.BitMap;

import org.apache.commons.lang3.ArrayUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class BinaryTVListTest {

  @Test
  public void testBinaryTVList() {
    BinaryTVList tvList = BinaryTVList.newList();
    for (int i = 0; i < 1000; i++) {
      tvList.putBinary(i, Binary.valueOf(String.valueOf(i)));
    }
    for (int i = 0; i < tvList.rowCount; i++) {
      Assert.assertEquals(String.valueOf(i), tvList.getBinary(i).toString());
      Assert.assertEquals(i, tvList.getTime(i));
    }
  }

  @Test
  public void testPutBinariesWithoutBitMap() {
    BinaryTVList tvList = BinaryTVList.newList();
    Binary[] binaryList = new Binary[1001];
    List<Long> timeList = new ArrayList<>();
    for (int i = 1000; i >= 0; i--) {
      timeList.add((long) i);
      binaryList[1000 - i] = Binary.valueOf(String.valueOf(i));
    }
    tvList.putBinaries(
        ArrayUtils.toPrimitive(timeList.toArray(new Long[0])), binaryList, null, 0, 1000);
    for (long i = 0; i < tvList.rowCount; i++) {
      Assert.assertEquals(tvList.rowCount - i, tvList.getTime((int) i));
    }
  }

  @Test
  public void testPutBinariesWithBitMap() {
    BinaryTVList tvList = BinaryTVList.newList();
    Binary[] binaryList = new Binary[1001];
    List<Long> timeList = new ArrayList<>();
    BitMap bitMap = new BitMap(1001);
    for (int i = 1000; i >= 0; i--) {
      timeList.add((long) i);
      binaryList[1000 - i] = Binary.valueOf(String.valueOf(i));
      if (i % 100 == 0) {
        bitMap.mark(i);
      }
    }
    tvList.putBinaries(
        ArrayUtils.toPrimitive(timeList.toArray(new Long[0])), binaryList, bitMap, 0, 1000);
    tvList.sort();
    int nullCnt = 0;
    for (long i = 1; i < binaryList.length; i++) {
      if (i % 100 == 0) {
        nullCnt++;
        continue;
      }
      Assert.assertEquals(
          Binary.valueOf(String.valueOf(i)), tvList.getBinary((int) i - nullCnt - 1));
      Assert.assertEquals(i, tvList.getTime((int) i - nullCnt - 1));
    }
  }

  @Test
  public void testClone() {
    BinaryTVList tvList = BinaryTVList.newList();
    Binary[] binaryList = new Binary[1001];
    List<Long> timeList = new ArrayList<>();
    BitMap bitMap = new BitMap(1001);
    for (int i = 1000; i >= 0; i--) {
      timeList.add((long) i);
      binaryList[i] = Binary.valueOf(String.valueOf(i));
      if (i % 100 == 0) {
        bitMap.mark(i);
      }
    }
    tvList.putBinaries(
        ArrayUtils.toPrimitive(timeList.toArray(new Long[0])), binaryList, bitMap, 0, 1000);
    tvList.sort();
    BinaryTVList clonedTvList = tvList.clone();
    for (long i = 0; i < tvList.rowCount; i++) {
      Assert.assertEquals(tvList.getBinary((int) i), clonedTvList.getBinary((int) i));
      Assert.assertEquals(tvList.getTime((int) i), clonedTvList.getTime((int) i));
    }
    Assert.assertEquals(tvList.memoryBinaryChunkSize, clonedTvList.memoryBinaryChunkSize);
  }

  @Test
  public void testCalculateChunkSize() {
    BinaryTVList tvList = BinaryTVList.newList();
    for (int i = 0; i < 10; i++) {
      tvList.putBinary(i, Binary.valueOf(String.valueOf(i)));
    }
    Assert.assertEquals(tvList.memoryBinaryChunkSize, 360);

    Binary[] binaryList = new Binary[10];
    List<Long> timeList = new ArrayList<>();
    BitMap bitMap = new BitMap(10);
    for (int i = 0; i < 10; i++) {
      timeList.add((long) i + 10);
      binaryList[i] = Binary.valueOf(String.valueOf(i));
      if (i % 2 == 0) {
        bitMap.mark(i);
      }
    }
    tvList.putBinaries(
        ArrayUtils.toPrimitive(timeList.toArray(new Long[0])), binaryList, bitMap, 0, 10);
    Assert.assertEquals(tvList.memoryBinaryChunkSize, 540);

    tvList.delete(5, 15);
    Assert.assertEquals(tvList.memoryBinaryChunkSize, 252);

    tvList.clear();
    Assert.assertEquals(tvList.memoryBinaryChunkSize, 0);
  }
}
