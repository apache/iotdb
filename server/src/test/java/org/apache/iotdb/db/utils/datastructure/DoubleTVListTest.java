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

import org.apache.iotdb.tsfile.utils.BitMap;

import org.apache.commons.lang3.ArrayUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class DoubleTVListTest {
  public static double delta = 0.001d;

  @Test
  public void testDoubleTVList1() {
    DoubleTVList tvList = DoubleTVList.newList();
    for (int i = 0; i < 1000; i++) {
      tvList.putDouble(i, i);
    }
    tvList.sort();
    for (int i = 0; i < tvList.rowCount; i++) {
      Assert.assertEquals(i, tvList.getDouble(i), delta);
      Assert.assertEquals(i, tvList.getTime(i));
    }
  }

  @Test
  public void testDoubleTVList2() {
    DoubleTVList tvList = DoubleTVList.newList();
    for (int i = 1000; i >= 0; i--) {
      tvList.putDouble(i, i);
    }
    tvList.sort();
    for (int i = 0; i < tvList.rowCount; i++) {
      Assert.assertEquals(i, tvList.getDouble(i), delta);
      Assert.assertEquals(i, tvList.getTime(i));
    }
  }

  @Test
  public void testPutDoublesWithoutBitMap() {
    DoubleTVList tvList = DoubleTVList.newList();
    List<Double> doubleList = new ArrayList<>();
    List<Long> timeList = new ArrayList<>();
    for (long i = 1000; i >= 0; i--) {
      timeList.add(i);
      doubleList.add((double) i);
    }
    tvList.putDoubles(
        ArrayUtils.toPrimitive(timeList.toArray(new Long[0])),
        ArrayUtils.toPrimitive(doubleList.toArray(new Double[0]), 0.0d),
        null,
        0,
        1000);
    for (long i = 0; i < tvList.rowCount; i++) {
      Assert.assertEquals((double) tvList.rowCount - i, tvList.getDouble((int) i), delta);
      Assert.assertEquals(tvList.rowCount - i, tvList.getTime((int) i));
    }
  }

  @Test
  public void testPutDoublesWithBitMap() {
    DoubleTVList tvList = DoubleTVList.newList();
    List<Double> doubleList = new ArrayList<>();
    List<Long> timeList = new ArrayList<>();
    BitMap bitMap = new BitMap(1001);
    for (long i = 1000; i >= 0; i--) {
      timeList.add((i));
      doubleList.add((double) i);
      if (i % 100 == 0) {
        bitMap.mark((int) i);
      }
    }
    tvList.putDoubles(
        ArrayUtils.toPrimitive(timeList.toArray(new Long[0])),
        ArrayUtils.toPrimitive(doubleList.toArray(new Double[0]), 0.0d),
        bitMap,
        0,
        1000);
    tvList.sort();
    int nullCnt = 0;
    for (long i = 1; i < doubleList.size(); i++) {
      if (i % 100 == 0) {
        nullCnt++;
        continue;
      }
      Assert.assertEquals(i, tvList.getDouble((int) i - nullCnt - 1), delta);
      Assert.assertEquals(i, tvList.getTime((int) i - nullCnt - 1));
    }
  }

  @Test
  public void testClone() {
    DoubleTVList tvList = DoubleTVList.newList();
    List<Double> doubleList = new ArrayList<>();
    List<Long> timeList = new ArrayList<>();
    BitMap bitMap = new BitMap(1001);
    for (long i = 1000; i >= 0; i--) {
      timeList.add((i));
      doubleList.add((double) i);
      if (i % 100 == 0) {
        bitMap.mark((int) i);
      }
    }
    tvList.putDoubles(
        ArrayUtils.toPrimitive(timeList.toArray(new Long[0])),
        ArrayUtils.toPrimitive(doubleList.toArray(new Double[0]), 0.0d),
        bitMap,
        0,
        1000);
    tvList.sort();
    DoubleTVList clonedTvList = tvList.clone();
    for (long i = 0; i < tvList.rowCount; i++) {
      Assert.assertEquals(tvList.getDouble((int) i), clonedTvList.getDouble((int) i), delta);
      Assert.assertEquals(tvList.getTime((int) i), clonedTvList.getTime((int) i));
    }
  }
}
