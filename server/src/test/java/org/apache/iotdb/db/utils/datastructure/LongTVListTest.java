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

import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.utils.BitMap;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType.TsLong;

import org.apache.commons.lang3.ArrayUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class LongTVListTest {

  @Test
  public void testLongTVList1() {
    LongTVList tvList = new LongTVList();
    for (long i = 0; i < 1000; i++) {
      tvList.putLong(i, i);
    }
    tvList.sort();
    for (long i = 0; i < tvList.size; i++) {
      Assert.assertEquals(i, tvList.getLong((int) i));
      Assert.assertEquals(i, tvList.getTime((int) i));
    }
  }

  @Test
  public void testLongTVList2() {
    LongTVList tvList = new LongTVList();
    for (long i = 1000; i >= 0; i--) {
      tvList.putLong(i, i);
    }
    tvList.sort();
    for (long i = 0; i < tvList.size; i++) {
      Assert.assertEquals(i, tvList.getLong((int) i));
      Assert.assertEquals(i, tvList.getTime((int) i));
    }
  }

  @Test
  public void testLongTVList3() {
    Random random = new Random();
    LongTVList tvList = new LongTVList();
    List<TimeValuePair> inputs = new ArrayList<>();
    for (long i = 0; i < 10000; i++) {
      long time = random.nextInt(10000);
      long value = random.nextInt(10000);
      tvList.putLong(time, value);
      inputs.add(new TimeValuePair(time, new TsLong(value)));
    }
    tvList.sort();
    inputs.sort(TimeValuePair::compareTo);
    for (long i = 0; i < tvList.size; i++) {
      Assert.assertEquals(inputs.get((int) i).getTimestamp(), tvList.getTime((int) i));
      Assert.assertEquals(inputs.get((int) i).getValue().getLong(), tvList.getLong((int) i));
    }
  }

  @Test
  public void testPutLongsWithoutBitMap() {
    LongTVList tvList = new LongTVList();
    List<Long> longList = new ArrayList<>();
    List<Long> timeList = new ArrayList<>();
    for (long i = 1000; i >= 0; i--) {
      timeList.add(i);
      longList.add(i);
    }
    tvList.putLongs(
        ArrayUtils.toPrimitive(timeList.toArray(new Long[0])),
        ArrayUtils.toPrimitive(longList.toArray(new Long[0])),
        null,
        0,
        1000);
    for (long i = 0; i < tvList.size; i++) {
      Assert.assertEquals(tvList.size - i, tvList.getLong((int) i));
      Assert.assertEquals(tvList.size - i, tvList.getTime((int) i));
    }
  }

  @Test
  public void testPutIntsWithBitMap() {
    LongTVList tvList = new LongTVList();
    List<Long> longList = new ArrayList<>();
    List<Long> timeList = new ArrayList<>();
    BitMap bitMap = new BitMap(1001);
    for (long i = 1000; i >= 0; i--) {
      timeList.add(i);
      longList.add(i);
      if (i % 100 == 0) {
        bitMap.mark((int) i);
      }
    }
    tvList.putLongs(
        ArrayUtils.toPrimitive(timeList.toArray(new Long[0])),
        ArrayUtils.toPrimitive(longList.toArray(new Long[0])),
        bitMap,
        0,
        1000);
    tvList.sort();
    int nullCnt = 0;
    for (long i = 1; i < longList.size(); i++) {
      if (i % 100 == 0) {
        nullCnt++;
        continue;
      }
      Assert.assertEquals(i, tvList.getLong((int) i - nullCnt - 1));
      Assert.assertEquals(i, tvList.getTime((int) i - nullCnt - 1));
    }
  }

  @Test
  public void testClone() {
    LongTVList tvList = new LongTVList();
    List<Long> longList = new ArrayList<>();
    List<Long> timeList = new ArrayList<>();
    BitMap bitMap = new BitMap(1001);
    for (long i = 1000; i >= 0; i--) {
      timeList.add(i);
      longList.add(i);
      if (i % 100 == 0) {
        bitMap.mark((int) i);
      }
    }
    tvList.putLongs(
        ArrayUtils.toPrimitive(timeList.toArray(new Long[0])),
        ArrayUtils.toPrimitive(longList.toArray(new Long[0])),
        bitMap,
        0,
        1000);
    tvList.sort();
    LongTVList clonedTvList = tvList.clone();
    for (long i = 0; i < tvList.size; i++) {
      Assert.assertEquals(tvList.getLong((int) i), clonedTvList.getLong((int) i));
      Assert.assertEquals(tvList.getTime((int) i), clonedTvList.getTime((int) i));
    }
  }
}
