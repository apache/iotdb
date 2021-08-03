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

import org.apache.commons.lang3.ArrayUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class DoubleTVListTest {
  public static double delta = 0.001d;

  @Test
  public void testDoubleTVList1() {
    DoubleTVList tvList = new DoubleTVList();
    for (int i = 0; i < 1000; i++) {
      tvList.putDouble(i, i);
    }
    tvList.sort();
    for (int i = 0; i < tvList.size; i++) {
      Assert.assertEquals(i, tvList.getDouble(i), delta);
      Assert.assertEquals(i, tvList.getTime(i));
    }
  }

  @Test
  public void testDoubleTVList2() {
    DoubleTVList tvList = new DoubleTVList();
    for (int i = 1000; i >= 0; i--) {
      tvList.putDouble(i, i);
    }
    tvList.sort();
    for (int i = 0; i < tvList.size; i++) {
      Assert.assertEquals(i, tvList.getDouble(i), delta);
      Assert.assertEquals(i, tvList.getTime(i));
    }
  }

  @Test
  public void testDoubleTVLists() {
    DoubleTVList tvList = new DoubleTVList();
    List<Double> doubleList = new ArrayList<>();
    List<Long> timeList = new ArrayList<>();
    for (long i = 1000; i >= 0; i--) {
      timeList.add(i);
      doubleList.add((double) i);
    }
    tvList.putDoubles(
        ArrayUtils.toPrimitive(timeList.toArray(new Long[0])),
        ArrayUtils.toPrimitive(doubleList.toArray(new Double[0]), 0.0d),
        0,
        1000);
    for (long i = 0; i < tvList.size; i++) {
      Assert.assertEquals((double) tvList.size - i, tvList.getDouble((int) i), delta);
      Assert.assertEquals(tvList.size - i, tvList.getTime((int) i));
    }
  }
}
