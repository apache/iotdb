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

public class BooleanTVListTest {
  @Test
  public void testBooleanTVList() {
    BooleanTVList tvList = new BooleanTVList();
    for (int i = 0; i < 50; i++) {
      tvList.putBoolean(i, true);
    }
    for (int i = 50; i < 100; i++) {
      tvList.putBoolean(i, false);
    }
    for (int i = 0; i < tvList.size / 2; i++) {
      Assert.assertTrue(tvList.getBoolean(i));
      Assert.assertEquals(i, tvList.getTime(i));
    }
    for (int i = tvList.size / 2 + 1; i < tvList.size; i++) {
      Assert.assertFalse(tvList.getBoolean(i));
      Assert.assertEquals(i, tvList.getTime(i));
    }
  }

  @Test
  public void testBooleanTVLists() {
    BooleanTVList tvList = new BooleanTVList();
    List<Boolean> booleanList = new ArrayList<>();
    List<Long> timeList = new ArrayList<>();
    for (long i = 1000; i >= 0; i--) {
      timeList.add(i);
      booleanList.add(i % 2 == 0);
    }
    tvList.putBooleans(
        ArrayUtils.toPrimitive(timeList.toArray(new Long[0])),
        ArrayUtils.toPrimitive(booleanList.toArray(new Boolean[0])),
        0,
        1000);
    for (long i = 0; i < tvList.size; i++) {
      Assert.assertEquals(tvList.size - i, tvList.getTime((int) i));
    }
  }
}
