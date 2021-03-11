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

import org.apache.iotdb.tsfile.utils.BytesUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class VectorTVListTest {

  @Test
  public void testVectorTVList() {
    VectorTVList tvList = new VectorTVList();
    for (int i = 0; i < 1000; i++) {
      byte[] value = new byte[4 * 5];
      byte[] bytes = new byte[4];
      for (int j = 0; j < 20; j++) {
        if (j % 4 == 0) {
          bytes = BytesUtils.intToBytes(i);
        }
        value[j] = bytes[j % 4];
      }
      tvList.putVector(i, value);
    }
    for (int i = 0; i < tvList.size; i++) {
      Assert.assertEquals(String.valueOf(i), tvList.getVector(i).toString());
      Assert.assertEquals(i, tvList.getTime(i));
    }
  }

  @Test
  public void testVectorTVLists() {
    VectorTVList tvList = new VectorTVList();
    byte[][] vectorList = new byte[1001][4 * 5];
    List<Long> timeList = new ArrayList<>();
    for (int i = 1000; i >= 0; i--) {
      timeList.add((long) i);
      for (int j = 0; j < 5; j++) {
        vectorList[i][j] = 0;
      }
    }
    tvList.putVectors(ArrayUtils.toPrimitive(timeList.toArray(new Long[0])), vectorList, 0, 1000);
    for (long i = 0; i < tvList.size; i++) {
      Assert.assertEquals(tvList.size - i, tvList.getTime((int) i));
    }
  }
}
