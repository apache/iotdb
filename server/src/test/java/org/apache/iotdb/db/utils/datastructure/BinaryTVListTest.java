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

import org.apache.commons.lang.ArrayUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class BinaryTVListTest {

  @Test
  public void testBinaryTVList() {
    BinaryTVList tvList = new BinaryTVList();
    for (int i = 0; i < 1000; i++) {
      tvList.putBinary(i, Binary.valueOf(String.valueOf(i)));
    }
    for (int i = 0; i < tvList.size; i++) {
      Assert.assertEquals(String.valueOf(i), tvList.getBinary(i).toString());
      Assert.assertEquals(i, tvList.getTime(i));
    }
  }

  @Test
  public void testBinaryTVLists() {
    BinaryTVList tvList = new BinaryTVList();
    Binary[] binaryList = new Binary[1001];
    List<Long> timeList = new ArrayList<>();
    for (int i = 1000; i >= 0; i--) {
      timeList.add((long) i);
      binaryList[i] = Binary.valueOf(String.valueOf(i));
    }
    tvList.putBinaries(ArrayUtils.toPrimitive(timeList.toArray(new Long[0])), binaryList, 0, 1000);
    for (long i = 0; i < tvList.size; i++) {
      Assert.assertEquals(tvList.size - i, tvList.getTime((int) i));
    }
  }
}
