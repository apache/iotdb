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

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.BitMap;

import org.apache.commons.lang3.ArrayUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class VectorTVListTest {

  @Test
  public void testVectorTVList1() {
    List<TSDataType> dataTypes = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      dataTypes.add(TSDataType.INT64);
    }
    VectorTVList tvList = new VectorTVList(dataTypes);
    for (long i = 0; i < 1000; i++) {
      Object[] value = new Object[5];
      for (int j = 0; j < 5; j++) {
        value[j] = i;
      }
      tvList.putVector(i, value);
    }
    for (int i = 0; i < tvList.size; i++) {
      StringBuilder builder = new StringBuilder("[");
      builder.append(String.valueOf(i));
      for (int j = 1; j < 5; j++) {
        builder.append(", ").append(String.valueOf(i));
      }
      builder.append("]");
      Assert.assertEquals(builder.toString(), tvList.getVector(i).toString());
      Assert.assertEquals(i, tvList.getTime(i));
    }
  }

  @Test
  public void testVectorTVList2() {
    List<TSDataType> dataTypes = new ArrayList<>();
    dataTypes.add(TSDataType.BOOLEAN);
    dataTypes.add(TSDataType.INT32);
    dataTypes.add(TSDataType.INT64);
    dataTypes.add(TSDataType.FLOAT);
    dataTypes.add(TSDataType.DOUBLE);
    dataTypes.add(TSDataType.TEXT);
    VectorTVList tvList = new VectorTVList(dataTypes);
    for (int i = 1000; i >= 0; i--) {
      Object[] value = new Object[6];
      value[0] = false;
      value[1] = 100;
      value[2] = 1000L;
      value[3] = 0.1f;
      value[4] = 0.2d;
      value[5] = new Binary("Test");
      tvList.putVector(i, value);
    }
    tvList.sort();
    for (int i = 0; i < tvList.size; i++) {
      StringBuilder builder = new StringBuilder("[");
      builder.append("false, 100, 1000, 0.1, 0.2, Test");
      builder.append("]");
      Assert.assertEquals(builder.toString(), tvList.getVector(i).toString());
      Assert.assertEquals(i, tvList.getTime(i));
    }
  }

  @Test
  public void testVectorTVLists() {
    List<TSDataType> dataTypes = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      dataTypes.add(TSDataType.INT64);
    }
    VectorTVList tvList = new VectorTVList(dataTypes);
    long[][] vectorArray = new long[5][1001];
    List<Long> timeList = new ArrayList<>();
    for (int i = 1000; i >= 0; i--) {
      timeList.add((long) i);
      for (int j = 0; j < 5; j++) {
        vectorArray[j][i] = (long) i;
      }
    }

    tvList.putVectors(
        ArrayUtils.toPrimitive(timeList.toArray(new Long[0])), vectorArray, null, 0, 1000);
    for (long i = 0; i < tvList.size; i++) {
      Assert.assertEquals(tvList.size - i, tvList.getTime((int) i));
    }
  }

  @Test
  public void testVectorTVListsWithBitMaps() {
    List<TSDataType> dataTypes = new ArrayList<>();
    BitMap[] bitMaps = new BitMap[5];
    for (int i = 0; i < 5; i++) {
      dataTypes.add(TSDataType.INT64);
      bitMaps[i] = new BitMap(1001);
    }
    VectorTVList tvList = new VectorTVList(dataTypes);
    long[][] vectorArray = new long[5][1001];
    List<Long> timeList = new ArrayList<>();
    for (int i = 1000; i >= 0; i--) {
      timeList.add((long) i);
      for (int j = 0; j < 5; j++) {
        vectorArray[j][i] = (long) i;
        if (i % 100 == 0) {
          bitMaps[j].mark(i);
        }
      }
    }

    tvList.putVectors(
        ArrayUtils.toPrimitive(timeList.toArray(new Long[0])), vectorArray, bitMaps, 0, 1000);
    for (long i = 0; i < tvList.size; i++) {
      Assert.assertEquals(tvList.size - i, tvList.getTime((int) i));
      if (i % 100 == 0) {
        Assert.assertEquals("[null, null, null, null, null]", tvList.getVector((int) i).toString());
      }
    }
  }

  @Test
  public void testClone() {
    List<TSDataType> dataTypes = new ArrayList<>();
    BitMap[] bitMaps = new BitMap[5];
    for (int i = 0; i < 5; i++) {
      dataTypes.add(TSDataType.INT64);
      bitMaps[i] = new BitMap(1001);
    }
    VectorTVList tvList = new VectorTVList(dataTypes);
    long[][] vectorArray = new long[5][1001];
    List<Long> timeList = new ArrayList<>();
    for (int i = 1000; i >= 0; i--) {
      timeList.add((long) i);
      for (int j = 0; j < 5; j++) {
        vectorArray[j][i] = (long) i;
        if (i % 100 == 0) {
          bitMaps[j].mark(i);
        }
      }
    }

    tvList.putVectors(
        ArrayUtils.toPrimitive(timeList.toArray(new Long[0])), vectorArray, bitMaps, 0, 1000);

    VectorTVList clonedTvList = tvList.clone();
    for (long i = 0; i < tvList.size; i++) {
      Assert.assertEquals(tvList.getTime((int) i), clonedTvList.getTime((int) i));
      Assert.assertEquals(
          tvList.getVector((int) i).toString(), clonedTvList.getVector((int) i).toString());
      for (int column = 0; i < 5; i++) {
        Assert.assertEquals(
            tvList.isValueMarked((int) i, column), clonedTvList.isValueMarked((int) i, column));
      }
    }
  }
}
