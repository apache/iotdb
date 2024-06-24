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
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BitMap;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class AlignedTVListTest {

  @Test
  public void testAlignedTVList1() {
    List<TSDataType> dataTypes = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      dataTypes.add(TSDataType.INT64);
    }
    AlignedTVList tvList = AlignedTVList.newAlignedList(dataTypes);
    for (long i = 0; i < 1000; i++) {
      Object[] value = new Object[5];
      for (int j = 0; j < 5; j++) {
        value[j] = i;
      }
      tvList.putAlignedValue(i, value);
    }
    for (int i = 0; i < tvList.rowCount; i++) {
      StringBuilder builder = new StringBuilder("[");
      builder.append(i);
      for (int j = 1; j < 5; j++) {
        builder.append(", ").append(i);
      }
      builder.append("]");
      Assert.assertEquals(builder.toString(), tvList.getAlignedValue(i).toString());
      Assert.assertEquals(i, tvList.getTime(i));
    }
  }

  @Test
  public void testAlignedTVList2() {
    List<TSDataType> dataTypes = new ArrayList<>();
    dataTypes.add(TSDataType.BOOLEAN);
    dataTypes.add(TSDataType.INT32);
    dataTypes.add(TSDataType.INT64);
    dataTypes.add(TSDataType.FLOAT);
    dataTypes.add(TSDataType.DOUBLE);
    dataTypes.add(TSDataType.TEXT);
    AlignedTVList tvList = AlignedTVList.newAlignedList(dataTypes);
    for (int i = 1000; i >= 0; i--) {
      Object[] value = new Object[6];
      value[0] = false;
      value[1] = 100;
      value[2] = 1000L;
      value[3] = 0.1f;
      value[4] = 0.2d;
      value[5] = new Binary("Test", TSFileConfig.STRING_CHARSET);
      tvList.putAlignedValue(i, value);
    }
    tvList.sort();
    for (int i = 0; i < tvList.rowCount; i++) {
      StringBuilder builder = new StringBuilder("[");
      builder.append("false, 100, 1000, 0.1, 0.2, Test");
      builder.append("]");
      Assert.assertEquals(builder.toString(), tvList.getAlignedValue(i).toString());
      Assert.assertEquals(i, tvList.getTime(i));
    }
  }

  @Test
  public void testAlignedTVLists() {
    List<TSDataType> dataTypes = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      dataTypes.add(TSDataType.INT64);
    }
    AlignedTVList tvList = AlignedTVList.newAlignedList(dataTypes);
    long[][] vectorArray = new long[5][1001];
    List<Long> timeList = new ArrayList<>();
    for (int i = 1000; i >= 0; i--) {
      timeList.add((long) i);
      for (int j = 0; j < 5; j++) {
        vectorArray[j][i] = (long) i;
      }
    }

    tvList.putAlignedValues(
        ArrayUtils.toPrimitive(timeList.toArray(new Long[0])), vectorArray, null, 0, 1000, null);
    for (long i = 0; i < tvList.rowCount; i++) {
      Assert.assertEquals(tvList.rowCount - i, tvList.getTime((int) i));
    }
  }

  @Test
  public void testAlignedTVListsWithBitMaps() {
    List<TSDataType> dataTypes = new ArrayList<>();
    BitMap[] bitMaps = new BitMap[5];
    for (int i = 0; i < 5; i++) {
      dataTypes.add(TSDataType.INT64);
      bitMaps[i] = new BitMap(1001);
    }
    AlignedTVList tvList = AlignedTVList.newAlignedList(dataTypes);
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

    tvList.putAlignedValues(
        ArrayUtils.toPrimitive(timeList.toArray(new Long[0])), vectorArray, bitMaps, 0, 1000, null);
    for (long i = 0; i < tvList.rowCount; i++) {
      Assert.assertEquals(tvList.rowCount - i, tvList.getTime((int) i));
      if (i % 100 == 0) {
        Assert.assertEquals(
            "[null, null, null, null, null]", tvList.getAlignedValue((int) i).toString());
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
    AlignedTVList tvList = AlignedTVList.newAlignedList(dataTypes);
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

    tvList.putAlignedValues(
        ArrayUtils.toPrimitive(timeList.toArray(new Long[0])), vectorArray, bitMaps, 0, 1000, null);

    AlignedTVList clonedTvList = tvList.clone();
    for (long i = 0; i < tvList.rowCount; i++) {
      Assert.assertEquals(tvList.getTime((int) i), clonedTvList.getTime((int) i));
      Assert.assertEquals(
          tvList.getAlignedValue((int) i).toString(),
          clonedTvList.getAlignedValue((int) i).toString());
      for (int column = 0; i < 5; i++) {
        Assert.assertEquals(
            tvList.isNullValue((int) i, column), clonedTvList.isNullValue((int) i, column));
      }
    }

    for (int i = 0; i < dataTypes.size(); i++) {
      Assert.assertEquals(tvList.memoryBinaryChunkSize[i], clonedTvList.memoryBinaryChunkSize[i]);
    }
  }

  @Test
  public void testCalculateChunkSize() {
    List<TSDataType> dataTypes = new ArrayList<>();
    dataTypes.add(TSDataType.INT32);
    dataTypes.add(TSDataType.TEXT);
    AlignedTVList tvList = AlignedTVList.newAlignedList(dataTypes);

    for (int i = 0; i < 10; i++) {
      Object[] value = new Object[2];
      value[0] = i;
      value[1] = new Binary(String.valueOf(i), TSFileConfig.STRING_CHARSET);
      tvList.putAlignedValue(i, value);
    }

    Assert.assertEquals(tvList.memoryBinaryChunkSize[0], 0);
    Assert.assertEquals(tvList.memoryBinaryChunkSize[1], 360);

    Object[] vectorArray = new Object[2];
    BitMap[] bitMaps = new BitMap[2];

    vectorArray[0] = new int[10];
    vectorArray[1] = new Binary[10];
    bitMaps[0] = new BitMap(10);
    bitMaps[1] = new BitMap(10);

    List<Long> timeList = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      timeList.add((long) i + 10);
      ((int[]) vectorArray[0])[i] = i;
      ((Binary[]) vectorArray[1])[i] = new Binary(String.valueOf(i), TSFileConfig.STRING_CHARSET);

      if (i % 2 == 0) {
        bitMaps[1].mark(i);
      }
    }

    tvList.putAlignedValues(
        ArrayUtils.toPrimitive(timeList.toArray(new Long[0])), vectorArray, bitMaps, 0, 10, null);
    Assert.assertEquals(tvList.memoryBinaryChunkSize[1], 720);

    tvList.delete(5, 15);
    Assert.assertEquals(tvList.memoryBinaryChunkSize[1], 324);

    tvList.deleteColumn(0);
    Assert.assertEquals(tvList.memoryBinaryChunkSize.length, 1);
    Assert.assertEquals(tvList.memoryBinaryChunkSize[0], 324);

    tvList.extendColumn(TSDataType.INT32);
    Assert.assertEquals(tvList.memoryBinaryChunkSize.length, 2);
    Assert.assertEquals(tvList.memoryBinaryChunkSize[0], 324);

    tvList.extendColumn(TSDataType.TEXT);
    Assert.assertEquals(tvList.memoryBinaryChunkSize.length, 3);
    Assert.assertEquals(tvList.memoryBinaryChunkSize[0], 324);
    Assert.assertEquals(tvList.memoryBinaryChunkSize[2], 0);

    tvList.delete(4, 6);
    Assert.assertEquals(tvList.memoryBinaryChunkSize.length, 3);
    Assert.assertEquals(tvList.memoryBinaryChunkSize[0], 216);
    Assert.assertEquals(tvList.memoryBinaryChunkSize[2], 0);

    tvList.clear();
    Assert.assertEquals(tvList.memoryBinaryChunkSize[0], 0);
    Assert.assertEquals(tvList.memoryBinaryChunkSize[2], 0);
  }
}
