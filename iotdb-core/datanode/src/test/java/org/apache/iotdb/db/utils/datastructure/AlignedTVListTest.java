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

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALByteBufferForTest;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.external.commons.lang3.ArrayUtils;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BitMap;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.iotdb.db.storageengine.rescon.memory.PrimitiveArrayManager.ARRAY_SIZE;

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
  public void testBitmapIsAllocatedLazilyWithCompactBackingArray() {
    AlignedTVList tvList =
        AlignedTVList.newAlignedList(Arrays.asList(TSDataType.INT64, TSDataType.INT64));
    Object[] values = new Object[] {1L, 1L};
    for (int i = 0; i < ARRAY_SIZE * 2 + 1; i++) {
      tvList.putAlignedValue(i, values);
    }

    Assert.assertNull(tvList.getBitMaps());
    tvList.putAlignedValue(ARRAY_SIZE * 2 + 1L, new Object[] {null, 1L});

    List<BitMap> firstColumnBitMaps = tvList.getBitMaps().get(0);
    Assert.assertEquals(3, firstColumnBitMaps.size());
    Assert.assertNull(firstColumnBitMaps.get(0));
    Assert.assertNull(firstColumnBitMaps.get(1));
    Assert.assertNotNull(firstColumnBitMaps.get(2));
    Assert.assertEquals(
        BitMap.getSizeOfBytes(ARRAY_SIZE), firstColumnBitMaps.get(2).getByteArray().length);
    Assert.assertTrue(
        firstColumnBitMaps.get(2).ramBytesUsed() < new BitMap(ARRAY_SIZE).ramBytesUsed());
    Assert.assertTrue(tvList.isNullValue(ARRAY_SIZE * 2 + 1, 0));
    Assert.assertFalse(tvList.isNullValue(ARRAY_SIZE * 2, 0));
  }

  @Test
  public void testFailedStatusBitmapWithNonByteAlignedOffset() {
    AlignedTVList tvList =
        AlignedTVList.newAlignedList(Arrays.asList(TSDataType.INT64, TSDataType.INT64));
    long[] times = new long[ARRAY_SIZE + 3];
    long[][] values = new long[2][ARRAY_SIZE + 3];
    TSStatus[] results = new TSStatus[ARRAY_SIZE + 3];
    for (int i = 0; i < times.length; i++) {
      times[i] = i;
      values[0][i] = i;
      values[1][i] = i;
    }
    results[ARRAY_SIZE - 1] = new TSStatus(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode());
    results[ARRAY_SIZE + 1] = new TSStatus(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode());

    tvList.putAlignedValues(times, values, null, 0, 3, results);
    tvList.putAlignedValues(times, values, null, 3, times.length, results);

    for (int column = 0; column < 2; column++) {
      Assert.assertFalse(tvList.isNullValue(ARRAY_SIZE - 2, column));
      Assert.assertTrue(tvList.isNullValue(ARRAY_SIZE - 1, column));
      Assert.assertFalse(tvList.isNullValue(ARRAY_SIZE, column));
      Assert.assertTrue(tvList.isNullValue(ARRAY_SIZE + 1, column));
      Assert.assertFalse(tvList.isNullValue(ARRAY_SIZE + 2, column));
    }

    Assert.assertEquals(1, AlignedTVList.buildResultBitMapBytes(new TSStatus[8], 0, 0, 8).length);
  }

  @Test
  public void testEmptyInputBitmapsDoNotMaterializeMemTableBitmaps() {
    AlignedTVList tvList = AlignedTVList.newAlignedList(List.of(TSDataType.INT64));
    long[] times = new long[ARRAY_SIZE];
    long[][] values = new long[1][ARRAY_SIZE];
    BitMap[] bitMaps = new BitMap[] {new BitMap(ARRAY_SIZE)};
    TSStatus[] results = new TSStatus[ARRAY_SIZE];
    for (int i = 0; i < ARRAY_SIZE; i++) {
      times[i] = i;
      values[0][i] = i;
    }

    tvList.putAlignedValues(times, values, bitMaps, 0, ARRAY_SIZE, results);

    Assert.assertNull(tvList.getBitMaps());

    Arrays.fill(results, new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));
    tvList.putAlignedValues(times, values, bitMaps, 0, ARRAY_SIZE, results);

    Assert.assertNull(tvList.getBitMaps());
  }

  @Test
  public void testPrimitiveArraysAreAllocatedOnFirstWrite() {
    AlignedTVList tvList =
        AlignedTVList.newAlignedList(
            new ArrayList<>(Arrays.asList(TSDataType.INT64, TSDataType.INT64)));
    for (int i = 0; i <= ARRAY_SIZE; i++) {
      tvList.putAlignedValue(i, new Object[] {(long) i, null});
    }

    Assert.assertNotNull(tvList.getValues().get(0).get(0));
    Assert.assertNotNull(tvList.getValues().get(0).get(1));
    Assert.assertNull(tvList.getValues().get(1).get(0));
    Assert.assertNull(tvList.getValues().get(1).get(1));

    tvList.putAlignedValue(ARRAY_SIZE + 1L, new Object[] {null, 1L});

    Assert.assertNull(tvList.getValues().get(1).get(0));
    Assert.assertNotNull(tvList.getValues().get(1).get(1));
    Assert.assertTrue(tvList.isNullValue(0, 1));
    Assert.assertEquals(1, tvList.getLongByValueIndex(ARRAY_SIZE + 1, 1));

    tvList.extendColumn(TSDataType.INT32);

    Assert.assertNull(tvList.getValues().get(2).get(0));
    Assert.assertNull(tvList.getValues().get(2).get(1));

    long ramSizeBeforeExtendedColumnMaterialization = tvList.calculateRamSize().getRamSize();
    tvList.putAlignedValue(ARRAY_SIZE + 2L, new Object[] {null, null, 2});

    Assert.assertNull(tvList.getValues().get(2).get(0));
    Assert.assertNotNull(tvList.getValues().get(2).get(1));
    Assert.assertTrue(tvList.isNullValue(0, 2));
    Assert.assertFalse(tvList.isNullValue(ARRAY_SIZE + 2, 2));
    Assert.assertEquals(2, tvList.getIntByValueIndex(ARRAY_SIZE + 2, 2));
    Assert.assertEquals(
        AlignedTVList.primitiveArrayMemCost(TSDataType.INT32),
        tvList.calculateRamSize().getRamSize() - ramSizeBeforeExtendedColumnMaterialization);
  }

  @Test
  public void testCalculateRamSizeCountsMaterializedPrimitiveArrays() {
    AlignedTVList tvList =
        AlignedTVList.newAlignedList(Arrays.asList(TSDataType.INT64, TSDataType.INT64));
    for (int i = 0; i <= ARRAY_SIZE; i++) {
      tvList.putAlignedValue(i, new Object[] {(long) i, null});
    }

    long ramSizeBeforeMaterialization = tvList.calculateRamSize().getRamSize();
    tvList.putAlignedValue(ARRAY_SIZE + 1L, new Object[] {1L, 1L});

    Assert.assertEquals(
        AlignedTVList.primitiveArrayMemCost(TSDataType.INT64),
        tvList.calculateRamSize().getRamSize() - ramSizeBeforeMaterialization);

    Assert.assertEquals(
        tvList.calculateRamSize().getRamSize(), tvList.clone().calculateRamSize().getRamSize());
    Assert.assertEquals(
        tvList.calculateRamSize().getRamSize(),
        tvList.cloneForFlushSort().calculateRamSize().getRamSize());

    AlignedTVList projectedTvList =
        (AlignedTVList) tvList.getTvListByColumnIndex(List.of(1), List.of(TSDataType.INT64), false);
    Assert.assertEquals(
        (long) projectedTvList.getValues().get(0).size()
                * projectedTvList.alignedTvListArrayMemCostWithoutPrimitiveArrays()
            + AlignedTVList.primitiveArrayMemCost(TSDataType.INT64),
        projectedTvList.calculateRamSize().getRamSize());

    tvList.clear();
    Assert.assertEquals(0, tvList.calculateRamSize().getRamSize());
  }

  @Test
  public void testCalculateRamSizeExcludesUnallocatedPrimitiveArrays() {
    AlignedTVList tvList =
        AlignedTVList.newAlignedList(Arrays.asList(TSDataType.INT64, TSDataType.INT64));
    for (int i = 0; i <= ARRAY_SIZE; i++) {
      tvList.putAlignedValue(i, new Object[] {(long) i, null});
    }

    int blockCount = tvList.getValues().get(0).size();
    long denseRamSize = blockCount * tvList.alignedTvListArrayMemCost();
    long expectedRamSize =
        denseRamSize - blockCount * AlignedTVList.primitiveArrayMemCost(TSDataType.INT64);

    Assert.assertEquals(expectedRamSize, tvList.calculateRamSize().getRamSize());
  }

  @Test
  public void testBatchDoesNotAllocateAllNullPrimitiveArray() {
    AlignedTVList tvList =
        AlignedTVList.newAlignedList(Arrays.asList(TSDataType.INT64, TSDataType.INT64));
    long[] times = new long[ARRAY_SIZE];
    long[][] values = new long[2][ARRAY_SIZE];
    BitMap[] bitMaps = new BitMap[] {null, new BitMap(ARRAY_SIZE)};
    bitMaps[1].markAll();
    for (int i = 0; i < ARRAY_SIZE; i++) {
      times[i] = i;
      values[0][i] = i;
      values[1][i] = i;
    }

    tvList.putAlignedValues(times, values, bitMaps, 0, ARRAY_SIZE, null);

    Assert.assertNotNull(tvList.getValues().get(0).get(0));
    Assert.assertNull(tvList.getValues().get(1).get(0));
    Assert.assertTrue(tvList.isNullValue(ARRAY_SIZE - 1, 1));
  }

  @Test
  public void testNullPrimitiveArrayCanBeClonedAndSerialized() throws IOException {
    AlignedTVList tvList =
        AlignedTVList.newAlignedList(Arrays.asList(TSDataType.TEXT, TSDataType.INT64));
    for (int i = 0; i <= ARRAY_SIZE; i++) {
      tvList.putAlignedValue(i, new Object[] {null, null});
    }
    tvList.putAlignedValue(
        ARRAY_SIZE + 1L, new Object[] {new Binary("value", TSFileConfig.STRING_CHARSET), 1L});

    AlignedTVList clonedTvList = tvList.clone();
    Assert.assertNull(clonedTvList.getValues().get(0).get(0));
    Assert.assertNull(clonedTvList.getValues().get(1).get(0));
    Assert.assertEquals("[null, null]", clonedTvList.getAlignedValue(0).toString());
    Assert.assertEquals("[value, 1]", clonedTvList.getAlignedValue(ARRAY_SIZE + 1).toString());

    WALByteBufferForTest walBuffer =
        new WALByteBufferForTest(ByteBuffer.allocate(tvList.serializedSize()));
    tvList.serializeToWAL(walBuffer);
    AlignedTVList deserializedTvList =
        AlignedTVList.deserialize(
            new DataInputStream(new ByteArrayInputStream(walBuffer.getBuffer().array())));

    Assert.assertEquals(tvList.rowCount(), deserializedTvList.rowCount());
    Assert.assertEquals("[null, null]", deserializedTvList.getAlignedValue(0).toString());
    Assert.assertEquals(
        "[value, 1]", deserializedTvList.getAlignedValue(ARRAY_SIZE + 1).toString());
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
    Assert.assertEquals(tvList.memoryBinaryChunkSize[1], 720);

    tvList.deleteColumn(0);
    Assert.assertEquals(tvList.memoryBinaryChunkSize.length, 2);
    Assert.assertEquals(tvList.memoryBinaryChunkSize[0], 0);

    tvList.extendColumn(TSDataType.INT32);
    Assert.assertEquals(tvList.memoryBinaryChunkSize.length, 3);
    Assert.assertEquals(tvList.memoryBinaryChunkSize[0], 0);

    tvList.extendColumn(TSDataType.TEXT);
    Assert.assertEquals(tvList.memoryBinaryChunkSize.length, 4);
    Assert.assertEquals(tvList.memoryBinaryChunkSize[0], 0);
    Assert.assertEquals(tvList.memoryBinaryChunkSize[1], 720);

    tvList.delete(4, 6);
    Assert.assertEquals(tvList.memoryBinaryChunkSize.length, 4);
    Assert.assertEquals(tvList.memoryBinaryChunkSize[0], 0);
    Assert.assertEquals(tvList.memoryBinaryChunkSize[1], 720);

    tvList.clear();
    Assert.assertEquals(tvList.memoryBinaryChunkSize[0], 0);
    Assert.assertEquals(tvList.memoryBinaryChunkSize[1], 0);
  }
}
