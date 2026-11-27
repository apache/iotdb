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
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALByteBufferForTest;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.external.commons.lang3.ArrayUtils;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import static org.apache.iotdb.db.storageengine.rescon.memory.PrimitiveArrayManager.ARRAY_SIZE;
import static org.apache.tsfile.utils.RamUsageEstimator.NUM_BYTES_ARRAY_HEADER;
import static org.apache.tsfile.utils.RamUsageEstimator.NUM_BYTES_OBJECT_REF;

public class AlignedTVListTest {

  @Test
  public void testExtendColumnRetainsImplicitNullsOnFirstWrite() {
    AlignedTVList list =
        AlignedTVList.newAlignedList(new ArrayList<>(Collections.singletonList(TSDataType.INT64)));
    list.putAlignedValue(0, new Object[] {null});
    list.putAlignedValue(1, new Object[] {1L});
    list.extendColumn(TSDataType.INT32);
    Assert.assertNull(list.getValues().get(1).get(0));
    list.putAlignedValue(2, new Object[] {2L, 7});
    Assert.assertTrue(list.isNullValue(0, 0));
    Assert.assertTrue(list.isNullValue(0, 1));
    Assert.assertTrue(list.isNullValue(1, 1));
    Assert.assertEquals(7, list.getIntByValueIndex(2, 1));
    list.clear();
  }

  @Test
  public void testExtendColumnsPreservesLazyBlocksAndAccounting() {
    AlignedTVList list =
        AlignedTVList.newAlignedList(
            new ArrayList<>(Arrays.asList(TSDataType.INT64, TSDataType.TEXT)));
    Binary text = new Binary("value", TSFileConfig.STRING_CHARSET);
    for (int row = 0; row <= ARRAY_SIZE; row++) {
      list.putAlignedValue(row, new Object[] {row == 1 ? null : (long) row, text});
    }
    long ramBefore = list.getRamSize();
    long containersBefore = list.calculateContainerRamCost(null);
    long binarySizeBefore = list.memoryBinaryChunkSize[1];
    list.extendColumns(Arrays.asList(TSDataType.INT32, TSDataType.TEXT, TSDataType.DOUBLE));

    Assert.assertEquals(5, list.memoryBinaryChunkSize.length);
    Assert.assertEquals(binarySizeBefore, list.memoryBinaryChunkSize[1]);
    for (int column = 2; column < 5; column++) {
      Assert.assertEquals(0, list.memoryBinaryChunkSize[column]);
      Assert.assertEquals(2, list.getValues().get(column).size());
      Assert.assertNull(list.getValues().get(column).get(0));
      Assert.assertNull(list.getValues().get(column).get(1));
      Assert.assertNull(list.getBitMaps().get(column));
      for (int row = 0; row <= ARRAY_SIZE; row++) {
        Assert.assertTrue(list.isNullValue(row, column));
      }
    }
    Assert.assertEquals(
        ramBefore + list.calculateContainerRamCost(null) - containersBefore, list.getRamSize());

    list.putAlignedValue(ARRAY_SIZE + 1L, new Object[] {null, null, 7, text, 3.5});
    Assert.assertEquals(7, list.getIntByValueIndex(ARRAY_SIZE + 1, 2));
    Assert.assertEquals(text, list.getBinaryByValueIndex(ARRAY_SIZE + 1, 3));
    Assert.assertEquals(3.5, list.getDoubleByValueIndex(ARRAY_SIZE + 1, 4), 0);
    Assert.assertTrue(list.memoryBinaryChunkSize[3] > 0);
    for (int column = 2; column < 5; column++) {
      Assert.assertNull(list.getValues().get(column).get(0));
      Assert.assertTrue(list.isNullValue(ARRAY_SIZE, column));
    }
    Assert.assertEquals(list.getRamSize(null), list.getRamSize());
    AlignedTVList clone = list.clone();
    Assert.assertEquals(list.getRamSize(), clone.getRamSize());
    Assert.assertEquals(list.getRamSize(), list.cloneForFlushSort().getRamSize());
    clone.clear();
    list.clear();
    Assert.assertEquals(list.calculateContainerRamCost(null), list.getRamSize());
    list.putAlignedValue(0, new Object[] {1L, text, 2, text, 3.0});
    Assert.assertEquals(list.getRamSize(null), list.getRamSize());
    list.clear();
  }

  @Test
  public void testExtendColumnsBeforeFirstWriteAndEmptyBatch() {
    AlignedTVList list = AlignedTVList.newAlignedList(new ArrayList<>());
    list.extendColumns(Arrays.asList(TSDataType.INT32, TSDataType.INT64));
    long[] binarySizes = list.memoryBinaryChunkSize;
    long ramSize = list.getRamSize();
    list.extendColumns(Collections.emptyList());
    Assert.assertSame(binarySizes, list.memoryBinaryChunkSize);
    Assert.assertEquals(ramSize, list.getRamSize());
    list.putAlignedValue(0, new Object[] {3, 4L});
    Assert.assertEquals(3, list.getIntByValueIndex(0, 0));
    Assert.assertEquals(4L, list.getLongByValueIndex(0, 1));
    Assert.assertEquals(list.getRamSize(null), list.getRamSize());
    list.clear();
  }

  @Test
  public void testValueListArrayMemCostExcludesLazyBitmapAndNullPlaceholder() {
    long expected = (long) ARRAY_SIZE * Long.BYTES + NUM_BYTES_ARRAY_HEADER + NUM_BYTES_OBJECT_REF;

    Assert.assertEquals(expected, AlignedTVList.valueListArrayMemCost(TSDataType.INT64));
    Assert.assertEquals(
        BitMap.createBitMapDynamically(ARRAY_SIZE).ramBytesUsed(), AlignedTVList.bitmapRamCost());
    Assert.assertEquals(NUM_BYTES_OBJECT_REF, AlignedTVList.bitmapReferenceRamCost());
  }

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
  public void testSegmentMovedMapOnlyMarksMovedSegments() {
    AlignedTVList tvList = AlignedTVList.newAlignedList(Arrays.asList(TSDataType.INT64));
    int rowCount = ARRAY_SIZE + 3;
    for (int i = 0; i < rowCount; i++) {
      // Keep the first segment ordered and introduce an inversion only in the second segment.
      long time =
          i < ARRAY_SIZE
              ? i
              : (i == ARRAY_SIZE
                  ? ARRAY_SIZE + 1L
                  : i == ARRAY_SIZE + 1 ? ARRAY_SIZE + 2L : ARRAY_SIZE);
      tvList.putAlignedValue(time, new Object[] {(long) i});
    }

    tvList.sort();

    Assert.assertNotNull(tvList.getSegmentMovedMap());
    Assert.assertTrue(tvList.getSegmentMovedMap().isMarked(1));
    for (int segmentIndex = 0; segmentIndex < 2; segmentIndex++) {
      boolean moved = false;
      int segmentStart = segmentIndex * ARRAY_SIZE;
      int segmentEnd = Math.min(segmentStart + ARRAY_SIZE, rowCount);
      for (int rowIndex = segmentStart; rowIndex < segmentEnd; rowIndex++) {
        moved |= tvList.getValueIndex(rowIndex) != rowIndex;
      }
      Assert.assertEquals(moved, tvList.getSegmentMovedMap().isMarked(segmentIndex));
    }
    Assert.assertEquals(0, tvList.getValueIndex(0));
    Assert.assertEquals(ARRAY_SIZE + 2, tvList.getValueIndex(ARRAY_SIZE));
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
    long ramSizeBeforeBitmapAllocation = tvList.calculateRamSize().getRamSize();
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
    // Only the lazily allocated bitmap and the slots of its container are charged; no additional
    // value array is materialized.
    Assert.assertEquals(
        AlignedTVList.bitmapRamCost()
            + AlignedTVList.listRamCostWithReferences(tvList.getBitMaps())
            + LazyBlockList.instanceRamCost()
            + RamUsageEstimator.sizeOfObjectArray(3),
        tvList.calculateRamSize().getRamSize() - ramSizeBeforeBitmapAllocation);
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

    long ramSizeBeforeExtension = tvList.calculateRamSize().getRamSize();
    long containerBeforeExtension = tvList.calculateContainerRamCost(null);
    tvList.extendColumn(TSDataType.INT32);

    Assert.assertNull(tvList.getValues().get(2).get(0));
    Assert.assertNull(tvList.getValues().get(2).get(1));
    Assert.assertNull(tvList.getBitMaps().get(2));
    // extendColumn only adds the N-wide container overhead of the new column; neither a value
    // array nor a bitmap is materialized.
    Assert.assertEquals(
        ramSizeBeforeExtension + tvList.calculateContainerRamCost(null) - containerBeforeExtension,
        tvList.calculateRamSize().getRamSize());

    long ramSizeBeforeExtendedColumnMaterialization = tvList.calculateRamSize().getRamSize();
    tvList.putAlignedValue(ARRAY_SIZE + 2L, new Object[] {null, null, 2});

    Assert.assertNull(tvList.getValues().get(2).get(0));
    Assert.assertNotNull(tvList.getValues().get(2).get(1));
    Assert.assertNull(tvList.getBitMaps().get(2).get(0));
    Assert.assertNotNull(tvList.getBitMaps().get(2).get(1));
    Assert.assertTrue(tvList.getBitMaps().get(2).get(1).isMarked(0));
    Assert.assertTrue(tvList.getBitMaps().get(2).get(1).isMarked(1));
    Assert.assertFalse(tvList.getBitMaps().get(2).get(1).isMarked(2));
    Assert.assertTrue(tvList.isNullValue(0, 2));
    Assert.assertFalse(tvList.isNullValue(ARRAY_SIZE + 2, 2));
    Assert.assertEquals(2, tvList.getIntByValueIndex(ARRAY_SIZE + 2, 2));
    // Materializing the extended column charges its primitive array, the value slot, the bitmap
    // and the slots of its container. The implicit null prefix before the first value is
    // represented by the bitmap only, never by an Object[] prefix.
    Assert.assertEquals(
        AlignedTVList.primitiveArrayMemCost(TSDataType.INT32)
            + RamUsageEstimator.sizeOfObjectArray(2)
            + AlignedTVList.bitmapRamCost()
            + LazyBlockList.instanceRamCost()
            + RamUsageEstimator.sizeOfObjectArray(2),
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

    // Materializing the second column charges its primitive array, the value slot, the bitmap and
    // the slots of its container.
    Assert.assertEquals(
        AlignedTVList.primitiveArrayMemCost(TSDataType.INT64)
            + RamUsageEstimator.sizeOfObjectArray(2)
            + AlignedTVList.bitmapRamCost()
            + AlignedTVList.listRamCostWithReferences(tvList.getBitMaps())
            + LazyBlockList.instanceRamCost()
            + RamUsageEstimator.sizeOfObjectArray(2),
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
            + AlignedTVList.primitiveArrayMemCost(TSDataType.INT64)
            + AlignedTVList.bitmapRamCost()
            + projectedTvList.calculateContainerRamCost(null),
        projectedTvList.calculateRamSize().getRamSize());

    tvList.clear();
    // clear() keeps the N-wide containers for reuse, so only the retained (empty) container
    // baseline remains charged; no per-block, materialized-array or bitmap payload is left.
    Assert.assertEquals(
        tvList.calculateContainerRamCost(null), tvList.calculateRamSize().getRamSize());
  }

  @Test
  public void testCalculateRamSizeExcludesUnallocatedPrimitiveArrays() {
    AlignedTVList tvList =
        AlignedTVList.newAlignedList(Arrays.asList(TSDataType.INT64, TSDataType.INT64));
    for (int i = 0; i <= ARRAY_SIZE; i++) {
      tvList.putAlignedValue(i, new Object[] {(long) i, null});
    }

    int blockCount = tvList.getValues().get(0).size();
    // The second column is never materialized (only nulls were written), so only the first
    // column's primitive arrays are charged; the N-wide container baseline is added once.
    long expectedRamSize =
        (long) blockCount * tvList.alignedTvListArrayMemCostWithoutPrimitiveArrays()
            + (long) blockCount * AlignedTVList.primitiveArrayMemCost(TSDataType.INT64)
            + tvList.calculateContainerRamCost(null);

    Assert.assertEquals(expectedRamSize, tvList.calculateRamSize().getRamSize());
    Assert.assertNull(tvList.getBitMaps());
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
    Assert.assertNull(tvList.getBitMaps());
    Assert.assertTrue(tvList.isNullValue(ARRAY_SIZE - 1, 1));
  }

  @Test
  public void testImplicitNullBlocksAndAllValueDeletedMap() {
    AlignedTVList tvList =
        AlignedTVList.newAlignedList(Arrays.asList(TSDataType.INT64, TSDataType.INT64));
    for (int i = 0; i <= ARRAY_SIZE; i++) {
      tvList.putAlignedValue(i, new Object[] {null, null});
    }

    Assert.assertNull(tvList.getBitMaps());
    Assert.assertNull(tvList.getValues().get(0).get(0));
    Assert.assertNull(tvList.getValues().get(0).get(1));
    Assert.assertNull(tvList.getValues().get(1).get(0));
    Assert.assertNull(tvList.getValues().get(1).get(1));
    BitMap allValueDeletedMap = tvList.getAllValueColDeletedMap();
    for (int i = 0; i <= ARRAY_SIZE; i++) {
      Assert.assertTrue(allValueDeletedMap.isMarked(i));
    }

    tvList.putAlignedValue(ARRAY_SIZE + 1L, new Object[] {1L, null});
    tvList.putAlignedValue(ARRAY_SIZE + 2L, new Object[] {null, 2L});

    Assert.assertNull(tvList.getBitMaps().get(0).get(0));
    Assert.assertNull(tvList.getBitMaps().get(1).get(0));
    Assert.assertNotNull(tvList.getBitMaps().get(0).get(1));
    Assert.assertNotNull(tvList.getBitMaps().get(1).get(1));
    allValueDeletedMap = tvList.getAllValueColDeletedMap();
    for (int i = 0; i <= ARRAY_SIZE; i++) {
      Assert.assertTrue(allValueDeletedMap.isMarked(i));
    }
    Assert.assertFalse(allValueDeletedMap.isMarked(ARRAY_SIZE + 1));
    Assert.assertFalse(allValueDeletedMap.isMarked(ARRAY_SIZE + 2));
  }

  @Test
  public void testDeletingImplicitNullColumnDoesNotMaterializeBitmaps() {
    AlignedTVList tvList =
        AlignedTVList.newAlignedList(Arrays.asList(TSDataType.INT64, TSDataType.INT64));
    for (int i = 0; i < ARRAY_SIZE; i++) {
      tvList.putAlignedValue(i, new Object[] {(long) i, null});
    }

    Assert.assertEquals(0, (int) tvList.delete(0, ARRAY_SIZE - 1L, 1).left);
    Assert.assertNull(tvList.getBitMaps());
    tvList.deleteColumn(1);
    Assert.assertNull(tvList.getBitMaps());

    tvList.deleteColumn(0);
    Assert.assertNotNull(tvList.getBitMaps().get(0).get(0));
    Assert.assertNull(tvList.getBitMaps().get(1));
    for (int i = 0; i < ARRAY_SIZE; i++) {
      Assert.assertTrue(tvList.isNullValue(i, 0));
      Assert.assertTrue(tvList.isNullValue(i, 1));
    }
  }

  @Test
  public void testAllValueDeletedMapMatchesPerColumnNullState() {
    AlignedTVList tvList =
        AlignedTVList.newAlignedList(
            Arrays.asList(TSDataType.INT64, TSDataType.INT64, TSDataType.INT64));
    int rowCount = ARRAY_SIZE * 2 + 7;
    for (int row = 0; row < rowCount; row++) {
      tvList.putAlignedValue(
          row,
          new Object[] {
            row % 3 == 0 ? null : (long) row,
            row % 5 == 0 ? null : (long) row,
            row < ARRAY_SIZE || row % 7 == 0 ? null : (long) row
          });
    }

    BitMap allValueDeletedMap = tvList.getAllValueColDeletedMap();
    for (int row = 0; row < rowCount; row++) {
      boolean allNull = true;
      for (int column = 0; column < 3; column++) {
        allNull &= tvList.isNullValue(row, column);
      }
      Assert.assertEquals(allNull, allValueDeletedMap != null && allValueDeletedMap.isMarked(row));
    }
  }

  @Test
  public void testNullPrimitiveArrayCanBeClonedAndSerialized() throws IOException {
    // Entirely null blocks must remain lazy during cloning and retain WAL placeholders.
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
    Assert.assertEquals(tvList.serializedSize(), walBuffer.getBuffer().position());
    AlignedTVList deserializedTvList =
        AlignedTVList.deserialize(
            new DataInputStream(new ByteArrayInputStream(walBuffer.getBuffer().array())));

    Assert.assertEquals(tvList.rowCount(), deserializedTvList.rowCount());
    Assert.assertEquals("[null, null]", deserializedTvList.getAlignedValue(0).toString());
    Assert.assertEquals(
        "[value, 1]", deserializedTvList.getAlignedValue(ARRAY_SIZE + 1).toString());
  }

  @Test
  public void testNullBlocksSerializeForAllScalarTypes() throws IOException {
    // Cover every WAL scalar writer with an unmaterialized block and a later real value.
    List<TSDataType> dataTypes =
        Arrays.asList(
            TSDataType.BOOLEAN,
            TSDataType.INT32,
            TSDataType.DATE,
            TSDataType.INT64,
            TSDataType.TIMESTAMP,
            TSDataType.FLOAT,
            TSDataType.DOUBLE,
            TSDataType.TEXT,
            TSDataType.STRING,
            TSDataType.BLOB,
            TSDataType.OBJECT);
    Binary binary = new Binary("value", TSFileConfig.STRING_CHARSET);
    Object[] rowValues = {true, 7, 20260908, 9L, 10L, 1.25F, 2.5D, binary, binary, binary, binary};
    AlignedTVList list = AlignedTVList.newAlignedList(dataTypes);
    for (int row = 0; row <= ARRAY_SIZE; row++) {
      list.putAlignedValue(row, new Object[dataTypes.size()]);
    }
    list.putAlignedValue(ARRAY_SIZE + 1L, rowValues);
    AlignedTVList cloned = list.clone();
    for (int column = 0; column < dataTypes.size(); column++) {
      Assert.assertNull(cloned.getValues().get(column).get(0));
    }

    WALByteBufferForTest buffer =
        new WALByteBufferForTest(ByteBuffer.allocate(list.serializedSize()));
    list.serializeToWAL(buffer);
    Assert.assertEquals(list.serializedSize(), buffer.getBuffer().position());
    DataInputStream stream =
        new DataInputStream(new ByteArrayInputStream(buffer.getBuffer().array()));
    AlignedTVList restored = AlignedTVList.deserialize(stream);
    Assert.assertEquals(-1, stream.read());
    Assert.assertEquals(list.rowCount(), restored.rowCount());
    for (int row = 0; row <= ARRAY_SIZE; row++) {
      for (int column = 0; column < dataTypes.size(); column++) {
        Assert.assertTrue(restored.isNullValue(row, column));
      }
    }
    Assert.assertEquals(
        list.getAlignedValue(ARRAY_SIZE + 1).toString(),
        restored.getAlignedValue(ARRAY_SIZE + 1).toString());
  }

  @Test
  public void testSerializedSizeIncludesDeletedTimeBitmap() throws IOException {
    AlignedTVList list = AlignedTVList.newAlignedList(Arrays.asList(TSDataType.INT64));
    for (int i = 0; i < ARRAY_SIZE + 3; i++) {
      list.putAlignedValue(i, new Object[] {(long) i});
    }
    list.deleteTime(1, ARRAY_SIZE);
    // Exact allocation used by WAL must include both values and the deleted-time bitmap.
    WALByteBufferForTest buffer =
        new WALByteBufferForTest(ByteBuffer.allocate(list.serializedSize()));
    list.serializeToWAL(buffer);
    Assert.assertEquals(list.serializedSize(), buffer.getBuffer().position());
    DataInputStream stream =
        new DataInputStream(new ByteArrayInputStream(buffer.getBuffer().array()));
    AlignedTVList restored = AlignedTVList.deserialize(stream);
    Assert.assertEquals(-1, stream.read());
    Assert.assertEquals(list.rowCount(), restored.rowCount());
    Assert.assertEquals(
        list.getAlignedValue(ARRAY_SIZE + 2).toString(),
        restored.getAlignedValue(ARRAY_SIZE + 2).toString());
    for (int i = 0; i < list.rowCount(); i++) {
      Assert.assertEquals(list.isTimeDeleted(i), restored.isTimeDeleted(i));
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

  @Test
  public void testMovesUnclonedColumns() {
    List<TSDataType> dataTypes = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      dataTypes.add(TSDataType.INT64);
    }
    AlignedTVList tvList = AlignedTVList.newAlignedList(dataTypes);
    tvList.putAlignedValue(0, new Object[] {1L, 2L, null});

    Set<Integer> columnsToClone = Collections.singleton(1);
    long retainedRamSize = tvList.calculateRamSize(columnsToClone).getRamSize();
    AlignedTVList.PartialClonePlan partialClonePlan = tvList.preparePartialClone(columnsToClone);
    AlignedTVList clonedTvList = partialClonePlan.getCloneList();

    Assert.assertNotNull(tvList.getValues().get(0));
    Assert.assertNotNull(tvList.getValues().get(2));
    Assert.assertEquals(1L, tvList.getLongByValueIndex(0, 0));
    Assert.assertTrue(tvList.isNullValue(0, 2));
    Assert.assertEquals(2L, clonedTvList.getLongByValueIndex(0, 1));

    partialClonePlan.commit();

    Assert.assertNull(tvList.getValues().get(0));
    Assert.assertNull(tvList.getValues().get(2));
    Assert.assertTrue(tvList.isNullValue(0, 0));
    Assert.assertTrue(tvList.isNullValue(0, 2));
    Assert.assertEquals(1L, clonedTvList.getLongByValueIndex(0, 0));
    Assert.assertEquals(2L, clonedTvList.getLongByValueIndex(0, 1));
    Assert.assertTrue(clonedTvList.isNullValue(0, 2));
    Assert.assertEquals(retainedRamSize, tvList.calculateRamSize().getRamSize());
  }

  @Test
  public void testPartialRamSizeScalesWithRetainedColumns() {
    int columnCount = 256;
    List<TSDataType> dataTypes = new ArrayList<>(columnCount);
    Object[] values = new Object[columnCount];
    for (int i = 0; i < columnCount; i++) {
      dataTypes.add(TSDataType.INT64);
      values[i] = (long) i;
    }

    AlignedTVList tvList = AlignedTVList.newAlignedList(dataTypes);
    tvList.putAlignedValue(1, values);
    Set<Integer> retainedColumns = Collections.singleton(0);
    long retainedRamSize = tvList.calculateRamSize(retainedColumns).getRamSize();
    long fullRamSize = tvList.calculateRamSize().getRamSize();

    // The N-wide container baseline is retained regardless of how many columns are cloned, so
    // exclude it to verify that the per-column payload scales with the retained column count:
    // keeping 1 of 256 columns must cost far less than the full list.
    long retainedPayload = retainedRamSize - tvList.calculateContainerRamCost(retainedColumns);
    long fullPayload = fullRamSize - tvList.calculateContainerRamCost(null);
    Assert.assertTrue(retainedPayload < fullPayload / 64);

    AlignedTVList.PartialClonePlan plan = tvList.preparePartialClone(retainedColumns);
    plan.commit();
    Assert.assertEquals(retainedRamSize, tvList.calculateRamSize().getRamSize());
  }

  @Test
  public void testPartialReservationMatchesCleanupCalculation() {
    for (boolean createIndices : new boolean[] {false, true}) {
      for (boolean retainValueColumn : new boolean[] {false, true}) {
        AlignedTVList tvList =
            AlignedTVList.newAlignedList(
                new ArrayList<>(
                    Arrays.asList(TSDataType.INT64, TSDataType.INT64, TSDataType.INT64)));
        for (int i = 0; i <= ARRAY_SIZE; i++) {
          long time = createIndices ? ARRAY_SIZE - i : i;
          tvList.putAlignedValue(
              time, new Object[] {(long) i, i % 2 == 0 ? null : (long) i, (long) i});
        }
        if (createIndices) {
          Assert.assertFalse(tvList.isSorted());
          tvList.sort();
          Assert.assertNotNull(tvList.getIndices());
        } else {
          Assert.assertNull(tvList.getIndices());
        }

        Set<Integer> retainedColumns =
            retainValueColumn ? Collections.singleton(1) : Collections.emptySet();
        long reservedMemoryBytes = tvList.calculateRamSize(retainedColumns).getRamSize();
        tvList.setReservedMemoryBytes(reservedMemoryBytes);

        AlignedTVList.PartialClonePlan plan = tvList.preparePartialClone(retainedColumns);
        plan.commit();

        long cleanupMemoryBytes = tvList.calculateRamSize().getRamSize();
        String scenario =
            String.format(
                "createIndices=%s, retainValueColumn=%s", createIndices, retainValueColumn);
        Assert.assertEquals(scenario, reservedMemoryBytes, cleanupMemoryBytes);
        Assert.assertEquals(scenario, tvList.getReservedMemoryBytes(), cleanupMemoryBytes);
      }
    }
  }

  @Test
  public void testPartialCloneFailureLeavesSourceUntouched() {
    AlignedTVList tvList =
        AlignedTVList.newAlignedList(
            Arrays.asList(TSDataType.INT64, TSDataType.INT64, TSDataType.INT64));
    // Materialize the first column before writing a null so master creates its lazy bitmap.
    tvList.putAlignedValue(0, new Object[] {1L, 2L, 3L});
    tvList.putAlignedValue(1, new Object[] {null, 4L, 5L});

    List<Object> firstColumnValues = tvList.getValues().get(0);
    List<Object> secondColumnValues = tvList.getValues().get(1);
    List<Object> thirdColumnValues = tvList.getValues().get(2);
    List<BitMap> firstColumnBitMaps = tvList.getBitMaps().get(0);
    Object invalidThirdColumnArray = new int[ARRAY_SIZE];
    thirdColumnValues.set(0, invalidThirdColumnArray);

    Set<Integer> columnsToClone = new HashSet<>(Arrays.asList(0, 1, 2));
    Assert.assertThrows(ClassCastException.class, () -> tvList.preparePartialClone(columnsToClone));

    Assert.assertSame(firstColumnValues, tvList.getValues().get(0));
    Assert.assertSame(secondColumnValues, tvList.getValues().get(1));
    Assert.assertSame(thirdColumnValues, tvList.getValues().get(2));
    Assert.assertSame(invalidThirdColumnArray, tvList.getValues().get(2).get(0));
    Assert.assertSame(firstColumnBitMaps, tvList.getBitMaps().get(0));
    Assert.assertTrue(tvList.isNullValue(1, 0));
    Assert.assertEquals(2L, tvList.getLongByValueIndex(0, 1));
  }

  @Test
  public void testReleaseNonQueryColumnsWithBitmaps() {
    List<TSDataType> dataTypes = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      dataTypes.add(TSDataType.INT64);
    }
    AlignedTVList tvList = AlignedTVList.newAlignedList(dataTypes);
    for (int i = 0; i < 100; i++) {
      Object[] values = new Object[3];
      values[0] = (long) i;
      // Alternate non-null and null values so the lazily allocated value array and bitmap both
      // exist in every block.
      values[1] = i % 2 == 0 ? (long) i : null;
      values[2] = (long) (i * 100);
      tvList.putAlignedValue(i, values);
    }

    // Verify bitmaps were created for column 1
    Assert.assertNotNull(tvList.getBitMaps());
    Assert.assertNotNull(tvList.getBitMaps().get(1));

    // Keep only column 0 and 2, release column 1
    Set<Integer> columnsToKeep = new HashSet<>(Arrays.asList(0, 2));
    tvList.releaseNonQueryColumns(columnsToKeep);

    // Verify column 1 is released
    Assert.assertNull(tvList.getValues().get(1));
    Assert.assertNull(tvList.getBitMaps().get(1));

    // Verify columns 0 and 2 are intact
    Assert.assertFalse(tvList.getValues().get(0).isEmpty());
    Assert.assertFalse(tvList.getValues().get(2).isEmpty());
    for (int i = 0; i < 100; i++) {
      Assert.assertEquals((long) i, tvList.getLongByValueIndex(i, 0));
      Assert.assertEquals((long) (i * 100), tvList.getLongByValueIndex(i, 2));
    }
  }

  @Test
  public void testSparseAlignedValuesMapColumnsBitmapsAndResults() {
    AlignedTVList tvList =
        AlignedTVList.newAlignedList(
            new ArrayList<>(Arrays.asList(TSDataType.INT64, TSDataType.BOOLEAN, TSDataType.TEXT)));
    int rowCount = ARRAY_SIZE + 2;
    long[] times = new long[rowCount];
    long[] first = new long[rowCount];
    Binary[] third = new Binary[rowCount];
    BitMap thirdBitmap = new BitMap(rowCount);
    for (int i = 0; i < rowCount; i++) {
      times[i] = i;
      first[i] = i * 10L;
      third[i] = new Binary("v" + i, TSFileConfig.STRING_CHARSET);
    }
    thirdBitmap.mark(1);
    thirdBitmap.mark(ARRAY_SIZE + 1);
    TSStatus[] results = new TSStatus[rowCount];
    results[ARRAY_SIZE] = new TSStatus(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode());

    tvList.putAlignedValues(
        times,
        new Object[] {first, third},
        new BitMap[] {null, thirdBitmap},
        new int[] {0, 2},
        0,
        rowCount,
        results);

    Assert.assertEquals(rowCount, tvList.rowCount());
    for (int i = 0; i < rowCount; i++) {
      if (i == ARRAY_SIZE) {
        // A failed row is masked in every column written by this call.
        Assert.assertTrue(tvList.isNullValue(i, 0));
      } else {
        Assert.assertEquals(i * 10L, tvList.getLongByValueIndex(i, 0));
      }
      Assert.assertTrue(tvList.isNullValue(i, 1));
      if (i == 1 || i == ARRAY_SIZE || i == ARRAY_SIZE + 1) {
        Assert.assertTrue(tvList.isNullValue(i, 2));
      } else {
        Assert.assertEquals("v" + i, tvList.getBinaryByValueIndex(i, 2).toString());
      }
    }
    tvList.clear();
  }

  @Test
  public void testStructureRamCostCacheTracksStructuralChanges() {
    AlignedTVList tvList =
        AlignedTVList.newAlignedList(
            new ArrayList<>(Arrays.asList(TSDataType.INT64, TSDataType.INT32)));
    assertStructureRamCostMatches(tvList);

    tvList.putAlignedValue(0, new Object[] {1L, 2});
    assertStructureRamCostMatches(tvList);

    tvList.extendColumn(TSDataType.TEXT);
    assertStructureRamCostMatches(tvList);

    // Materialize a bitmap container for both existing columns.
    tvList.putAlignedValue(1, new Object[] {null, null, null});
    assertStructureRamCostMatches(tvList);

    // Cross a block boundary to change the value-column container sizes.
    for (int i = 2; i <= ARRAY_SIZE; i++) {
      tvList.putAlignedValue(i, new Object[] {(long) i, i, null});
    }
    Assert.assertFalse(tvList.getValues().get(0).isEmpty());
    assertStructureRamCostMatches(tvList);

    tvList.releaseNonQueryColumns(Collections.singleton(0));
    assertStructureRamCostMatches(tvList);

    tvList.clear();
    assertStructureRamCostMatches(tvList);
  }

  private static void assertStructureRamCostMatches(AlignedTVList tvList) {
    long expected = tvList.calculateContainerRamCost(null);
    Assert.assertEquals(expected, tvList.getStructureRamSize());
    // The second call exercises the cached path.
    Assert.assertEquals(expected, tvList.getStructureRamSize());
  }

  @Test
  public void testAllNullRowIsReportedAsNull() {
    // A single-column list has no "untouched column" that would make hasNull true, so an all-null
    // row (a row that writes no measurement at all) must still be reported as null.
    AlignedTVList tvList =
        AlignedTVList.newAlignedList(new ArrayList<>(Collections.singletonList(TSDataType.INT64)));
    tvList.putAlignedValue(0, new Object[] {1L}, new int[] {0});
    tvList.putAlignedValue(1, new Object[0], new int[0]);
    Assert.assertFalse(tvList.isNullValue(0, 0));
    Assert.assertTrue(tvList.isNullValue(1, 0));
    Assert.assertEquals(2, tvList.rowCount());
    tvList.clear();
  }

  @Test
  public void testNullCellsAreNeverMaterializedAsDefaultValues() {
    // A block that holds no value for a column must stay "no array at all" so that null is never
    // confused with a default value such as 0. This is the same representation the list used before
    // the sparse-write rework.
    AlignedTVList tvList =
        AlignedTVList.newAlignedList(
            new ArrayList<>(Arrays.asList(TSDataType.INT64, TSDataType.INT64)));
    for (int row = 0; row < ARRAY_SIZE; row++) {
      tvList.putAlignedValue(row, new Object[] {(long) row, null});
    }
    // Column 1 never got a value in block 0: the slot is null, not a zero-filled array.
    Assert.assertNull(tvList.getValues().get(1).get(0));
    Assert.assertTrue(tvList.getBitMaps() == null || tvList.getBitMaps().get(1) == null);
    for (int row = 0; row < ARRAY_SIZE; row++) {
      Assert.assertTrue(tvList.isNullValue(row, 1));
    }

    // The first real value in block 1 materializes only that block; block 0 stays null.
    tvList.putAlignedValue(ARRAY_SIZE, new Object[] {(long) ARRAY_SIZE, 7L});
    Assert.assertNull(tvList.getValues().get(1).get(0));
    Assert.assertNotNull(tvList.getValues().get(1).get(1));
    Assert.assertFalse(tvList.isNullValue(ARRAY_SIZE, 1));
    Assert.assertTrue(tvList.isNullValue(ARRAY_SIZE - 1, 1));

    // An explicit null is recorded in the bitmap and must not be read back as a value.
    tvList.putAlignedValue(ARRAY_SIZE + 1L, new Object[] {(long) ARRAY_SIZE + 1, null});
    Assert.assertTrue(tvList.isNullValue(ARRAY_SIZE + 1, 1));
    Assert.assertTrue(tvList.getBitMaps().get(1).get(1).isMarked(1));
    Assert.assertFalse(tvList.isNullValue(ARRAY_SIZE, 1));

    // The flush snapshot must report exactly the same nulls.
    tvList.materializeOpenBlockForRead();
    Assert.assertNull(tvList.getValues().get(1).get(0));
    for (int row = 0; row < ARRAY_SIZE; row++) {
      Assert.assertTrue(tvList.isNullValue(row, 1));
    }
    Assert.assertFalse(tvList.isNullValue(ARRAY_SIZE, 1));
    Assert.assertTrue(tvList.isNullValue(ARRAY_SIZE + 1, 1));
    tvList.clear();
  }

  @Test
  public void testStructureRamCostTracksLazyIndicesAfterSort() {
    AlignedTVList tvList =
        AlignedTVList.newAlignedList(new ArrayList<>(Collections.singletonList(TSDataType.INT64)));
    // Out-of-order rows force sort() to lazily materialize the indices container and the
    // moved-segment bitmap, both of which are part of the container cost.
    long[] times = {5, 3, 4, 1, 2};
    for (long time : times) {
      tvList.putAlignedValue(time, new Object[] {time});
    }
    assertStructureRamCostMatches(tvList);
    long beforeSort = tvList.getStructureRamSize();

    tvList.sort();
    assertStructureRamCostMatches(tvList);
    Assert.assertTrue(tvList.getStructureRamSize() >= beforeSort);
    Assert.assertNotNull(tvList.getSegmentMovedMap());

    // Sorting an already sorted list must keep the cached cost valid.
    tvList.sort();
    assertStructureRamCostMatches(tvList);
    tvList.clear();
  }

  @Test
  public void testExtendedColumnsOverManyBlocksPreserveNulls() {
    AlignedTVList tvList =
        AlignedTVList.newAlignedList(new ArrayList<>(Collections.singletonList(TSDataType.INT64)));
    int rows = ARRAY_SIZE * 3 + 7;
    for (int i = 0; i < rows; i++) {
      tvList.putAlignedValue(i, new Object[] {(long) i});
    }

    int newColumns = 512;
    tvList.extendColumns(Collections.nCopies(newColumns, TSDataType.INT32));

    // Only the last new column is written; the other new columns stay null in every block.
    tvList.putAlignedValue(rows, new Object[] {7}, new int[] {newColumns});
    for (int column = 1; column <= newColumns; column++) {
      Assert.assertNull(tvList.getValues().get(column).get(0));
    }
    Assert.assertEquals(7, tvList.getIntByValueIndex(rows, newColumns));
    for (int column = 1; column < newColumns; column++) {
      Assert.assertTrue(tvList.isNullValue(rows, column));
    }
    tvList.clear();
  }

  @Test
  public void testStructureRamCostIncludesWriteMetadata() {
    int columns = 1024;
    AlignedTVList tvList =
        AlignedTVList.newAlignedList(
            new ArrayList<>(Collections.nCopies(columns, TSDataType.INT64)));

    // The per-column open-block state (last written row per column and the touched-column bitset)
    // must be reported by the container cost, not only the pre-existing accounting arrays. The
    // columnTypes cache is one reference per column; the two int arrays are the materialized-array
    // counts and blockLastRow.
    long expected =
        AlignedTVList.listRamCostWithReferences(tvList.getTsDataTypes())
            + RamUsageEstimator.sizeOfObjectArray(columns)
            + RamUsageEstimator.sizeOfLongArray(columns)
            + 2L * RamUsageEstimator.sizeOfIntArray(columns)
            + new BitMap(columns).ramBytesUsed()
            + AlignedTVList.listRamCostWithoutReferences(tvList.getTimestamps())
            + AlignedTVList.listRamCostWithReferences(tvList.getValues())
            + (long) columns * LazyBlockList.instanceRamCost();
    Assert.assertEquals(expected, tvList.getStructureRamSize());
    tvList.clear();
  }

  @Test
  public void testLazyColumnPrefixIsAccountedOnlyWhenMaterialized() {
    // With many blocks the lazy wrapper is cheaper than one reference per block, which is the
    // scenario the representation is meant for.
    int blocks = 64;
    int rows = blocks * ARRAY_SIZE - 1;
    AlignedTVList tvList =
        AlignedTVList.newAlignedList(new ArrayList<>(Collections.singletonList(TSDataType.INT64)));
    for (int i = 0; i < rows; i++) {
      tvList.putAlignedValue(i, new Object[] {(long) i});
    }
    Assert.assertNull(tvList.getBitMaps());

    int extended = 64;
    int columnsAfterExtension = 1 + extended;
    long beforeExtension = tvList.getStructureRamSize();
    tvList.extendColumns(Collections.nCopies(extended, TSDataType.INT32));
    long afterExtension = tvList.getStructureRamSize();

    long expectedExtension =
        3L
                * (RamUsageEstimator.sizeOfObjectArray(columnsAfterExtension)
                    - RamUsageEstimator.sizeOfObjectArray(1))
            + (RamUsageEstimator.sizeOfLongArray(columnsAfterExtension)
                - RamUsageEstimator.sizeOfLongArray(1))
            + (RamUsageEstimator.sizeOfIntArray(columnsAfterExtension)
                - RamUsageEstimator.sizeOfIntArray(1))
            + (RamUsageEstimator.sizeOfIntArray(columnsAfterExtension)
                - RamUsageEstimator.sizeOfIntArray(1))
            + (new BitMap(columnsAfterExtension).ramBytesUsed() - new BitMap(1).ramBytesUsed())
            + (long) extended * lazyColumnContainerRamCost();
    // Each lazy column also carries one null slot for the open block, so allow a small per-column
    // slack instead of pinning the exact wrapper capacity.
    long extensionGrowth = afterExtension - beforeExtension;
    Assert.assertTrue(extensionGrowth >= expectedExtension);
    Assert.assertTrue(extensionGrowth <= expectedExtension + 16L * extended);
    // The lazy wrapper costs less than a dense array of block references at this block count.
    Assert.assertTrue(lazyColumnContainerRamCost() < RamUsageEstimator.sizeOfObjectArray(blocks));
    assertStructureRamCostMatches(tvList);

    long beforeMaterialization = tvList.getStructureRamSize();
    tvList.putAlignedValue(rows, new Object[] {7}, new int[] {extended});
    long afterMaterialization = tvList.getStructureRamSize();

    // The implicit null prefix is never materialized; only the bitmap for the written block is
    // allocated, and the cached cost must be refreshed.
    Assert.assertNull(tvList.getValues().get(extended).get(0));
    Assert.assertNotNull(tvList.getBitMaps());
    long expectedMaterialization = 0;
    expectedMaterialization += AlignedTVList.listRamCostWithReferences(tvList.getBitMaps());
    for (List<BitMap> columnBitMaps : tvList.getBitMaps()) {
      if (columnBitMaps != null) {
        expectedMaterialization += AlignedTVList.listRamCostWithoutReferences(columnBitMaps);
      }
    }
    long materializationGrowth = afterMaterialization - beforeMaterialization;
    Assert.assertTrue(materializationGrowth >= expectedMaterialization);
    Assert.assertTrue(materializationGrowth <= expectedMaterialization + 1024);
    assertStructureRamCostMatches(tvList);
    tvList.clear();
  }

  @Test
  public void testAllValueColDeletedMapDoesNotMaterializeOpenBlockTail() {
    AlignedTVList tvList =
        AlignedTVList.newAlignedList(
            new ArrayList<>(Arrays.asList(TSDataType.INT64, TSDataType.INT64)));
    tvList.putAlignedValue(0, new Object[] {1L, 2L});
    // The second row only writes the first column, so the open block has an implicit null tail for
    // the second column that is not materialized in its bitmap yet.
    tvList.putAlignedValue(1, new Object[] {3L}, new int[] {0});
    Assert.assertNull(tvList.getBitMaps());

    Assert.assertNull(tvList.getAllValueColDeletedMap());

    // Computing the all-null map runs on the query thread and must not materialize the tail: a
    // concurrent flush or query may move or release columns of this list at the same time.
    Assert.assertNull(tvList.getBitMaps());
    Assert.assertTrue(tvList.isNullValue(1, 1));
  }

  @Test
  public void testAllValueColDeletedMapAfterDeleteAllAndReAdd() {
    AlignedTVList tvList =
        AlignedTVList.newAlignedList(
            new ArrayList<>(Arrays.asList(TSDataType.INT64, TSDataType.INT64)));
    tvList.putAlignedValue(9, new Object[] {1L, 2L});
    tvList.putAlignedValue(11, new Object[] {3L, 4L});
    // DELETE TIMESERIES deletes every row of every column and removes the measurements, then the
    // aligned timeseries are re-created and written again.
    tvList.deleteColumn(0);
    tvList.deleteColumn(1);
    tvList.extendColumns(new ArrayList<>(Arrays.asList(TSDataType.INT64, TSDataType.INT64)));
    tvList.putAlignedValue(9, new Object[] {5L, 6L}, new int[] {2, 3});
    tvList.putAlignedValue(11, new Object[] {7L}, new int[] {2});

    BitMap allValueDeletedMap = tvList.getAllValueColDeletedMap();
    Assert.assertTrue(allValueDeletedMap.isMarked(0));
    Assert.assertTrue(allValueDeletedMap.isMarked(1));
    Assert.assertFalse(allValueDeletedMap.isMarked(2));
    Assert.assertFalse(allValueDeletedMap.isMarked(3));
  }

  @Test
  public void testAllValueColDeletedMapMatchesSparseWriteNullState() {
    int columnCount = 4;
    List<TSDataType> dataTypes = new ArrayList<>();
    for (int i = 0; i < columnCount; i++) {
      dataTypes.add(TSDataType.INT64);
    }
    AlignedTVList tvList = AlignedTVList.newAlignedList(dataTypes);
    Random random = new Random(20261123L);
    int rowCount = ARRAY_SIZE + 9;
    for (int row = 0; row < rowCount; row++) {
      // Write a random subset of the columns so that most rows only have implicit nulls for the
      // columns they do not write.
      int[] destColumns = new int[columnCount];
      Object[] values = new Object[columnCount];
      int writtenColumns = 0;
      for (int column = 0; column < columnCount; column++) {
        if (random.nextBoolean()) {
          destColumns[writtenColumns] = column;
          values[writtenColumns] = (long) row * columnCount + column;
          writtenColumns++;
        }
      }
      if (writtenColumns == 0) {
        // An all-null row.
        for (int column = 0; column < columnCount; column++) {
          destColumns[column] = column;
          values[column] = null;
        }
        writtenColumns = columnCount;
      }
      tvList.putAlignedValue(
          row, Arrays.copyOf(values, writtenColumns), Arrays.copyOf(destColumns, writtenColumns));
    }

    BitMap allValueDeletedMap = tvList.getAllValueColDeletedMap();
    for (int row = 0; row < rowCount; row++) {
      boolean allNull = true;
      for (int column = 0; column < columnCount; column++) {
        allNull &= tvList.isNullValue(row, column);
      }
      Assert.assertEquals(
          "row " + row, allNull, allValueDeletedMap != null && allValueDeletedMap.isMarked(row));
    }
  }

  private static long lazyColumnContainerRamCost() {
    return LazyBlockList.instanceRamCost();
  }

  @Test
  public void testUnwrittenColumnsRetainNoBlockStorage() {
    int wideColumns = 20_000;
    AlignedTVList emptyList = AlignedTVList.newAlignedList(new ArrayList<>());
    AlignedTVList wideList =
        AlignedTVList.newAlignedList(
            new ArrayList<>(Collections.nCopies(wideColumns, TSDataType.INT64)));

    for (int column = 0; column < wideColumns; column += 997) {
      List<Object> columnValues = wideList.getValues().get(column);
      Assert.assertTrue(columnValues instanceof LazyBlockList);
      Assert.assertTrue(columnValues.isEmpty());
      Assert.assertEquals(
          LazyBlockList.instanceRamCost(), ((LazyBlockList<?>) columnValues).ramCost());
    }
    // The reported cost charges one container per column, nothing per block.
    long perColumnCost =
        (wideList.calculateContainerRamCost(null) - emptyList.calculateContainerRamCost(null))
            / wideColumns;
    Assert.assertTrue("reported per-column container cost: " + perColumnCost, perColumnCost <= 64);
    emptyList.clear();
    wideList.clear();
  }

  @Test
  public void testUnwrittenColumnKeepsBlockSlotsImplicit() {
    AlignedTVList tvList =
        AlignedTVList.newAlignedList(
            Arrays.asList(TSDataType.INT64, TSDataType.INT64, TSDataType.INT64));
    for (int row = 0; row < ARRAY_SIZE * 3; row++) {
      tvList.putAlignedValue(row, new Object[] {(long) row, null, null});
    }
    for (int column = 1; column < 3; column++) {
      Assert.assertEquals(3, tvList.getValues().get(column).size());
      for (int block = 0; block < 3; block++) {
        Assert.assertNull(tvList.getValues().get(column).get(block));
      }
      Assert.assertEquals(
          LazyBlockList.instanceRamCost(),
          ((LazyBlockList<?>) tvList.getValues().get(column)).ramCost());
    }
    Assert.assertTrue(tvList.isNullValue(ARRAY_SIZE * 2 + 5, 1));
    assertStructureRamCostMatches(tvList);

    // Writing the column materializes only the slot of the written block.
    tvList.putAlignedValue(ARRAY_SIZE * 3L, new Object[] {1L, 2L, null});
    Assert.assertNotNull(tvList.getValues().get(1).get(3));
    Assert.assertNull(tvList.getValues().get(2).get(3));
    Assert.assertEquals(
        LazyBlockList.instanceRamCost() + RamUsageEstimator.sizeOfObjectArray(4),
        ((LazyBlockList<?>) tvList.getValues().get(1)).ramCost());
    Assert.assertEquals(
        LazyBlockList.instanceRamCost(), ((LazyBlockList<?>) tvList.getValues().get(2)).ramCost());
    assertStructureRamCostMatches(tvList);
    Assert.assertEquals(tvList.getRamSize(null), tvList.getRamSize());
    Assert.assertEquals(tvList.getRamSize(), tvList.clone().getRamSize());
    tvList.clear();
    Assert.assertEquals(0, tvList.getValues().get(0).size());
  }

  @Test
  public void testBlockPreallocationSwitch() {
    final boolean originalValue =
        IoTDBDescriptor.getInstance().getConfig().isEnableTVListBlockPreallocation();
    try {
      IoTDBDescriptor.getInstance().getConfig().setEnableTVListBlockPreallocation(false);
      AlignedTVList lazyList =
          AlignedTVList.newAlignedList(
              new ArrayList<>(Collections.singletonList(TSDataType.INT64)));
      Assert.assertEquals(
          LazyBlockList.instanceRamCost(),
          ((LazyBlockList<?>) lazyList.getValues().get(0)).ramCost());

      IoTDBDescriptor.getInstance().getConfig().setEnableTVListBlockPreallocation(true);
      // Cold start (fewer than MIN_PREALLOCATION_SAMPLES flushed memtables) falls back to the
      // configured bound, which is what the memchunk passes to the list.
      int hint = TVList.getDefaultArrayNum(null);
      Assert.assertEquals(
          IoTDBDescriptor.getInstance().getConfig().getTvListMaxPreallocatedBlocks(), hint);
      AlignedTVList preallocatedList =
          AlignedTVList.newAlignedList(
              new ArrayList<>(Collections.singletonList(TSDataType.INT64)), true, hint);
      // Nothing is reserved before the column is written, even with pre-allocation enabled.
      Assert.assertEquals(0, preallocatedList.getValues().get(0).size());
      Assert.assertEquals(
          LazyBlockList.instanceRamCost(),
          ((LazyBlockList<?>) preallocatedList.getValues().get(0)).ramCost());

      // The first write that materializes a block reserves the hint in one step; a null-only write
      // materializes nothing.
      preallocatedList.putAlignedValue(0, new Object[] {null});
      Assert.assertTrue(preallocatedList.isNullValue(0, 0));
      Assert.assertEquals(
          LazyBlockList.instanceRamCost(),
          ((LazyBlockList<?>) preallocatedList.getValues().get(0)).ramCost());
      preallocatedList.putAlignedValue(1, new Object[] {1L});
      Assert.assertEquals(1L, preallocatedList.getLongByValueIndex(1, 0));
      long preallocatedCost = ((LazyBlockList<?>) preallocatedList.getValues().get(0)).ramCost();
      Assert.assertEquals(
          LazyBlockList.instanceRamCost() + RamUsageEstimator.sizeOfObjectArray(hint),
          preallocatedCost);
      assertStructureRamCostMatches(preallocatedList);
      lazyList.clear();
      preallocatedList.clear();
    } finally {
      IoTDBDescriptor.getInstance().getConfig().setEnableTVListBlockPreallocation(originalValue);
    }
  }
}
