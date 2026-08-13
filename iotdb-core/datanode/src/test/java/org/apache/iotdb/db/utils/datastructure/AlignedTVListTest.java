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
import org.apache.tsfile.utils.RamUsageEstimator;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static org.apache.iotdb.db.storageengine.rescon.memory.PrimitiveArrayManager.ARRAY_SIZE;
import static org.apache.tsfile.utils.RamUsageEstimator.NUM_BYTES_ARRAY_HEADER;
import static org.apache.tsfile.utils.RamUsageEstimator.NUM_BYTES_OBJECT_REF;

public class AlignedTVListTest {

  // A null-only column keeps a null array slot while a populated column materializes its array.
  @Test
  public void testPrimitiveArraysAreMaterializedOnlyForNonNullColumns() {
    AlignedTVList tvList =
        AlignedTVList.newAlignedList(Arrays.asList(TSDataType.INT64, TSDataType.DOUBLE));

    tvList.putAlignedValue(1, new Object[] {null, 2.0D});
    Assert.assertNull(tvList.getValues().get(0).get(0));
    Assert.assertNotNull(tvList.getValues().get(1).get(0));

    // A row containing only nulls must not allocate another value array.
    tvList.putAlignedValue(2, new Object[] {null, null});
    Assert.assertNull(tvList.getValues().get(0).get(0));
    Assert.assertEquals(1, tvList.getValues().get(1).stream().filter(Objects::nonNull).count());
  }

  // Tablet writes allocate arrays only for input columns that carry a column vector.
  @Test
  public void testBatchWriteMaterializesOnlyColumnsWithValues() {
    AlignedTVList tvList =
        AlignedTVList.newAlignedList(Arrays.asList(TSDataType.INT64, TSDataType.INT32));
    long[] times = {1, 2, 3};
    Object[] columns = {null, new int[] {1, 2, 3}};

    tvList.putAlignedValues(times, columns, null, 0, times.length);
    Assert.assertTrue(tvList.getValues().get(0).stream().allMatch(Objects::isNull));
    Assert.assertNotNull(tvList.getValues().get(1).get(0));
  }

  // Value-array cost excludes bitmap storage because bitmaps are allocated independently.
  @Test
  public void testValueListArrayMemCostExcludesBitmapReservation() {
    long expected = (long) ARRAY_SIZE * Long.BYTES + NUM_BYTES_ARRAY_HEADER + NUM_BYTES_OBJECT_REF;

    Assert.assertEquals(expected, AlignedTVList.valueListArrayMemCost(TSDataType.INT64));
    Assert.assertEquals(
        RamUsageEstimator.shallowSizeOfInstance(BitMap.class)
            + RamUsageEstimator.sizeOfByteArray(ARRAY_SIZE / Byte.SIZE + 1),
        AlignedTVList.bitmapRamCost());
    Assert.assertEquals(NUM_BYTES_OBJECT_REF, AlignedTVList.bitmapReferenceRamCost());
  }

  @Test
  public void testStaticNewAlignedListMemoryCosts() {
    List<TSDataType> dataTypes = Arrays.asList(TSDataType.INT64, TSDataType.INT32);
    AlignedTVList tvList = AlignedTVList.newAlignedList(dataTypes);

    Assert.assertEquals(
        tvList.getRamSize(), AlignedTVList.alignedTvListInitialMemCost(dataTypes.size()));
    long primitiveArrayMemCost =
        dataTypes.stream()
            .mapToLong(
                dataType -> AlignedTVList.valueListArrayMemCost(dataType) - NUM_BYTES_OBJECT_REF)
            .sum();
    Assert.assertEquals(
        tvList.alignedTvListArrayMemCost() - primitiveArrayMemCost,
        AlignedTVList.alignedTvListArrayMemCostWithoutPrimitiveArrays(dataTypes.size()));
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
        ArrayUtils.toPrimitive(timeList.toArray(new Long[0])), vectorArray, null, 0, 1000);
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
        ArrayUtils.toPrimitive(timeList.toArray(new Long[0])), vectorArray, bitMaps, 0, 1000);
    for (long i = 0; i < tvList.rowCount; i++) {
      Assert.assertEquals(tvList.rowCount - i, tvList.getTime((int) i));
      if (i % 100 == 0) {
        Assert.assertEquals(
            "[null, null, null, null, null]", tvList.getAlignedValue((int) i).toString());
      }
    }
  }

  // A null first appears in the third block, so only that block receives a compact bitmap.
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
        ARRAY_SIZE / Byte.SIZE + 1, firstColumnBitMaps.get(2).getByteArray().length);
    Assert.assertTrue(tvList.isNullValue(ARRAY_SIZE * 2 + 1, 0));
    Assert.assertFalse(tvList.isNullValue(ARRAY_SIZE * 2, 0));
    Assert.assertEquals(3, tvList.getValues().get(0).stream().filter(Objects::nonNull).count());
    Assert.assertEquals(3, tvList.getValues().get(1).stream().filter(Objects::nonNull).count());
    Assert.assertEquals(tvList.getRamSize(), tvList.calculateRamSize().getRamSize());
    Assert.assertEquals(tvList.getRamSize(), tvList.clone().getRamSize());
    Assert.assertEquals(tvList.getRamSize(), tvList.cloneForFlushSort().getRamSize());
  }

  // Extending a populated TVList creates null slots but no value arrays or bitmap structures.
  @Test
  public void testExtendColumnDoesNotMaterializeArraysOrBitmaps() {
    AlignedTVList tvList =
        AlignedTVList.newAlignedList(new ArrayList<>(Arrays.asList(TSDataType.INT64)));
    for (int i = 0; i <= ARRAY_SIZE; i++) {
      tvList.putAlignedValue(i, new Object[] {(long) i});
    }

    long ramSizeBeforeExtension = tvList.getRamSize();
    int oldColumnCount = tvList.getTsDataTypes().size();
    long expectedExtensionCost =
        (long) tvList.getTimestamps().size() * NUM_BYTES_OBJECT_REF
            + 2L
                * (RamUsageEstimator.sizeOfObjectArray(oldColumnCount + 1)
                    - RamUsageEstimator.sizeOfObjectArray(oldColumnCount))
            + RamUsageEstimator.sizeOfLongArray(oldColumnCount + 1)
            - RamUsageEstimator.sizeOfLongArray(oldColumnCount)
            + RamUsageEstimator.sizeOfIntArray(oldColumnCount + 1)
            - RamUsageEstimator.sizeOfIntArray(oldColumnCount)
            + RamUsageEstimator.shallowSizeOf(new ArrayList<>())
            + RamUsageEstimator.sizeOfObjectArray(0);
    tvList.extendColumn(TSDataType.INT32);

    Assert.assertEquals(expectedExtensionCost, tvList.getRamSize() - ramSizeBeforeExtension);
    Assert.assertTrue(tvList.getValues().get(1).stream().allMatch(Objects::isNull));
    Assert.assertNull(tvList.getBitMaps());
    tvList.clear();
    Assert.assertTrue(tvList.getRamSize() > 0);
  }

  // An input bitmap without marked values must not create a retained TVList bitmap.
  @Test
  public void testEmptyInputBitmapsDoNotMaterializeMemTableBitmaps() {
    AlignedTVList tvList = AlignedTVList.newAlignedList(Arrays.asList(TSDataType.INT64));
    long[] times = new long[ARRAY_SIZE];
    long[][] values = new long[1][ARRAY_SIZE];
    BitMap[] bitMaps = new BitMap[] {new BitMap(ARRAY_SIZE)};
    for (int i = 0; i < ARRAY_SIZE; i++) {
      times[i] = i;
      values[0][i] = i;
    }

    tvList.putAlignedValues(times, values, bitMaps, 0, ARRAY_SIZE);

    Assert.assertNull(tvList.getBitMaps());
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
        ArrayUtils.toPrimitive(timeList.toArray(new Long[0])), vectorArray, bitMaps, 0, 1000);

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

  // A full clone must carry the same RAM as the source, and the clone must keep accounting
  // correctly when it keeps receiving writes into new primitive-array blocks afterwards.
  @Test
  public void testCloneRamReconciliationAndWriteNewBlockAfterClone() {
    List<TSDataType> dataTypes =
        Arrays.asList(TSDataType.INT64, TSDataType.INT32, TSDataType.DOUBLE);
    AlignedTVList tvList = AlignedTVList.newAlignedList(dataTypes);

    // Span multiple primitive-array blocks, with nulls exercising bitmap accounting.
    int initialRowCount = ARRAY_SIZE * 2 + 1;
    for (int i = 0; i < initialRowCount; i++) {
      tvList.putAlignedValue(
          i, new Object[] {(long) i, (i % 2 == 0) ? null : i, (i % 3 == 0) ? null : (double) i});
    }

    AlignedTVList clonedTvList = tvList.clone();
    Assert.assertEquals(tvList.getRamSize(), clonedTvList.getRamSize());
    Assert.assertEquals(tvList.getRamSize(), tvList.calculateRamSize().getRamSize());
    Assert.assertEquals(clonedTvList.getRamSize(), clonedTvList.calculateRamSize().getRamSize());

    // Keep writing a new block only into the clone. The clone must charge the newly materialized
    // arrays while the source stays untouched.
    long sourceRamAfterClone = tvList.getRamSize();
    int additionalRowCount = ARRAY_SIZE + 1;
    for (int i = 0; i < additionalRowCount; i++) {
      long time = initialRowCount + i;
      clonedTvList.putAlignedValue(time, new Object[] {time, (int) time, (double) time});
    }

    Assert.assertEquals(clonedTvList.getRamSize(), clonedTvList.calculateRamSize().getRamSize());
    Assert.assertTrue(clonedTvList.getRamSize() > sourceRamAfterClone);
    Assert.assertEquals(sourceRamAfterClone, tvList.getRamSize());
    Assert.assertEquals(initialRowCount, tvList.rowCount);
    Assert.assertEquals(initialRowCount + additionalRowCount, clonedTvList.rowCount);
    Assert.assertEquals(
        initialRowCount + additionalRowCount - 1L,
        clonedTvList.getTime(initialRowCount + additionalRowCount - 1));
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
        ArrayUtils.toPrimitive(timeList.toArray(new Long[0])), vectorArray, bitMaps, 0, 10);
    Assert.assertEquals(tvList.memoryBinaryChunkSize[1], 720);

    tvList.delete(5, 15);
    Assert.assertEquals(tvList.memoryBinaryChunkSize[1], 720);

    tvList.deleteColumn(0);
    Assert.assertEquals(tvList.memoryBinaryChunkSize.length, 2);
    Assert.assertEquals(tvList.memoryBinaryChunkSize[1], 720);

    tvList.extendColumn(TSDataType.INT32);
    Assert.assertEquals(tvList.memoryBinaryChunkSize.length, 3);
    Assert.assertEquals(tvList.memoryBinaryChunkSize[1], 720);

    tvList.extendColumn(TSDataType.TEXT);
    Assert.assertEquals(tvList.memoryBinaryChunkSize.length, 4);
    Assert.assertEquals(tvList.memoryBinaryChunkSize[1], 720);
    Assert.assertEquals(tvList.memoryBinaryChunkSize[2], 0);

    tvList.delete(4, 6);
    Assert.assertEquals(tvList.memoryBinaryChunkSize.length, 4);
    Assert.assertEquals(tvList.memoryBinaryChunkSize[1], 720);
    Assert.assertEquals(tvList.memoryBinaryChunkSize[2], 0);

    tvList.clear();
    Assert.assertEquals(tvList.memoryBinaryChunkSize[1], 0);
    Assert.assertEquals(tvList.memoryBinaryChunkSize[2], 0);
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
  public void testPartialRamSizeIncludesWideColumnContainers() {
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
    long primitiveArrayCost =
        (long) tvList.getTimestamps().size() * tvList.alignedTvListArrayMemCost(retainedColumns);
    long retainedRamSize = tvList.calculateRamSize(retainedColumns).getRamSize();

    // memoryBinaryChunkSize and the outer column containers remain N-wide after partial move.
    Assert.assertTrue(retainedRamSize - primitiveArrayCost >= (long) columnCount * Long.BYTES);

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
    tvList.putAlignedValue(0, new Object[] {null, 2L, 3L});

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
    Assert.assertTrue(tvList.isNullValue(0, 0));
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
      values[1] = null; // This will create a bitmap
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
}
