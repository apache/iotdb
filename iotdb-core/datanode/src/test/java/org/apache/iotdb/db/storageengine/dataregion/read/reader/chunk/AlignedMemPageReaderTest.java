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

package org.apache.iotdb.db.storageengine.dataregion.read.reader.chunk;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.AlignedChunkMetadata;
import org.apache.tsfile.file.metadata.statistics.IntegerStatistics;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.file.metadata.statistics.TimeStatistics;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.read.filter.factory.TimeFilterApi;
import org.apache.tsfile.read.filter.factory.ValueFilterApi;
import org.apache.tsfile.read.reader.series.PaginationController;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;

import static org.apache.tsfile.read.filter.factory.ValueFilterApi.DEFAULT_MEASUREMENT_INDEX;

public class AlignedMemPageReaderTest {

  private static final TsBlock tsBlock1;
  private static final AlignedChunkMetadata chunkMetadata1 =
      Mockito.mock(AlignedChunkMetadata.class);

  private static final TsBlock tsBlock2;
  private static final AlignedChunkMetadata chunkMetadata2 =
      Mockito.mock(AlignedChunkMetadata.class);

  static {
    TsBlockBuilder tsBlockBuilder1 =
        new TsBlockBuilder(Arrays.asList(TSDataType.INT32, TSDataType.INT32));
    TsBlockBuilder tsBlockBuilder2 =
        new TsBlockBuilder(Collections.singletonList(TSDataType.INT32));

    TimeStatistics timeStatistics = new TimeStatistics();
    IntegerStatistics valueStatistics1 = new IntegerStatistics();
    IntegerStatistics valueStatistics2 = new IntegerStatistics();

    for (int i = 0; i < 100; i++) {
      tsBlockBuilder1.getTimeColumnBuilder().writeLong(i);
      tsBlockBuilder2.getTimeColumnBuilder().writeLong(i);
      timeStatistics.update(i);

      tsBlockBuilder1.getValueColumnBuilders()[0].writeInt(i);
      valueStatistics1.update(i, i);

      if (i >= 10 && i < 90) {
        tsBlockBuilder1.getValueColumnBuilders()[1].writeInt(i);
        tsBlockBuilder2.getValueColumnBuilders()[0].writeInt(i);
        valueStatistics2.update(i, i);
      } else {
        tsBlockBuilder1.getValueColumnBuilders()[1].appendNull();
        tsBlockBuilder2.getValueColumnBuilders()[0].appendNull();
      }
      tsBlockBuilder1.declarePosition();
      tsBlockBuilder2.declarePosition();
    }

    tsBlock1 = tsBlockBuilder1.build();
    Mockito.when(chunkMetadata1.getTimeStatistics()).thenReturn((Statistics) timeStatistics);
    Mockito.when(chunkMetadata1.getMeasurementStatistics(0))
        .thenReturn(Optional.of(valueStatistics1));
    Mockito.when(chunkMetadata1.getMeasurementStatistics(1))
        .thenReturn(Optional.of(valueStatistics2));

    tsBlock2 = tsBlockBuilder2.build();
    Mockito.when(chunkMetadata2.getTimeStatistics()).thenReturn((Statistics) timeStatistics);
    Mockito.when(chunkMetadata2.getMeasurementStatistics(0))
        .thenReturn(Optional.of(valueStatistics2));
  }

  private MemAlignedPageReader generateAlignedPageReader() {
    MemAlignedPageReader alignedPageReader =
        new MemAlignedPageReader(tsBlock1, chunkMetadata1, null);
    alignedPageReader.initTsBlockBuilder(Arrays.asList(TSDataType.INT32, TSDataType.INT32));
    return alignedPageReader;
  }

  private MemAlignedPageReader generateSingleColumnAlignedPageReader() {
    MemAlignedPageReader alignedPageReader =
        new MemAlignedPageReader(tsBlock2, chunkMetadata2, null);
    alignedPageReader.initTsBlockBuilder(Collections.singletonList(TSDataType.INT32));
    return alignedPageReader;
  }

  @Test
  public void testNullFilter() {
    MemAlignedPageReader alignedPageReader1 = generateAlignedPageReader();
    TsBlock tsBlock1 = alignedPageReader1.getAllSatisfiedData();
    Assert.assertEquals(100, tsBlock1.getPositionCount());

    MemAlignedPageReader alignedPageReader2 = generateSingleColumnAlignedPageReader();
    TsBlock tsBlock2 = alignedPageReader2.getAllSatisfiedData();
    Assert.assertEquals(100, tsBlock2.getPositionCount());
  }

  @Test
  public void testNullFilterWithLimitOffset() {
    MemAlignedPageReader alignedPageReader1 = generateAlignedPageReader();
    alignedPageReader1.setLimitOffset(new PaginationController(10, 10));
    TsBlock tsBlock1 = alignedPageReader1.getAllSatisfiedData();
    Assert.assertEquals(10, tsBlock1.getPositionCount());
    Assert.assertEquals(10, tsBlock1.getTimeByIndex(0));
    Assert.assertEquals(19, tsBlock1.getTimeByIndex(9));

    MemAlignedPageReader alignedPageReader2 = generateSingleColumnAlignedPageReader();
    alignedPageReader2.setLimitOffset(new PaginationController(10, 10));
    TsBlock tsBlock2 = alignedPageReader2.getAllSatisfiedData();
    Assert.assertEquals(10, tsBlock2.getPositionCount());
    Assert.assertEquals(10, tsBlock2.getTimeByIndex(0));
    Assert.assertEquals(19, tsBlock2.getTimeByIndex(9));
  }

  @Test
  public void testGlobalTimeFilterAllSatisfy() {
    Filter globalTimeFilter = TimeFilterApi.gtEq(0L);
    MemAlignedPageReader alignedPageReader1 = generateAlignedPageReader();
    alignedPageReader1.addRecordFilter(globalTimeFilter);
    alignedPageReader1.addRecordFilter(
        ValueFilterApi.gtEq(DEFAULT_MEASUREMENT_INDEX, 50, TSDataType.INT32));
    TsBlock tsBlock1 = alignedPageReader1.getAllSatisfiedData();
    Assert.assertEquals(50, tsBlock1.getPositionCount());

    MemAlignedPageReader alignedPageReader2 = generateSingleColumnAlignedPageReader();
    alignedPageReader2.addRecordFilter(globalTimeFilter);
    alignedPageReader2.addRecordFilter(
        ValueFilterApi.gtEq(DEFAULT_MEASUREMENT_INDEX, 50, TSDataType.INT32));
    TsBlock tsBlock2 = alignedPageReader2.getAllSatisfiedData();
    Assert.assertEquals(40, tsBlock2.getPositionCount());
  }

  @Test
  public void testGlobalTimeFilterAllSatisfyWithLimitOffset() {
    Filter globalTimeFilter = TimeFilterApi.gtEq(0L);
    MemAlignedPageReader alignedPageReader1 = generateAlignedPageReader();
    alignedPageReader1.addRecordFilter(globalTimeFilter);
    alignedPageReader1.addRecordFilter(
        ValueFilterApi.gtEq(DEFAULT_MEASUREMENT_INDEX, 50, TSDataType.INT32));
    alignedPageReader1.setLimitOffset(new PaginationController(10, 10));
    TsBlock tsBlock1 = alignedPageReader1.getAllSatisfiedData();
    Assert.assertEquals(10, tsBlock1.getPositionCount());
    Assert.assertEquals(60, tsBlock1.getTimeByIndex(0));
    Assert.assertEquals(69, tsBlock1.getTimeByIndex(9));

    MemAlignedPageReader alignedPageReader2 = generateSingleColumnAlignedPageReader();
    alignedPageReader2.addRecordFilter(globalTimeFilter);
    alignedPageReader2.addRecordFilter(
        ValueFilterApi.gtEq(DEFAULT_MEASUREMENT_INDEX, 50, TSDataType.INT32));
    alignedPageReader2.setLimitOffset(new PaginationController(10, 10));
    TsBlock tsBlock2 = alignedPageReader2.getAllSatisfiedData();
    Assert.assertEquals(10, tsBlock2.getPositionCount());
    Assert.assertEquals(60, tsBlock2.getTimeByIndex(0));
    Assert.assertEquals(69, tsBlock2.getTimeByIndex(9));
  }

  @Test
  public void testPushDownFilterAllSatisfy() {
    Filter globalTimeFilter = TimeFilterApi.gtEq(50L);
    MemAlignedPageReader alignedPageReader1 = generateAlignedPageReader();
    alignedPageReader1.addRecordFilter(globalTimeFilter);
    alignedPageReader1.addRecordFilter(
        ValueFilterApi.gtEq(DEFAULT_MEASUREMENT_INDEX, 0, TSDataType.INT32));
    TsBlock tsBlock1 = alignedPageReader1.getAllSatisfiedData();
    Assert.assertEquals(50, tsBlock1.getPositionCount());

    MemAlignedPageReader alignedPageReader2 = generateSingleColumnAlignedPageReader();
    alignedPageReader2.addRecordFilter(globalTimeFilter);
    alignedPageReader2.addRecordFilter(
        ValueFilterApi.gtEq(DEFAULT_MEASUREMENT_INDEX, 0, TSDataType.INT32));
    TsBlock tsBlock2 = alignedPageReader2.getAllSatisfiedData();
    Assert.assertEquals(40, tsBlock2.getPositionCount());
  }

  @Test
  public void testPushDownFilterAllSatisfyWithLimitOffset() {
    Filter globalTimeFilter = TimeFilterApi.gtEq(50L);
    MemAlignedPageReader alignedPageReader1 = generateAlignedPageReader();
    alignedPageReader1.addRecordFilter(globalTimeFilter);
    alignedPageReader1.addRecordFilter(
        ValueFilterApi.gtEq(DEFAULT_MEASUREMENT_INDEX, 0, TSDataType.INT32));
    alignedPageReader1.setLimitOffset(new PaginationController(10, 10));
    TsBlock tsBlock1 = alignedPageReader1.getAllSatisfiedData();
    Assert.assertEquals(10, tsBlock1.getPositionCount());
    Assert.assertEquals(60, tsBlock1.getTimeByIndex(0));
    Assert.assertEquals(69, tsBlock1.getTimeByIndex(9));

    MemAlignedPageReader alignedPageReader2 = generateSingleColumnAlignedPageReader();
    alignedPageReader2.addRecordFilter(globalTimeFilter);
    alignedPageReader2.addRecordFilter(
        ValueFilterApi.gtEq(DEFAULT_MEASUREMENT_INDEX, 0, TSDataType.INT32));
    alignedPageReader2.setLimitOffset(new PaginationController(10, 10));
    TsBlock tsBlock2 = alignedPageReader2.getAllSatisfiedData();
    Assert.assertEquals(10, tsBlock2.getPositionCount());
    Assert.assertEquals(60, tsBlock2.getTimeByIndex(0));
    Assert.assertEquals(69, tsBlock2.getTimeByIndex(9));
  }

  @Test
  public void testFilter() {
    Filter globalTimeFilter = TimeFilterApi.gtEq(30L);
    MemAlignedPageReader alignedPageReader1 = generateAlignedPageReader();
    alignedPageReader1.addRecordFilter(globalTimeFilter);
    alignedPageReader1.addRecordFilter(
        ValueFilterApi.lt(DEFAULT_MEASUREMENT_INDEX, 80, TSDataType.INT32));
    TsBlock tsBlock1 = alignedPageReader1.getAllSatisfiedData();
    Assert.assertEquals(50, tsBlock1.getPositionCount());

    MemAlignedPageReader alignedPageReader2 = generateSingleColumnAlignedPageReader();
    alignedPageReader2.addRecordFilter(globalTimeFilter);
    alignedPageReader2.addRecordFilter(
        ValueFilterApi.lt(DEFAULT_MEASUREMENT_INDEX, 80, TSDataType.INT32));
    TsBlock tsBlock2 = alignedPageReader2.getAllSatisfiedData();
    Assert.assertEquals(50, tsBlock2.getPositionCount());
  }

  @Test
  public void testFilterWithLimitOffset() {
    Filter globalTimeFilter = TimeFilterApi.gtEq(50L);
    MemAlignedPageReader alignedPageReader1 = generateAlignedPageReader();
    alignedPageReader1.addRecordFilter(globalTimeFilter);
    alignedPageReader1.addRecordFilter(
        ValueFilterApi.lt(DEFAULT_MEASUREMENT_INDEX, 80, TSDataType.INT32));
    alignedPageReader1.setLimitOffset(new PaginationController(10, 10));
    TsBlock tsBlock1 = alignedPageReader1.getAllSatisfiedData();
    Assert.assertEquals(10, tsBlock1.getPositionCount());
    Assert.assertEquals(60, tsBlock1.getTimeByIndex(0));
    Assert.assertEquals(69, tsBlock1.getTimeByIndex(9));

    MemAlignedPageReader alignedPageReader2 = generateSingleColumnAlignedPageReader();
    alignedPageReader2.addRecordFilter(globalTimeFilter);
    alignedPageReader2.addRecordFilter(
        ValueFilterApi.lt(DEFAULT_MEASUREMENT_INDEX, 80, TSDataType.INT32));
    alignedPageReader2.setLimitOffset(new PaginationController(10, 10));
    TsBlock tsBlock2 = alignedPageReader2.getAllSatisfiedData();
    Assert.assertEquals(10, tsBlock2.getPositionCount());
    Assert.assertEquals(60, tsBlock2.getTimeByIndex(0));
    Assert.assertEquals(69, tsBlock2.getTimeByIndex(9));
  }
}
