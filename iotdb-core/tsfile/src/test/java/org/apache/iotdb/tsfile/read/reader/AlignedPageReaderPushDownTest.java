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

package org.apache.iotdb.tsfile.read.reader;

import org.apache.iotdb.tsfile.compress.ICompressor;
import org.apache.iotdb.tsfile.encoding.decoder.Decoder;
import org.apache.iotdb.tsfile.encoding.decoder.DeltaBinaryDecoder;
import org.apache.iotdb.tsfile.encoding.decoder.IntRleDecoder;
import org.apache.iotdb.tsfile.encoding.encoder.DeltaBinaryEncoder;
import org.apache.iotdb.tsfile.encoding.encoder.IntRleEncoder;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.factory.TimeFilterApi;
import org.apache.iotdb.tsfile.read.filter.factory.ValueFilterApi;
import org.apache.iotdb.tsfile.read.reader.page.AlignedPageReader;
import org.apache.iotdb.tsfile.read.reader.series.PaginationController;
import org.apache.iotdb.tsfile.write.page.TimePageWriter;
import org.apache.iotdb.tsfile.write.page.ValuePageWriter;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class AlignedPageReaderPushDownTest {

  private static final PageHeader testTimePageHeader;
  private static ByteBuffer testTimePageData;

  private static final PageHeader testValuePageHeader1;
  private static ByteBuffer testValuePageData1;

  private static final PageHeader testValuePageHeader2;
  private static ByteBuffer testValuePageData2;

  static {
    TimePageWriter timePageWriter =
        new TimePageWriter(
            new DeltaBinaryEncoder.LongDeltaEncoder(),
            ICompressor.getCompressor(CompressionType.LZ4));
    for (long i = 0; i < 100; i++) {
      timePageWriter.write(i);
    }
    testTimePageHeader = new PageHeader(0, 0, timePageWriter.getStatistics());
    testTimePageData = null;
    try {
      testTimePageData = ByteBuffer.wrap(timePageWriter.getUncompressedBytes().array());
    } catch (IOException e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }

    ValuePageWriter valuePageWriter1 =
        new ValuePageWriter(
            new IntRleEncoder(), ICompressor.getCompressor(CompressionType.LZ4), TSDataType.INT32);
    for (int i = 0; i < 100; i++) {
      valuePageWriter1.write(i, i, false);
    }
    testValuePageHeader1 = new PageHeader(0, 0, valuePageWriter1.getStatistics());
    testValuePageData1 = null;
    try {
      testValuePageData1 = ByteBuffer.wrap(valuePageWriter1.getUncompressedBytes().array());
    } catch (IOException e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }

    ValuePageWriter valuePageWriter2 =
        new ValuePageWriter(
            new IntRleEncoder(), ICompressor.getCompressor(CompressionType.LZ4), TSDataType.INT32);
    for (int i = 0; i < 10; i++) {
      valuePageWriter2.write(i, i, true);
    }
    for (int i = 10; i < 90; i++) {
      valuePageWriter2.write(i, i, false);
    }
    for (int i = 90; i < 100; i++) {
      valuePageWriter2.write(i, i, true);
    }
    testValuePageHeader2 = new PageHeader(0, 0, valuePageWriter2.getStatistics());
    testValuePageData2 = null;
    try {
      testValuePageData2 = ByteBuffer.wrap(valuePageWriter2.getUncompressedBytes().array());
    } catch (IOException e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
  }

  private void resetDataBuffer() {
    testTimePageData.position(0);
    testValuePageData1.position(0);
    testValuePageData2.position(0);
  }

  private AlignedPageReader generateAlignedPageReader(
      Filter globalTimeFilter, List<Boolean> modified) throws IOException {
    resetDataBuffer();
    testValuePageHeader1.setModified(modified.get(0));
    testValuePageHeader2.setModified(modified.get(1));
    List<PageHeader> valuePageHeaderList =
        Arrays.asList(testValuePageHeader1, testValuePageHeader2);
    List<ByteBuffer> valuePageDataList = Arrays.asList(testValuePageData1, testValuePageData2);
    List<TSDataType> valueDataTypeList = Arrays.asList(TSDataType.INT32, TSDataType.INT32);
    List<Decoder> valueDecoderList = Arrays.asList(new IntRleDecoder(), new IntRleDecoder());
    AlignedPageReader alignedPageReader =
        new AlignedPageReader(
            testTimePageHeader,
            testTimePageData,
            new DeltaBinaryDecoder.LongDeltaDecoder(),
            valuePageHeaderList,
            valuePageDataList,
            valueDataTypeList,
            valueDecoderList,
            globalTimeFilter);
    alignedPageReader.initTsBlockBuilder(valueDataTypeList);
    return alignedPageReader;
  }

  private AlignedPageReader generateAlignedPageReader(Filter globalTimeFilter) throws IOException {
    return generateAlignedPageReader(globalTimeFilter, Arrays.asList(false, false));
  }

  private AlignedPageReader generateSingleColumnAlignedPageReader(
      Filter globalTimeFilter, boolean modified) {
    resetDataBuffer();
    testValuePageHeader2.setModified(modified);
    List<PageHeader> valuePageHeaderList = Collections.singletonList(testValuePageHeader2);
    List<ByteBuffer> valuePageDataList = Collections.singletonList(testValuePageData2);
    List<TSDataType> valueDataTypeList = Collections.singletonList(TSDataType.INT32);
    List<Decoder> valueDecoderList = Collections.singletonList(new IntRleDecoder());
    AlignedPageReader alignedPageReader =
        new AlignedPageReader(
            testTimePageHeader,
            testTimePageData,
            new DeltaBinaryDecoder.LongDeltaDecoder(),
            valuePageHeaderList,
            valuePageDataList,
            valueDataTypeList,
            valueDecoderList,
            globalTimeFilter);
    alignedPageReader.initTsBlockBuilder(valueDataTypeList);
    return alignedPageReader;
  }

  private AlignedPageReader generateSingleColumnAlignedPageReader(Filter globalTimeFilter) {
    return generateSingleColumnAlignedPageReader(globalTimeFilter, false);
  }

  @Test
  public void testNullFilter() throws IOException {
    AlignedPageReader alignedPageReader1 = generateAlignedPageReader(null);
    TsBlock tsBlock1 = alignedPageReader1.getAllSatisfiedData();
    Assert.assertEquals(100, tsBlock1.getPositionCount());

    AlignedPageReader alignedPageReader2 = generateSingleColumnAlignedPageReader(null);
    TsBlock tsBlock2 = alignedPageReader2.getAllSatisfiedData();
    Assert.assertEquals(80, tsBlock2.getPositionCount());
  }

  @Test
  public void testDelete() throws IOException {
    AlignedPageReader alignedPageReader1 =
        generateAlignedPageReader(null, Arrays.asList(true, true));
    alignedPageReader1.setDeleteIntervalList(
        Arrays.asList(
            Arrays.asList(new TimeRange(0, 9), new TimeRange(20, 29)),
            Collections.singletonList(new TimeRange(30, 39))));
    TsBlock tsBlock1 = alignedPageReader1.getAllSatisfiedData();
    Assert.assertEquals(90, tsBlock1.getPositionCount());

    AlignedPageReader alignedPageReader2 = generateSingleColumnAlignedPageReader(null, true);
    alignedPageReader2.setDeleteIntervalList(
        Collections.singletonList(Collections.singletonList(new TimeRange(30, 39))));
    TsBlock tsBlock2 = alignedPageReader2.getAllSatisfiedData();
    Assert.assertEquals(70, tsBlock2.getPositionCount());
  }

  @Test
  public void testNullFilterWithLimitOffset() throws IOException {
    AlignedPageReader alignedPageReader1 = generateAlignedPageReader(null);
    alignedPageReader1.setLimitOffset(new PaginationController(10, 10));
    TsBlock tsBlock1 = alignedPageReader1.getAllSatisfiedData();
    Assert.assertEquals(10, tsBlock1.getPositionCount());
    Assert.assertEquals(10, tsBlock1.getTimeByIndex(0));
    Assert.assertEquals(19, tsBlock1.getTimeByIndex(9));

    AlignedPageReader alignedPageReader2 = generateSingleColumnAlignedPageReader(null);
    alignedPageReader2.setLimitOffset(new PaginationController(10, 10));
    TsBlock tsBlock2 = alignedPageReader2.getAllSatisfiedData();
    Assert.assertEquals(10, tsBlock2.getPositionCount());
    Assert.assertEquals(20, tsBlock2.getTimeByIndex(0));
    Assert.assertEquals(29, tsBlock2.getTimeByIndex(9));
  }

  @Test
  public void testGlobalTimeFilterAllSatisfy() throws IOException {
    Filter globalTimeFilter = TimeFilterApi.gtEq(0L);
    AlignedPageReader alignedPageReader1 = generateAlignedPageReader(globalTimeFilter);
    alignedPageReader1.addRecordFilter(ValueFilterApi.gtEq(50));
    TsBlock tsBlock1 = alignedPageReader1.getAllSatisfiedData();
    Assert.assertEquals(50, tsBlock1.getPositionCount());

    AlignedPageReader alignedPageReader2 = generateSingleColumnAlignedPageReader(globalTimeFilter);
    alignedPageReader2.addRecordFilter(ValueFilterApi.gtEq(50));
    TsBlock tsBlock2 = alignedPageReader2.getAllSatisfiedData();
    Assert.assertEquals(40, tsBlock2.getPositionCount());
  }

  @Test
  public void testGlobalTimeFilterAllSatisfyWithLimitOffset() throws IOException {
    Filter globalTimeFilter = TimeFilterApi.gtEq(0L);
    AlignedPageReader alignedPageReader1 = generateAlignedPageReader(globalTimeFilter);
    alignedPageReader1.addRecordFilter(ValueFilterApi.gtEq(50));
    alignedPageReader1.setLimitOffset(new PaginationController(10, 10));
    TsBlock tsBlock1 = alignedPageReader1.getAllSatisfiedData();
    Assert.assertEquals(10, tsBlock1.getPositionCount());
    Assert.assertEquals(60, tsBlock1.getTimeByIndex(0));
    Assert.assertEquals(69, tsBlock1.getTimeByIndex(9));

    AlignedPageReader alignedPageReader2 = generateSingleColumnAlignedPageReader(globalTimeFilter);
    alignedPageReader2.addRecordFilter(ValueFilterApi.gtEq(50));
    alignedPageReader2.setLimitOffset(new PaginationController(10, 10));
    TsBlock tsBlock2 = alignedPageReader2.getAllSatisfiedData();
    Assert.assertEquals(10, tsBlock2.getPositionCount());
    Assert.assertEquals(60, tsBlock2.getTimeByIndex(0));
    Assert.assertEquals(69, tsBlock2.getTimeByIndex(9));
  }

  @Test
  public void testPushDownFilterAllSatisfy() throws IOException {
    Filter globalTimeFilter = TimeFilterApi.gtEq(50L);
    AlignedPageReader alignedPageReader1 = generateAlignedPageReader(globalTimeFilter);
    alignedPageReader1.addRecordFilter(ValueFilterApi.gtEq(0));
    TsBlock tsBlock1 = alignedPageReader1.getAllSatisfiedData();
    Assert.assertEquals(50, tsBlock1.getPositionCount());

    AlignedPageReader alignedPageReader2 = generateSingleColumnAlignedPageReader(globalTimeFilter);
    alignedPageReader2.addRecordFilter(ValueFilterApi.gtEq(0));
    TsBlock tsBlock2 = alignedPageReader2.getAllSatisfiedData();
    Assert.assertEquals(40, tsBlock2.getPositionCount());
  }

  @Test
  public void testPushDownFilterAllSatisfyWithLimitOffset() throws IOException {
    Filter globalTimeFilter = TimeFilterApi.gtEq(50L);
    AlignedPageReader alignedPageReader1 = generateAlignedPageReader(globalTimeFilter);
    alignedPageReader1.addRecordFilter(ValueFilterApi.gtEq(0));
    alignedPageReader1.setLimitOffset(new PaginationController(10, 10));
    TsBlock tsBlock1 = alignedPageReader1.getAllSatisfiedData();
    Assert.assertEquals(10, tsBlock1.getPositionCount());
    Assert.assertEquals(60, tsBlock1.getTimeByIndex(0));
    Assert.assertEquals(69, tsBlock1.getTimeByIndex(9));

    AlignedPageReader alignedPageReader2 = generateSingleColumnAlignedPageReader(globalTimeFilter);
    alignedPageReader2.addRecordFilter(ValueFilterApi.gtEq(0));
    alignedPageReader2.setLimitOffset(new PaginationController(10, 10));
    TsBlock tsBlock2 = alignedPageReader2.getAllSatisfiedData();
    Assert.assertEquals(10, tsBlock2.getPositionCount());
    Assert.assertEquals(60, tsBlock2.getTimeByIndex(0));
    Assert.assertEquals(69, tsBlock2.getTimeByIndex(9));
  }

  @Test
  public void testFilter() throws IOException {
    Filter globalTimeFilter = TimeFilterApi.gtEq(30L);
    AlignedPageReader alignedPageReader1 = generateAlignedPageReader(globalTimeFilter);
    alignedPageReader1.addRecordFilter(ValueFilterApi.lt(80));
    TsBlock tsBlock1 = alignedPageReader1.getAllSatisfiedData();
    Assert.assertEquals(50, tsBlock1.getPositionCount());

    AlignedPageReader alignedPageReader2 = generateSingleColumnAlignedPageReader(globalTimeFilter);
    alignedPageReader2.addRecordFilter(ValueFilterApi.lt(80));
    TsBlock tsBlock2 = alignedPageReader2.getAllSatisfiedData();
    Assert.assertEquals(50, tsBlock2.getPositionCount());
  }

  @Test
  public void testFilterWithLimitOffset() throws IOException {
    Filter globalTimeFilter = TimeFilterApi.gtEq(50L);
    AlignedPageReader alignedPageReader1 = generateAlignedPageReader(globalTimeFilter);
    alignedPageReader1.addRecordFilter(ValueFilterApi.lt(80));
    alignedPageReader1.setLimitOffset(new PaginationController(10, 10));
    TsBlock tsBlock1 = alignedPageReader1.getAllSatisfiedData();
    Assert.assertEquals(10, tsBlock1.getPositionCount());
    Assert.assertEquals(60, tsBlock1.getTimeByIndex(0));
    Assert.assertEquals(69, tsBlock1.getTimeByIndex(9));

    AlignedPageReader alignedPageReader2 = generateSingleColumnAlignedPageReader(globalTimeFilter);
    alignedPageReader2.addRecordFilter(ValueFilterApi.lt(80));
    alignedPageReader2.setLimitOffset(new PaginationController(10, 10));
    TsBlock tsBlock2 = alignedPageReader2.getAllSatisfiedData();
    Assert.assertEquals(10, tsBlock2.getPositionCount());
    Assert.assertEquals(60, tsBlock2.getTimeByIndex(0));
    Assert.assertEquals(69, tsBlock2.getTimeByIndex(9));
  }
}
