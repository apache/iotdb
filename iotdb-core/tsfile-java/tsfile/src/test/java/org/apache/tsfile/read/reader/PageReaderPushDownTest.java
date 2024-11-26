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

package org.apache.tsfile.read.reader;

import org.apache.tsfile.encoding.decoder.DeltaBinaryDecoder;
import org.apache.tsfile.encoding.decoder.IntRleDecoder;
import org.apache.tsfile.encoding.encoder.DeltaBinaryEncoder;
import org.apache.tsfile.encoding.encoder.IntRleEncoder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.header.PageHeader;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.filter.factory.TimeFilterApi;
import org.apache.tsfile.read.filter.factory.ValueFilterApi;
import org.apache.tsfile.read.reader.page.PageReader;
import org.apache.tsfile.read.reader.series.PaginationController;
import org.apache.tsfile.write.page.PageWriter;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.apache.tsfile.read.filter.factory.ValueFilterApi.DEFAULT_MEASUREMENT_INDEX;

public class PageReaderPushDownTest {

  private static final PageHeader testPageHeader;
  private static ByteBuffer testPageData;

  static {
    PageWriter pageWriter = new PageWriter();
    pageWriter.setTimeEncoder(new DeltaBinaryEncoder.LongDeltaEncoder());
    pageWriter.setValueEncoder(new IntRleEncoder());
    pageWriter.initStatistics(TSDataType.INT32);

    for (int i = 0; i < 100; i++) {
      pageWriter.write(i, i);
    }

    testPageHeader = new PageHeader(0, 0, pageWriter.getStatistics());

    testPageData = null;
    try {
      testPageData = ByteBuffer.wrap(pageWriter.getUncompressedBytes().array());
    } catch (IOException e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
  }

  private IPageReader generatePageReader() {
    testPageData.position(0);
    return new PageReader(
        testPageHeader,
        testPageData,
        TSDataType.INT32,
        new IntRleDecoder(),
        new DeltaBinaryDecoder.LongDeltaDecoder());
  }

  @Test
  public void testNullFilter() throws IOException {
    IPageReader pageReader = generatePageReader();

    TsBlock tsBlock = pageReader.getAllSatisfiedData();

    Assert.assertEquals(100, tsBlock.getPositionCount());
  }

  @Test
  public void testNullFilterAndLimitOffset() throws IOException {
    IPageReader pageReader = generatePageReader();
    pageReader.setLimitOffset(new PaginationController(10, 10));

    TsBlock tsBlock = pageReader.getAllSatisfiedData();

    Assert.assertEquals(10, tsBlock.getPositionCount());
    Assert.assertEquals(10, tsBlock.getTimeByIndex(0));
    Assert.assertEquals(19, tsBlock.getTimeByIndex(9));
  }

  @Test
  public void testFilterAllSatisfy() throws IOException {
    IPageReader pageReader = generatePageReader();
    pageReader.addRecordFilter(TimeFilterApi.gtEq(0));

    TsBlock tsBlock = pageReader.getAllSatisfiedData();

    Assert.assertEquals(100, tsBlock.getPositionCount());
  }

  @Test
  public void testFilterAllSatisfyAndLimitOffset() throws IOException {
    IPageReader pageReader = generatePageReader();
    pageReader.addRecordFilter(TimeFilterApi.gtEq(0));
    pageReader.setLimitOffset(new PaginationController(10, 10));

    TsBlock tsBlock = pageReader.getAllSatisfiedData();

    Assert.assertEquals(10, tsBlock.getPositionCount());
    Assert.assertEquals(10, tsBlock.getTimeByIndex(0));
    Assert.assertEquals(19, tsBlock.getTimeByIndex(9));
  }

  @Test
  public void testFilter() throws IOException {
    IPageReader pageReader = generatePageReader();
    pageReader.addRecordFilter(TimeFilterApi.gtEq(50));
    pageReader.addRecordFilter(ValueFilterApi.lt(DEFAULT_MEASUREMENT_INDEX, 80, TSDataType.INT32));

    TsBlock tsBlock = pageReader.getAllSatisfiedData();

    Assert.assertEquals(30, tsBlock.getPositionCount());
  }

  @Test
  public void testFilterAndLimitOffset() throws IOException {
    IPageReader pageReader = generatePageReader();
    pageReader.addRecordFilter(TimeFilterApi.gtEq(50));
    pageReader.addRecordFilter(ValueFilterApi.lt(DEFAULT_MEASUREMENT_INDEX, 80, TSDataType.INT32));
    pageReader.setLimitOffset(new PaginationController(10, 10));

    TsBlock tsBlock = pageReader.getAllSatisfiedData();

    Assert.assertEquals(10, tsBlock.getPositionCount());
    Assert.assertEquals(60, tsBlock.getTimeByIndex(0));
    Assert.assertEquals(69, tsBlock.getTimeByIndex(9));
  }
}
