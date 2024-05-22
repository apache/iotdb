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

package org.apache.iotdb.db.storageengine.dataregion.read.reader.series;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.IFullPath;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.db.queryengine.execution.operator.source.SeriesScanUtil;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.SeriesScanOptions;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;
import org.apache.iotdb.db.storageengine.dataregion.read.QueryDataSource;
import org.apache.iotdb.db.utils.EnvironmentUtils;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;

public class SeriesScanLimitOffsetPushDownTest extends AbstractSeriesScanTest {

  private SeriesScanUtil getSeriesScanUtil(long limit, long offset, Ordering scanOrder)
      throws IllegalPathException {
    MeasurementPath scanPath = new MeasurementPath(TEST_PATH, TSDataType.INT32);

    SeriesScanOptions.Builder scanOptionsBuilder = new SeriesScanOptions.Builder();
    scanOptionsBuilder.withAllSensors(Collections.singleton(scanPath.getMeasurement()));
    scanOptionsBuilder.withPushDownLimit(limit);
    scanOptionsBuilder.withPushDownOffset(offset);
    SeriesScanUtil seriesScanUtil =
        new SeriesScanUtil(
            IFullPath.convertToIFullPath(scanPath),
            scanOrder,
            scanOptionsBuilder.build(),
            EnvironmentUtils.TEST_QUERY_FI_CONTEXT);
    seriesScanUtil.initQueryDataSource(new QueryDataSource(seqResources, unSeqResources));
    return seriesScanUtil;
  }

  @Test
  public void testSkipFile() throws IllegalPathException, IOException {
    SeriesScanUtil seriesScanUtil = getSeriesScanUtil(5, 10, Ordering.ASC);

    Assert.assertTrue(seriesScanUtil.hasNextFile());
    Assert.assertTrue(seriesScanUtil.hasNextChunk());
    Assert.assertTrue(seriesScanUtil.hasNextPage());

    TsBlock tsBlock = seriesScanUtil.nextPage();
    Assert.assertEquals(5, tsBlock.getPositionCount());
    long expectedTime = 10;
    for (int i = 0, size = tsBlock.getPositionCount(); i < size; i++) {
      Assert.assertEquals(expectedTime++, tsBlock.getTimeByIndex(i));
    }

    Assert.assertFalse(seriesScanUtil.hasNextPage());
    Assert.assertFalse(seriesScanUtil.hasNextChunk());
    Assert.assertFalse(seriesScanUtil.hasNextFile());
  }

  @Test
  public void testSkipChunk() throws IllegalPathException, IOException {
    SeriesScanUtil seriesScanUtil = getSeriesScanUtil(5, 20, Ordering.ASC);

    Assert.assertTrue(seriesScanUtil.hasNextFile());
    Assert.assertTrue(seriesScanUtil.hasNextChunk());
    Assert.assertTrue(seriesScanUtil.hasNextPage());

    TsBlock tsBlock = seriesScanUtil.nextPage();
    Assert.assertEquals(5, tsBlock.getPositionCount());
    long expectedTime = 20;
    for (int i = 0, size = tsBlock.getPositionCount(); i < size; i++) {
      Assert.assertEquals(expectedTime++, tsBlock.getTimeByIndex(i));
    }

    Assert.assertFalse(seriesScanUtil.hasNextPage());
    Assert.assertFalse(seriesScanUtil.hasNextChunk());
    Assert.assertFalse(seriesScanUtil.hasNextFile());
  }

  @Test
  public void testSkipPage() throws IllegalPathException, IOException {
    SeriesScanUtil seriesScanUtil = getSeriesScanUtil(5, 30, Ordering.ASC);

    Assert.assertTrue(seriesScanUtil.hasNextFile());
    Assert.assertTrue(seriesScanUtil.hasNextChunk());
    Assert.assertTrue(seriesScanUtil.hasNextPage());

    TsBlock tsBlock = seriesScanUtil.nextPage();
    Assert.assertEquals(5, tsBlock.getPositionCount());
    long expectedTime = 30;
    for (int i = 0, size = tsBlock.getPositionCount(); i < size; i++) {
      Assert.assertEquals(expectedTime++, tsBlock.getTimeByIndex(i));
    }

    Assert.assertFalse(seriesScanUtil.hasNextPage());
    Assert.assertFalse(seriesScanUtil.hasNextChunk());
    Assert.assertFalse(seriesScanUtil.hasNextFile());
  }

  @Test
  public void testSkipPoint1() throws IllegalPathException, IOException {
    SeriesScanUtil seriesScanUtil = getSeriesScanUtil(10, 45, Ordering.ASC);

    Assert.assertTrue(seriesScanUtil.hasNextFile());
    Assert.assertTrue(seriesScanUtil.hasNextChunk());
    Assert.assertTrue(seriesScanUtil.hasNextPage());

    TsBlock tsBlock = seriesScanUtil.nextPage();
    Assert.assertTrue(tsBlock == null || tsBlock.isEmpty());

    Assert.assertTrue(seriesScanUtil.hasNextPage());
    tsBlock = seriesScanUtil.nextPage();
    Assert.assertEquals(5, tsBlock.getPositionCount());
    long expectedTime = 45;
    for (int i = 0, size = tsBlock.getPositionCount(); i < size; i++) {
      Assert.assertEquals(expectedTime++, tsBlock.getTimeByIndex(i));
    }

    Assert.assertTrue(seriesScanUtil.hasNextChunk());
    Assert.assertTrue(seriesScanUtil.hasNextPage());
    tsBlock = seriesScanUtil.nextPage();
    Assert.assertEquals(5, tsBlock.getPositionCount());
    for (int i = 0, size = tsBlock.getPositionCount(); i < size; i++) {
      Assert.assertEquals(expectedTime++, tsBlock.getTimeByIndex(i));
    }

    Assert.assertFalse(seriesScanUtil.hasNextPage());
    Assert.assertFalse(seriesScanUtil.hasNextChunk());
    Assert.assertFalse(seriesScanUtil.hasNextFile());
  }

  @Test
  public void testSkipPoint2() throws IllegalPathException, IOException {
    SeriesScanUtil seriesScanUtil = getSeriesScanUtil(10, 55, Ordering.ASC);

    Assert.assertTrue(seriesScanUtil.hasNextFile());
    Assert.assertTrue(seriesScanUtil.hasNextChunk());
    Assert.assertTrue(seriesScanUtil.hasNextPage());

    TsBlock tsBlock = seriesScanUtil.nextPage();
    Assert.assertEquals(5, tsBlock.getPositionCount());
    long expectedTime = 55;
    for (int i = 0, size = tsBlock.getPositionCount(); i < size; i++) {
      Assert.assertEquals(expectedTime++, tsBlock.getTimeByIndex(i));
    }

    Assert.assertTrue(seriesScanUtil.hasNextPage());
    tsBlock = seriesScanUtil.nextPage();
    Assert.assertEquals(5, tsBlock.getPositionCount());
    for (int i = 0, size = tsBlock.getPositionCount(); i < size; i++) {
      Assert.assertEquals(expectedTime++, tsBlock.getTimeByIndex(i));
    }

    Assert.assertFalse(seriesScanUtil.hasNextPage());
    Assert.assertFalse(seriesScanUtil.hasNextChunk());
    Assert.assertFalse(seriesScanUtil.hasNextFile());
  }

  @Test
  public void testSkipPointDesc1() throws IllegalPathException, IOException {
    SeriesScanUtil seriesScanUtil = getSeriesScanUtil(10, 5, Ordering.DESC);

    Assert.assertTrue(seriesScanUtil.hasNextFile());
    Assert.assertTrue(seriesScanUtil.hasNextChunk());
    Assert.assertTrue(seriesScanUtil.hasNextPage());

    TsBlock tsBlock = seriesScanUtil.nextPage();
    Assert.assertEquals(5, tsBlock.getPositionCount());

    long expectedTime = 64;
    for (int i = 0, size = tsBlock.getPositionCount(); i < size; i++) {
      Assert.assertEquals(expectedTime--, tsBlock.getTimeByIndex(i));
    }

    Assert.assertTrue(seriesScanUtil.hasNextPage());
    tsBlock = seriesScanUtil.nextPage();
    Assert.assertEquals(5, tsBlock.getPositionCount());

    expectedTime = 59;
    for (int i = 0, size = tsBlock.getPositionCount(); i < size; i++) {
      Assert.assertEquals(expectedTime--, tsBlock.getTimeByIndex(i));
    }

    Assert.assertFalse(seriesScanUtil.hasNextPage());
    Assert.assertFalse(seriesScanUtil.hasNextChunk());
    Assert.assertFalse(seriesScanUtil.hasNextFile());
  }

  @Test
  public void testSkipPointDesc2() throws IllegalPathException, IOException {
    SeriesScanUtil seriesScanUtil = getSeriesScanUtil(10, 25, Ordering.DESC);

    Assert.assertTrue(seriesScanUtil.hasNextFile());
    Assert.assertTrue(seriesScanUtil.hasNextChunk());
    Assert.assertTrue(seriesScanUtil.hasNextPage());

    TsBlock tsBlock = seriesScanUtil.nextPage();
    Assert.assertTrue(tsBlock == null || tsBlock.isEmpty());

    Assert.assertTrue(seriesScanUtil.hasNextPage());

    tsBlock = seriesScanUtil.nextPage();
    Assert.assertTrue(tsBlock == null || tsBlock.isEmpty());

    Assert.assertFalse(seriesScanUtil.hasNextPage());

    Assert.assertTrue(seriesScanUtil.hasNextChunk());
    Assert.assertTrue(seriesScanUtil.hasNextPage());

    tsBlock = seriesScanUtil.nextPage();
    Assert.assertEquals(5, tsBlock.getPositionCount());

    long expectedTime = 44;
    for (int i = 0, size = tsBlock.getPositionCount(); i < size; i++) {
      Assert.assertEquals(expectedTime--, tsBlock.getTimeByIndex(i));
    }

    Assert.assertTrue(seriesScanUtil.hasNextPage());
    tsBlock = seriesScanUtil.nextPage();
    Assert.assertEquals(5, tsBlock.getPositionCount());

    expectedTime = 39;
    for (int i = 0, size = tsBlock.getPositionCount(); i < size; i++) {
      Assert.assertEquals(expectedTime--, tsBlock.getTimeByIndex(i));
    }

    Assert.assertFalse(seriesScanUtil.hasNextPage());
    Assert.assertFalse(seriesScanUtil.hasNextChunk());
    Assert.assertFalse(seriesScanUtil.hasNextFile());
  }
}
