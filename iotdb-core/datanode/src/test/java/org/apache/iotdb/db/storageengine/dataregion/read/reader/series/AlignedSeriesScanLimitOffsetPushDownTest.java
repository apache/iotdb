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
import org.apache.iotdb.commons.path.AlignedFullPath;
import org.apache.iotdb.db.queryengine.execution.operator.source.AlignedSeriesScanUtil;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.SeriesScanOptions;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;
import org.apache.iotdb.db.storageengine.dataregion.read.QueryDataSource;
import org.apache.iotdb.db.utils.EnvironmentUtils;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;

public class AlignedSeriesScanLimitOffsetPushDownTest extends AbstractAlignedSeriesScanTest {

  private AlignedSeriesScanUtil getAlignedSeriesScanUtil(long limit, long offset)
      throws IllegalPathException {
    AlignedFullPath scanPath =
        new AlignedFullPath(
            TEST_DEVICE,
            Arrays.asList("s1", "s2"),
            Arrays.asList(
                new MeasurementSchema("s1", TSDataType.INT32),
                new MeasurementSchema("s2", TSDataType.INT32)));

    SeriesScanOptions.Builder scanOptionsBuilder = new SeriesScanOptions.Builder();
    scanOptionsBuilder.withAllSensors(new HashSet<>(scanPath.getMeasurementList()));
    scanOptionsBuilder.withPushDownLimit(limit);
    scanOptionsBuilder.withPushDownOffset(offset);
    AlignedSeriesScanUtil seriesScanUtil =
        new AlignedSeriesScanUtil(
            scanPath,
            Ordering.ASC,
            scanOptionsBuilder.build(),
            EnvironmentUtils.TEST_QUERY_FI_CONTEXT);
    seriesScanUtil.initQueryDataSource(new QueryDataSource(seqResources, unSeqResources));
    return seriesScanUtil;
  }

  @Test
  public void testSkipFile() throws IllegalPathException, IOException {
    AlignedSeriesScanUtil seriesScanUtil = getAlignedSeriesScanUtil(5, 10);

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
  public void testCannotSkipFile() throws IllegalPathException, IOException {
    AlignedSeriesScanUtil seriesScanUtil = getAlignedSeriesScanUtil(5, 20);

    Assert.assertTrue(seriesScanUtil.hasNextFile());
    Assert.assertTrue(seriesScanUtil.hasNextChunk());
    Assert.assertTrue(seriesScanUtil.hasNextPage());

    TsBlock tsBlock = seriesScanUtil.nextPage();
    Assert.assertEquals(0, tsBlock.getPositionCount());

    Assert.assertFalse(seriesScanUtil.hasNextPage());
    Assert.assertFalse(seriesScanUtil.hasNextChunk());

    Assert.assertTrue(seriesScanUtil.hasNextFile());
    Assert.assertTrue(seriesScanUtil.hasNextChunk());
    Assert.assertTrue(seriesScanUtil.hasNextPage());

    tsBlock = seriesScanUtil.nextPage();
    Assert.assertEquals(5, tsBlock.getPositionCount());
    long expectedTime = 20;
    for (int i = 0, size = tsBlock.getPositionCount(); i < size; i++) {
      Assert.assertEquals(expectedTime++, tsBlock.getTimeByIndex(i));
    }
  }

  @Test
  public void testSkipChunk() throws IllegalPathException, IOException {
    AlignedSeriesScanUtil seriesScanUtil = getAlignedSeriesScanUtil(5, 30);

    Assert.assertTrue(seriesScanUtil.hasNextFile());
    Assert.assertTrue(seriesScanUtil.hasNextChunk());
    Assert.assertTrue(seriesScanUtil.hasNextPage());

    TsBlock tsBlock = seriesScanUtil.nextPage();
    Assert.assertEquals(0, tsBlock.getPositionCount());

    Assert.assertFalse(seriesScanUtil.hasNextPage());
    Assert.assertFalse(seriesScanUtil.hasNextChunk());

    Assert.assertTrue(seriesScanUtil.hasNextFile());
    Assert.assertTrue(seriesScanUtil.hasNextChunk());
    Assert.assertTrue(seriesScanUtil.hasNextPage());

    tsBlock = seriesScanUtil.nextPage();
    Assert.assertEquals(5, tsBlock.getPositionCount());
    long expectedTime = 30;
    for (int i = 0, size = tsBlock.getPositionCount(); i < size; i++) {
      Assert.assertEquals(expectedTime++, tsBlock.getTimeByIndex(i));
    }
  }

  @Test
  public void testCannotSkipChunk() throws IllegalPathException, IOException {
    AlignedSeriesScanUtil seriesScanUtil = getAlignedSeriesScanUtil(5, 40);

    Assert.assertTrue(seriesScanUtil.hasNextFile());
    Assert.assertTrue(seriesScanUtil.hasNextChunk());
    Assert.assertTrue(seriesScanUtil.hasNextPage());

    TsBlock tsBlock = seriesScanUtil.nextPage();
    Assert.assertEquals(0, tsBlock.getPositionCount());

    Assert.assertFalse(seriesScanUtil.hasNextPage());
    Assert.assertFalse(seriesScanUtil.hasNextChunk());

    Assert.assertTrue(seriesScanUtil.hasNextFile());
    Assert.assertTrue(seriesScanUtil.hasNextChunk());
    Assert.assertTrue(seriesScanUtil.hasNextPage());

    tsBlock = seriesScanUtil.nextPage();
    Assert.assertEquals(0, tsBlock.getPositionCount());

    Assert.assertFalse(seriesScanUtil.hasNextPage());
    Assert.assertFalse(seriesScanUtil.hasNextChunk());

    Assert.assertTrue(seriesScanUtil.hasNextFile());
    Assert.assertTrue(seriesScanUtil.hasNextChunk());
    Assert.assertTrue(seriesScanUtil.hasNextPage());

    tsBlock = seriesScanUtil.nextPage();
    Assert.assertEquals(5, tsBlock.getPositionCount());
    long expectedTime = 40;
    for (int i = 0, size = tsBlock.getPositionCount(); i < size; i++) {
      Assert.assertEquals(expectedTime++, tsBlock.getTimeByIndex(i));
    }
  }

  @Test
  public void testSkipPage() throws IllegalPathException, IOException {
    AlignedSeriesScanUtil seriesScanUtil = getAlignedSeriesScanUtil(5, 50);

    Assert.assertTrue(seriesScanUtil.hasNextFile());
    Assert.assertTrue(seriesScanUtil.hasNextChunk());
    Assert.assertTrue(seriesScanUtil.hasNextPage());

    TsBlock tsBlock = seriesScanUtil.nextPage();
    Assert.assertEquals(0, tsBlock.getPositionCount());

    Assert.assertFalse(seriesScanUtil.hasNextPage());
    Assert.assertFalse(seriesScanUtil.hasNextChunk());

    Assert.assertTrue(seriesScanUtil.hasNextFile());
    Assert.assertTrue(seriesScanUtil.hasNextChunk());
    Assert.assertTrue(seriesScanUtil.hasNextPage());

    tsBlock = seriesScanUtil.nextPage();
    Assert.assertEquals(0, tsBlock.getPositionCount());

    Assert.assertFalse(seriesScanUtil.hasNextPage());
    Assert.assertFalse(seriesScanUtil.hasNextChunk());

    Assert.assertTrue(seriesScanUtil.hasNextFile());
    Assert.assertTrue(seriesScanUtil.hasNextChunk());
    Assert.assertTrue(seriesScanUtil.hasNextPage());

    tsBlock = seriesScanUtil.nextPage();
    Assert.assertEquals(5, tsBlock.getPositionCount());
    long expectedTime = 50;
    for (int i = 0, size = tsBlock.getPositionCount(); i < size; i++) {
      Assert.assertEquals(expectedTime++, tsBlock.getTimeByIndex(i));
    }
  }

  @Test
  public void testCannotSkipPage() throws IllegalPathException, IOException {
    AlignedSeriesScanUtil seriesScanUtil = getAlignedSeriesScanUtil(5, 60);

    Assert.assertTrue(seriesScanUtil.hasNextFile());
    Assert.assertTrue(seriesScanUtil.hasNextChunk());
    Assert.assertTrue(seriesScanUtil.hasNextPage());

    TsBlock tsBlock = seriesScanUtil.nextPage();
    Assert.assertEquals(0, tsBlock.getPositionCount());

    Assert.assertFalse(seriesScanUtil.hasNextPage());
    Assert.assertFalse(seriesScanUtil.hasNextChunk());

    Assert.assertTrue(seriesScanUtil.hasNextFile());
    Assert.assertTrue(seriesScanUtil.hasNextChunk());
    Assert.assertTrue(seriesScanUtil.hasNextPage());

    tsBlock = seriesScanUtil.nextPage();
    Assert.assertEquals(0, tsBlock.getPositionCount());

    Assert.assertFalse(seriesScanUtil.hasNextPage());
    Assert.assertFalse(seriesScanUtil.hasNextChunk());

    Assert.assertTrue(seriesScanUtil.hasNextFile());
    Assert.assertTrue(seriesScanUtil.hasNextChunk());
    Assert.assertTrue(seriesScanUtil.hasNextPage());

    tsBlock = seriesScanUtil.nextPage();
    Assert.assertEquals(0, tsBlock.getPositionCount());

    Assert.assertFalse(seriesScanUtil.hasNextPage());
    Assert.assertTrue(seriesScanUtil.hasNextChunk());
    Assert.assertTrue(seriesScanUtil.hasNextPage());

    tsBlock = seriesScanUtil.nextPage();
    Assert.assertEquals(5, tsBlock.getPositionCount());
    long expectedTime = 60;
    for (int i = 0, size = tsBlock.getPositionCount(); i < size; i++) {
      Assert.assertEquals(expectedTime++, tsBlock.getTimeByIndex(i));
    }
  }

  @Test
  public void testSkipPoint() throws IllegalPathException, IOException {
    AlignedSeriesScanUtil seriesScanUtil = getAlignedSeriesScanUtil(10, 75);

    Assert.assertTrue(seriesScanUtil.hasNextFile());
    Assert.assertTrue(seriesScanUtil.hasNextChunk());
    Assert.assertTrue(seriesScanUtil.hasNextPage());

    TsBlock tsBlock = seriesScanUtil.nextPage();
    Assert.assertEquals(0, tsBlock.getPositionCount());

    Assert.assertFalse(seriesScanUtil.hasNextPage());
    Assert.assertFalse(seriesScanUtil.hasNextChunk());

    Assert.assertTrue(seriesScanUtil.hasNextFile());
    Assert.assertTrue(seriesScanUtil.hasNextChunk());
    Assert.assertTrue(seriesScanUtil.hasNextPage());

    tsBlock = seriesScanUtil.nextPage();
    Assert.assertEquals(0, tsBlock.getPositionCount());

    Assert.assertFalse(seriesScanUtil.hasNextPage());
    Assert.assertFalse(seriesScanUtil.hasNextChunk());

    Assert.assertTrue(seriesScanUtil.hasNextFile());
    Assert.assertTrue(seriesScanUtil.hasNextChunk());
    Assert.assertTrue(seriesScanUtil.hasNextPage());

    tsBlock = seriesScanUtil.nextPage();
    Assert.assertEquals(0, tsBlock.getPositionCount());

    Assert.assertFalse(seriesScanUtil.hasNextPage());
    Assert.assertTrue(seriesScanUtil.hasNextChunk());
    Assert.assertTrue(seriesScanUtil.hasNextPage());

    tsBlock = seriesScanUtil.nextPage();
    Assert.assertEquals(5, tsBlock.getPositionCount());
    long expectedTime = 75;
    for (int i = 0, size = tsBlock.getPositionCount(); i < size; i++) {
      Assert.assertEquals(expectedTime++, tsBlock.getTimeByIndex(i));
    }

    Assert.assertTrue(seriesScanUtil.hasNextPage());

    tsBlock = seriesScanUtil.nextPage();
    Assert.assertEquals(5, tsBlock.getPositionCount());
    expectedTime = 80;
    for (int i = 0, size = tsBlock.getPositionCount(); i < size; i++) {
      Assert.assertEquals(expectedTime++, tsBlock.getTimeByIndex(i));
    }
  }
}
