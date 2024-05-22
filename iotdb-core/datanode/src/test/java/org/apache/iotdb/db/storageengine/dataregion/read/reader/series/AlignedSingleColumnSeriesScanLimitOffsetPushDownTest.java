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
import java.util.Collections;
import java.util.HashSet;

public class AlignedSingleColumnSeriesScanLimitOffsetPushDownTest
    extends AbstractAlignedSeriesScanTest {

  private static final int TEST_LIMIT = 5;

  private AlignedSeriesScanUtil getAlignedSingleColumnSeriesScanUtil(long offset)
      throws IllegalPathException {
    AlignedFullPath scanPath =
        new AlignedFullPath(
            TEST_DEVICE,
            Collections.singletonList("s1"),
            Collections.singletonList(new MeasurementSchema("s1", TSDataType.INT32)));

    SeriesScanOptions.Builder scanOptionsBuilder = new SeriesScanOptions.Builder();
    scanOptionsBuilder.withAllSensors(new HashSet<>(scanPath.getMeasurementList()));
    scanOptionsBuilder.withPushDownLimit(TEST_LIMIT);
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
    AlignedSeriesScanUtil seriesScanUtil = getAlignedSingleColumnSeriesScanUtil(20);

    // File 1 skipped
    // File 2
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
    long expectedTime = 24;
    for (int i = 0, size = tsBlock.getPositionCount(); i < size; i++) {
      Assert.assertEquals(expectedTime++, tsBlock.getTimeByIndex(i));
    }
  }

  @Test
  public void testSkipChunk() throws IllegalPathException, IOException {
    AlignedSeriesScanUtil seriesScanUtil = getAlignedSingleColumnSeriesScanUtil(30);

    // File 1 skipped (10 points)
    // File 2 skipped (6 points)
    Assert.assertTrue(seriesScanUtil.hasNextFile());
    Assert.assertTrue(seriesScanUtil.hasNextChunk());
    Assert.assertTrue(seriesScanUtil.hasNextPage());

    TsBlock tsBlock = seriesScanUtil.nextPage();
    Assert.assertEquals(0, tsBlock.getPositionCount());

    Assert.assertFalse(seriesScanUtil.hasNextPage());
    Assert.assertFalse(seriesScanUtil.hasNextChunk());

    // File 3
    Assert.assertTrue(seriesScanUtil.hasNextFile());
    // File 3 Chunk 1 skipped (10 points)
    // File 3 Chunk 2 (6 points should skip 4 points)
    Assert.assertTrue(seriesScanUtil.hasNextChunk());
    Assert.assertTrue(seriesScanUtil.hasNextPage());

    tsBlock = seriesScanUtil.nextPage();
    Assert.assertEquals(2, tsBlock.getPositionCount());
    long expectedTime = 34;
    for (int i = 0, size = tsBlock.getPositionCount(); i < size; i++) {
      Assert.assertEquals(expectedTime++, tsBlock.getTimeByIndex(i));
    }
    // remaining 3 points selected

    Assert.assertFalse(seriesScanUtil.hasNextPage());
    Assert.assertFalse(seriesScanUtil.hasNextChunk());

    // File 3
    Assert.assertTrue(seriesScanUtil.hasNextFile());
    Assert.assertTrue(seriesScanUtil.hasNextChunk());
    Assert.assertTrue(seriesScanUtil.hasNextPage());
    tsBlock = seriesScanUtil.nextPage();
    Assert.assertEquals(3, tsBlock.getPositionCount());
    expectedTime = 40;
    for (int i = 0, size = tsBlock.getPositionCount(); i < size; i++) {
      Assert.assertEquals(expectedTime++, tsBlock.getTimeByIndex(i));
    }

    Assert.assertFalse(seriesScanUtil.hasNextPage());
    Assert.assertFalse(seriesScanUtil.hasNextChunk());
    Assert.assertFalse(seriesScanUtil.hasNextFile());
  }

  @Test
  public void testSkipPage() throws IllegalPathException, IOException {
    AlignedSeriesScanUtil seriesScanUtil = getAlignedSingleColumnSeriesScanUtil(45);

    // File 1 skipped (10 points)
    // File 2 skipped (6 points)
    Assert.assertTrue(seriesScanUtil.hasNextFile());
    Assert.assertTrue(seriesScanUtil.hasNextChunk());
    Assert.assertTrue(seriesScanUtil.hasNextPage());

    TsBlock tsBlock = seriesScanUtil.nextPage();
    Assert.assertEquals(0, tsBlock.getPositionCount());

    Assert.assertFalse(seriesScanUtil.hasNextPage());
    Assert.assertFalse(seriesScanUtil.hasNextChunk());

    // File 3
    Assert.assertTrue(seriesScanUtil.hasNextFile());

    // File 3 - Chunk 1 skipped (10 points)
    // File 3 - Chunk 2 skipped (6 points)
    Assert.assertTrue(seriesScanUtil.hasNextChunk());
    Assert.assertTrue(seriesScanUtil.hasNextPage());

    tsBlock = seriesScanUtil.nextPage();
    Assert.assertEquals(0, tsBlock.getPositionCount());

    Assert.assertFalse(seriesScanUtil.hasNextPage());
    Assert.assertFalse(seriesScanUtil.hasNextChunk());

    // File 4
    Assert.assertTrue(seriesScanUtil.hasNextFile());

    // File 4 - Chunk 1 - Page 1 skipped (10 points)
    // File 4 - Chunk 1 - Page 2  (6 points should skip 3 points)
    Assert.assertTrue(seriesScanUtil.hasNextChunk());
    Assert.assertTrue(seriesScanUtil.hasNextPage());
    tsBlock = seriesScanUtil.nextPage();
    Assert.assertEquals(3, tsBlock.getPositionCount());
    long expectedTime = 53;
    for (int i = 0, size = tsBlock.getPositionCount(); i < size; i++) {
      Assert.assertEquals(expectedTime++, tsBlock.getTimeByIndex(i));
    }
    Assert.assertFalse(seriesScanUtil.hasNextPage());

    // File 4 - Chunk 1 - Page 2 (remaining 2 points)
    Assert.assertTrue(seriesScanUtil.hasNextChunk());
    Assert.assertTrue(seriesScanUtil.hasNextPage());

    tsBlock = seriesScanUtil.nextPage();
    Assert.assertEquals(2, tsBlock.getPositionCount());
    expectedTime = 60;
    for (int i = 0, size = tsBlock.getPositionCount(); i < size; i++) {
      Assert.assertEquals(expectedTime++, tsBlock.getTimeByIndex(i));
    }
  }
}
