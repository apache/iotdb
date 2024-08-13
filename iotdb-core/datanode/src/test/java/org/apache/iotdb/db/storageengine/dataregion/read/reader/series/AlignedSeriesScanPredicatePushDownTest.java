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
import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.read.filter.factory.FilterFactory;
import org.apache.tsfile.read.filter.factory.TimeFilterApi;
import org.apache.tsfile.read.filter.factory.ValueFilterApi;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;

public class AlignedSeriesScanPredicatePushDownTest extends AbstractAlignedSeriesScanTest {

  private AlignedSeriesScanUtil getAlignedSeriesScanUtil(
      Filter globalTimeFilter, Filter pushDownFilter) throws IllegalPathException {
    AlignedFullPath scanPath =
        new AlignedFullPath(
            TEST_DEVICE,
            Arrays.asList("s1", "s2"),
            Arrays.asList(
                new MeasurementSchema("s1", TSDataType.INT32),
                new MeasurementSchema("s2", TSDataType.INT32)));

    SeriesScanOptions.Builder scanOptionsBuilder = new SeriesScanOptions.Builder();
    scanOptionsBuilder.withAllSensors(new HashSet<>(scanPath.getMeasurementList()));
    scanOptionsBuilder.withGlobalTimeFilter(globalTimeFilter);
    scanOptionsBuilder.withPushDownFilter(pushDownFilter);
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
  @SuppressWarnings("squid:S5961") // Suppress "Test methods should not contain too many assertions"
  public void testNoFilter() throws IllegalPathException, IOException {
    AlignedSeriesScanUtil seriesScanUtil = getAlignedSeriesScanUtil(null, null);

    // File 1
    Assert.assertTrue(seriesScanUtil.hasNextFile());
    Assert.assertTrue(seriesScanUtil.canUseCurrentFileStatistics());

    // File 1 - Chunk 1
    Assert.assertTrue(seriesScanUtil.hasNextChunk());
    Assert.assertTrue(seriesScanUtil.canUseCurrentChunkStatistics());

    // File 1 - Chunk 1 - Page 1
    Assert.assertTrue(seriesScanUtil.hasNextPage());
    Assert.assertTrue(seriesScanUtil.canUseCurrentPageStatistics());
    TsBlock tsBlock = seriesScanUtil.nextPage();
    Assert.assertEquals(10, tsBlock.getPositionCount());
    Assert.assertFalse(seriesScanUtil.hasNextPage());
    Assert.assertFalse(seriesScanUtil.hasNextChunk());

    // File 2
    Assert.assertTrue(seriesScanUtil.hasNextFile());
    Assert.assertTrue(seriesScanUtil.canUseCurrentFileStatistics());

    // File 2 - Chunk 1
    Assert.assertTrue(seriesScanUtil.hasNextChunk());
    Assert.assertTrue(seriesScanUtil.canUseCurrentChunkStatistics());

    // File 2 - Chunk 1 - Page 1
    Assert.assertTrue(seriesScanUtil.hasNextPage());
    Assert.assertTrue(seriesScanUtil.canUseCurrentPageStatistics());
    tsBlock = seriesScanUtil.nextPage();
    Assert.assertEquals(10, tsBlock.getPositionCount());
    Assert.assertFalse(seriesScanUtil.hasNextPage());
    Assert.assertFalse(seriesScanUtil.hasNextChunk());

    // File 3
    Assert.assertTrue(seriesScanUtil.hasNextFile());
    Assert.assertTrue(seriesScanUtil.canUseCurrentFileStatistics());

    // File 3 - Chunk 1
    Assert.assertTrue(seriesScanUtil.hasNextChunk());
    Assert.assertTrue(seriesScanUtil.canUseCurrentChunkStatistics());

    // File 3 - Chunk 1 - Page 1
    Assert.assertTrue(seriesScanUtil.hasNextPage());
    Assert.assertTrue(seriesScanUtil.canUseCurrentPageStatistics());
    tsBlock = seriesScanUtil.nextPage();
    Assert.assertEquals(10, tsBlock.getPositionCount());
    Assert.assertFalse(seriesScanUtil.hasNextPage());

    // File 3 - Chunk 2
    Assert.assertTrue(seriesScanUtil.hasNextChunk());
    Assert.assertTrue(seriesScanUtil.canUseCurrentChunkStatistics());

    // File 3 - Chunk 2 - Page 1
    Assert.assertTrue(seriesScanUtil.hasNextPage());
    Assert.assertTrue(seriesScanUtil.canUseCurrentPageStatistics());
    tsBlock = seriesScanUtil.nextPage();
    Assert.assertEquals(10, tsBlock.getPositionCount());
    Assert.assertFalse(seriesScanUtil.hasNextPage());
    Assert.assertFalse(seriesScanUtil.hasNextChunk());

    // File 4
    Assert.assertTrue(seriesScanUtil.hasNextFile());
    Assert.assertFalse(seriesScanUtil.canUseCurrentFileStatistics());

    // File 4 - Chunk 1
    Assert.assertTrue(seriesScanUtil.hasNextChunk());
    Assert.assertTrue(seriesScanUtil.canUseCurrentChunkStatistics());

    // File 4 - Chunk 1 - Page 1 (chunk actually)
    Assert.assertTrue(seriesScanUtil.hasNextPage());
    Assert.assertTrue(seriesScanUtil.canUseCurrentPageStatistics());
    tsBlock = seriesScanUtil.nextPage();
    Assert.assertEquals(10, tsBlock.getPositionCount());
    Assert.assertFalse(seriesScanUtil.hasNextPage());

    // File 4 - Chunk 1 - Page 2 (chunk actually)
    Assert.assertTrue(seriesScanUtil.hasNextChunk());
    Assert.assertTrue(seriesScanUtil.hasNextPage());
    Assert.assertTrue(seriesScanUtil.canUseCurrentPageStatistics());
    tsBlock = seriesScanUtil.nextPage();
    Assert.assertEquals(10, tsBlock.getPositionCount());
    Assert.assertFalse(seriesScanUtil.hasNextPage());

    // File 4 - Chunk 1 - Page 3 (chunk actually)
    Assert.assertTrue(seriesScanUtil.hasNextChunk());
    Assert.assertTrue(seriesScanUtil.hasNextPage());
    Assert.assertTrue(seriesScanUtil.canUseCurrentPageStatistics());
    tsBlock = seriesScanUtil.nextPage();
    Assert.assertEquals(10, tsBlock.getPositionCount());
    Assert.assertFalse(seriesScanUtil.hasNextPage());

    // (File 4 - Chunk 2) merge (File 5 - Chunk 1)
    Assert.assertTrue(seriesScanUtil.hasNextChunk());
    Assert.assertFalse(seriesScanUtil.canUseCurrentChunkStatistics());

    // (File 4 - Chunk 2 - Page 1) merge (File 5 - Chunk 1 - Page 1)
    Assert.assertTrue(seriesScanUtil.hasNextPage());
    Assert.assertFalse(seriesScanUtil.canUseCurrentPageStatistics());
    tsBlock = seriesScanUtil.nextPage();
    Assert.assertEquals(10, tsBlock.getPositionCount());

    // File 5 - Chunk 1 - Page 2
    Assert.assertTrue(seriesScanUtil.hasNextPage());
    Assert.assertTrue(seriesScanUtil.canUseCurrentPageStatistics());
    tsBlock = seriesScanUtil.nextPage();
    Assert.assertEquals(10, tsBlock.getPositionCount());
    Assert.assertFalse(seriesScanUtil.hasNextPage());
    Assert.assertFalse(seriesScanUtil.hasNextChunk());
    Assert.assertFalse(seriesScanUtil.hasNextFile());
  }

  @Test
  public void testSkipWithFilter() throws IllegalPathException, IOException {
    AlignedSeriesScanUtil seriesScanUtil =
        getAlignedSeriesScanUtil(
            TimeFilterApi.gt(10),
            FilterFactory.and(
                ValueFilterApi.gtEq(0, 20, TSDataType.INT32),
                ValueFilterApi.lt(1, 30, TSDataType.INT32)));

    // File 1 skipped
    // File 2 skipped
    // File 3
    Assert.assertTrue(seriesScanUtil.hasNextFile());
    Assert.assertFalse(seriesScanUtil.canUseCurrentFileStatistics());

    // File 3 - Chunk 1
    Assert.assertTrue(seriesScanUtil.hasNextChunk());
    Assert.assertTrue(seriesScanUtil.canUseCurrentChunkStatistics());

    // File 3 - Chunk 1 - Page 1
    Assert.assertTrue(seriesScanUtil.hasNextPage());
    Assert.assertTrue(seriesScanUtil.canUseCurrentPageStatistics());
    TsBlock tsBlock = seriesScanUtil.nextPage();
    Assert.assertEquals(10, tsBlock.getPositionCount());
    Assert.assertEquals(20, tsBlock.getTimeByIndex(0));

    Assert.assertFalse(seriesScanUtil.hasNextPage());

    // File 3 - Chunk 2 skipped
    // File 4 - Chunk 1 skipped
    // (File 4 - Chunk 2) merge (File 5 - Chunk 1)
    Assert.assertTrue(seriesScanUtil.hasNextChunk());
    Assert.assertFalse(seriesScanUtil.canUseCurrentChunkStatistics());
    Assert.assertTrue(seriesScanUtil.hasNextPage());
    Assert.assertFalse(seriesScanUtil.canUseCurrentPageStatistics());
    tsBlock = seriesScanUtil.nextPage();
    Assert.assertTrue(tsBlock == null || tsBlock.isEmpty());

    Assert.assertTrue(seriesScanUtil.hasNextPage());
    Assert.assertFalse(seriesScanUtil.canUseCurrentPageStatistics());
    tsBlock = seriesScanUtil.nextPage();
    Assert.assertTrue(tsBlock == null || tsBlock.isEmpty());

    Assert.assertFalse(seriesScanUtil.hasNextPage());
    Assert.assertFalse(seriesScanUtil.hasNextChunk());
    Assert.assertFalse(seriesScanUtil.hasNextFile());
  }
}
