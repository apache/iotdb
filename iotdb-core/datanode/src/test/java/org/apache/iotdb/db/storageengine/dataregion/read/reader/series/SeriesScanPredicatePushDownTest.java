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
import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.read.filter.factory.TimeFilterApi;
import org.apache.tsfile.read.filter.factory.ValueFilterApi;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;

import static org.apache.tsfile.read.filter.factory.ValueFilterApi.DEFAULT_MEASUREMENT_INDEX;

public class SeriesScanPredicatePushDownTest extends AbstractSeriesScanTest {

  private SeriesScanUtil getSeriesScanUtil(Filter globalTimeFilter, Filter pushDownFilter)
      throws IllegalPathException {
    MeasurementPath scanPath = new MeasurementPath(TEST_PATH, TSDataType.INT32);

    SeriesScanOptions.Builder scanOptionsBuilder = new SeriesScanOptions.Builder();
    scanOptionsBuilder.withAllSensors(Collections.singleton(scanPath.getMeasurement()));
    scanOptionsBuilder.withGlobalTimeFilter(globalTimeFilter);
    scanOptionsBuilder.withPushDownFilter(pushDownFilter);
    SeriesScanUtil seriesScanUtil =
        new SeriesScanUtil(
            IFullPath.convertToIFullPath(scanPath),
            Ordering.ASC,
            scanOptionsBuilder.build(),
            EnvironmentUtils.TEST_QUERY_FI_CONTEXT);
    seriesScanUtil.initQueryDataSource(new QueryDataSource(seqResources, unSeqResources));
    return seriesScanUtil;
  }

  @Test
  @SuppressWarnings("squid:S5961") // Suppress "Test methods should not contain too many assertions"
  public void testNoFilter() throws IllegalPathException, IOException {
    SeriesScanUtil seriesScanUtil = getSeriesScanUtil(null, null);

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

    // File 2 - Chunk 2
    Assert.assertTrue(seriesScanUtil.hasNextChunk());
    Assert.assertTrue(seriesScanUtil.canUseCurrentChunkStatistics());

    // File 2 - Chunk 2 - Page 1
    Assert.assertTrue(seriesScanUtil.hasNextPage());
    Assert.assertTrue(seriesScanUtil.canUseCurrentPageStatistics());
    tsBlock = seriesScanUtil.nextPage();
    Assert.assertEquals(10, tsBlock.getPositionCount());
    Assert.assertFalse(seriesScanUtil.hasNextPage());
    Assert.assertFalse(seriesScanUtil.hasNextChunk());

    // File 3
    Assert.assertTrue(seriesScanUtil.hasNextFile());
    Assert.assertFalse(seriesScanUtil.canUseCurrentFileStatistics());

    // File 3 - Chunk 1
    Assert.assertTrue(seriesScanUtil.hasNextChunk());
    Assert.assertTrue(seriesScanUtil.canUseCurrentChunkStatistics());

    // File 3 - Chunk 1 - Page 1
    Assert.assertTrue(seriesScanUtil.hasNextPage());
    Assert.assertTrue(seriesScanUtil.canUseCurrentPageStatistics());
    tsBlock = seriesScanUtil.nextPage();
    Assert.assertEquals(10, tsBlock.getPositionCount());

    // File 3 - Chunk 1 - Page 2
    Assert.assertTrue(seriesScanUtil.hasNextPage());
    Assert.assertTrue(seriesScanUtil.canUseCurrentPageStatistics());
    tsBlock = seriesScanUtil.nextPage();
    Assert.assertEquals(10, tsBlock.getPositionCount());
    Assert.assertFalse(seriesScanUtil.hasNextPage());

    // (File 3 - Chunk 2) merge (File 4 - Chunk 1)
    Assert.assertTrue(seriesScanUtil.hasNextChunk());
    Assert.assertFalse(seriesScanUtil.canUseCurrentChunkStatistics());

    // (File 3 - Chunk 2 - Page 1) merge (File 4 - Chunk 1 - Page 1)
    Assert.assertTrue(seriesScanUtil.hasNextPage());
    Assert.assertFalse(seriesScanUtil.canUseCurrentPageStatistics());
    tsBlock = seriesScanUtil.nextPage();
    Assert.assertEquals(10, tsBlock.getPositionCount());

    // File 4 - Chunk 1 - Page 2
    Assert.assertTrue(seriesScanUtil.hasNextPage());
    Assert.assertTrue(seriesScanUtil.canUseCurrentPageStatistics());
    tsBlock = seriesScanUtil.nextPage();
    Assert.assertEquals(10, tsBlock.getPositionCount());
    Assert.assertFalse(seriesScanUtil.hasNextPage());
    Assert.assertFalse(seriesScanUtil.hasNextChunk());
    Assert.assertFalse(seriesScanUtil.hasNextFile());
  }

  @Test
  public void testSkipFileByGlobalTimeFilter() throws IllegalPathException, IOException {
    SeriesScanUtil seriesScanUtil = getSeriesScanUtil(TimeFilterApi.gtEq(10), null);
    checkFile1Skipped(seriesScanUtil);
  }

  @Test
  public void testSkipFileByPushDownFilter() throws IllegalPathException, IOException {
    SeriesScanUtil seriesScanUtil =
        getSeriesScanUtil(
            TimeFilterApi.gt(0),
            ValueFilterApi.gtEq(DEFAULT_MEASUREMENT_INDEX, 10, TSDataType.INT32));
    checkFile1Skipped(seriesScanUtil);
  }

  private void checkFile1Skipped(SeriesScanUtil seriesScanUtil) throws IOException {
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
    Assert.assertEquals(10, tsBlock.getTimeByIndex(0));
  }

  @Test
  public void testSkipChunkByGlobalTimeFilter() throws IllegalPathException, IOException {
    SeriesScanUtil seriesScanUtil = getSeriesScanUtil(TimeFilterApi.gtEq(20), null);
    checkFile2Chunk1Skipped(seriesScanUtil);
  }

  @Test
  public void testSkipChunkByPushDownFilter() throws IllegalPathException, IOException {
    SeriesScanUtil seriesScanUtil =
        getSeriesScanUtil(
            TimeFilterApi.gt(0),
            ValueFilterApi.gtEq(DEFAULT_MEASUREMENT_INDEX, 20, TSDataType.INT32));
    checkFile2Chunk1Skipped(seriesScanUtil);
  }

  private void checkFile2Chunk1Skipped(SeriesScanUtil seriesScanUtil) throws IOException {
    // File 1 skipped
    // File 2
    Assert.assertTrue(seriesScanUtil.hasNextFile());
    Assert.assertFalse(seriesScanUtil.canUseCurrentFileStatistics());

    // File 2 - Chunk 1 skipped
    // File 2 - Chunk 2
    Assert.assertTrue(seriesScanUtil.hasNextChunk());
    Assert.assertTrue(seriesScanUtil.canUseCurrentChunkStatistics());

    // File 2 - Chunk 2 - Page 1
    Assert.assertTrue(seriesScanUtil.hasNextPage());
    Assert.assertTrue(seriesScanUtil.canUseCurrentPageStatistics());
    TsBlock tsBlock = seriesScanUtil.nextPage();
    Assert.assertEquals(10, tsBlock.getPositionCount());
    Assert.assertEquals(20, tsBlock.getTimeByIndex(0));
  }

  @Test
  public void testSkipPageByGlobalTimeFilter() throws IllegalPathException, IOException {
    SeriesScanUtil seriesScanUtil = getSeriesScanUtil(TimeFilterApi.gtEq(40), null);
    checkFile1AndFile2Skipped(seriesScanUtil);

    // File 3 - Chunk 1 - Page 1 skipped
    // File 3 - Chunk 1 - Page 2
    Assert.assertTrue(seriesScanUtil.hasNextPage());
    Assert.assertTrue(seriesScanUtil.canUseCurrentPageStatistics());
    TsBlock tsBlock = seriesScanUtil.nextPage();
    Assert.assertEquals(10, tsBlock.getPositionCount());
    Assert.assertEquals(40, tsBlock.getTimeByIndex(0));
  }

  @Test
  public void testSkipPageByPushDownFilter() throws IllegalPathException, IOException {
    SeriesScanUtil seriesScanUtil =
        getSeriesScanUtil(
            TimeFilterApi.gt(0),
            ValueFilterApi.gtEq(DEFAULT_MEASUREMENT_INDEX, 40, TSDataType.INT32));
    checkFile1AndFile2Skipped(seriesScanUtil);

    // File 3 - Chunk 1 - Page 1 skipped
    Assert.assertTrue(seriesScanUtil.hasNextPage());
    Assert.assertFalse(seriesScanUtil.canUseCurrentPageStatistics());
    TsBlock tsBlock = seriesScanUtil.nextPage();
    Assert.assertNull(tsBlock);

    // File 3 - Chunk 1 - Page 2
    Assert.assertTrue(seriesScanUtil.hasNextPage());
    Assert.assertTrue(seriesScanUtil.canUseCurrentPageStatistics());
    tsBlock = seriesScanUtil.nextPage();
    Assert.assertEquals(10, tsBlock.getPositionCount());
    Assert.assertEquals(40, tsBlock.getTimeByIndex(0));
  }

  private void checkFile1AndFile2Skipped(SeriesScanUtil seriesScanUtil) throws IOException {
    // File 1 skipped
    // File 2 skipped
    // File 3
    Assert.assertTrue(seriesScanUtil.hasNextFile());
    Assert.assertFalse(seriesScanUtil.canUseCurrentFileStatistics());

    // File 3 - Chunk 1
    Assert.assertTrue(seriesScanUtil.hasNextChunk());
    Assert.assertFalse(seriesScanUtil.canUseCurrentChunkStatistics());
  }

  @Test
  public void testSkipMergeReaderPointByGlobalTimeFilter()
      throws IllegalPathException, IOException {
    SeriesScanUtil seriesScanUtil = getSeriesScanUtil(TimeFilterApi.gtEq(55), null);
    checkFile1AndFile2AndMergeReaderPointSkipped(seriesScanUtil);
  }

  @Test
  public void testSkipMergeReaderPointByPushDownFilter() throws IllegalPathException, IOException {
    SeriesScanUtil seriesScanUtil =
        getSeriesScanUtil(
            TimeFilterApi.gt(0),
            ValueFilterApi.gtEq(DEFAULT_MEASUREMENT_INDEX, 55, TSDataType.INT32));
    checkFile1AndFile2AndMergeReaderPointSkipped(seriesScanUtil);
  }

  private void checkFile1AndFile2AndFile3Chunk1Skipped(SeriesScanUtil seriesScanUtil)
      throws IOException {
    // File 1 skipped
    // File 2 skipped
    // File 3
    Assert.assertTrue(seriesScanUtil.hasNextFile());
    Assert.assertFalse(seriesScanUtil.canUseCurrentFileStatistics());

    // File 3 - Chunk 1 skipped
    // File 3 - Chunk 2
    Assert.assertTrue(seriesScanUtil.hasNextChunk());
    Assert.assertFalse(seriesScanUtil.canUseCurrentChunkStatistics());
  }

  private void checkFile1AndFile2AndMergeReaderPointSkipped(SeriesScanUtil seriesScanUtil)
      throws IOException {
    checkFile1AndFile2AndFile3Chunk1Skipped(seriesScanUtil);

    // (File 3 - Chunk 2) merge (File 4 - Chunk 1)
    Assert.assertTrue(seriesScanUtil.hasNextPage());
    Assert.assertFalse(seriesScanUtil.canUseCurrentPageStatistics());
    TsBlock tsBlock = seriesScanUtil.nextPage();
    Assert.assertEquals(5, tsBlock.getPositionCount());
    Assert.assertEquals(55, tsBlock.getTimeByIndex(0));
  }

  @Test
  public void testSkipMergeReaderByGlobalTimeFilter() throws IllegalPathException, IOException {
    SeriesScanUtil seriesScanUtil = getSeriesScanUtil(TimeFilterApi.gtEq(60), null);
    checkFile1AndFile2AndFile3Chunk1Skipped(seriesScanUtil);

    // (File 3 - Chunk 1) merge (File 4 - Chunk 1) skipped
    // File 4 - Chunk 2
    Assert.assertTrue(seriesScanUtil.hasNextPage());
    Assert.assertTrue(seriesScanUtil.canUseCurrentPageStatistics());
    TsBlock tsBlock = seriesScanUtil.nextPage();
    Assert.assertEquals(10, tsBlock.getPositionCount());
    Assert.assertEquals(60, tsBlock.getTimeByIndex(0));
  }

  @Test
  public void testSkipMergeReaderByPushDownFilter() throws IllegalPathException, IOException {
    SeriesScanUtil seriesScanUtil =
        getSeriesScanUtil(
            TimeFilterApi.gt(0),
            ValueFilterApi.gtEq(DEFAULT_MEASUREMENT_INDEX, 60, TSDataType.INT32));

    checkFile1AndFile2AndFile3Chunk1Skipped(seriesScanUtil);

    // (File 3 - Chunk 1) merge (File 4 - Chunk 1)
    Assert.assertTrue(seriesScanUtil.hasNextPage());
    Assert.assertFalse(seriesScanUtil.canUseCurrentPageStatistics());
    TsBlock tsBlock = seriesScanUtil.nextPage();
    Assert.assertTrue(tsBlock == null || tsBlock.isEmpty());

    // File 4 - Chunk 2
    Assert.assertTrue(seriesScanUtil.hasNextPage());
    Assert.assertTrue(seriesScanUtil.canUseCurrentPageStatistics());
    tsBlock = seriesScanUtil.nextPage();
    Assert.assertEquals(10, tsBlock.getPositionCount());
    Assert.assertEquals(60, tsBlock.getTimeByIndex(0));
  }
}
