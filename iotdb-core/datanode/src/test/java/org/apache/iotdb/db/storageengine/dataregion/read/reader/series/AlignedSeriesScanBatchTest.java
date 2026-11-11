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

/** Verifies the wide-table batch metadata scan (see FileLoaderUtils) reads data correctly. */
public class AlignedSeriesScanBatchTest extends AbstractAlignedSeriesScanTest {

  @Test
  public void testBatchScanReadsAlignedData() throws IOException, IllegalPathException {
    AlignedFullPath scanPath =
        new AlignedFullPath(
            TEST_DEVICE,
            Arrays.asList("s1", "s2", "s3"),
            Arrays.asList(
                new MeasurementSchema("s1", TSDataType.INT32),
                new MeasurementSchema("s2", TSDataType.INT32),
                new MeasurementSchema("s3", TSDataType.INT32)));

    SeriesScanOptions.Builder builder = new SeriesScanOptions.Builder();
    builder.withAllSensors(new HashSet<>(scanPath.getMeasurementList()));
    SeriesScanOptions scanOptions = builder.build();
    // all 3 value columns are selected, so selected/total = 1.0 >= 0.6 and the batch path is taken
    scanOptions.setTotalFieldColumnCount(3);

    AlignedSeriesScanUtil seriesScanUtil =
        new AlignedSeriesScanUtil(
            scanPath, Ordering.ASC, scanOptions, EnvironmentUtils.TEST_QUERY_FI_CONTEXT);
    seriesScanUtil.initQueryDataSource(new QueryDataSource(seqResources, unSeqResources));

    // the batch path must still assemble the same aligned pages: file 1 has 10 points
    SeriesReaderTestUtil.assertWithHasNext(seriesScanUtil::hasNextFile, true);
    SeriesReaderTestUtil.assertWithHasNext(seriesScanUtil::hasNextChunk, true);
    Assert.assertTrue(seriesScanUtil.hasNextPage());
    TsBlock tsBlock = seriesScanUtil.nextPage();
    Assert.assertEquals(10, tsBlock.getPositionCount());
  }
}
