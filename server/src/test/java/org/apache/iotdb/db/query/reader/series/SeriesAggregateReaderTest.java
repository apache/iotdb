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

package org.apache.iotdb.db.query.reader.series;

import static org.apache.iotdb.db.conf.IoTDBConstant.PATH_SEPARATOR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.query.PathException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.query.aggregation.AggregateResult;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.factory.AggreResultFactory;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SeriesAggregateReaderTest {

  private static final String SERIES_READER_TEST_SG = "root.seriesReaderTest";
  private List<String> deviceIds = new ArrayList<>();
  private List<MeasurementSchema> measurementSchemas = new ArrayList<>();

  private List<TsFileResource> seqResources = new ArrayList<>();
  private List<TsFileResource> unseqResources = new ArrayList<>();


  @Before
  public void setUp() throws MetadataException, PathException, IOException, WriteProcessException {
    SeriesReaderTestUtil.setUp(measurementSchemas, deviceIds, seqResources, unseqResources);
  }

  @After
  public void tearDown() throws IOException, StorageEngineException {
    SeriesReaderTestUtil.tearDown(seqResources, unseqResources);
  }

  @Test
  public void aggregateTest() {
    try {
      Path path = new Path(SERIES_READER_TEST_SG + PATH_SEPARATOR + "device0", "sensor0");
      QueryDataSource queryDataSource = new QueryDataSource(path, seqResources, unseqResources);
      SeriesAggregateReader seriesReader = new SeriesAggregateReader(path, TSDataType.INT32,
          new QueryContext(), queryDataSource, null, null);
      AggregateResult aggregateResult = AggreResultFactory
          .getAggrResultByName("count", TSDataType.INT32);
      while (seriesReader.hasNextChunk()) {
        if (seriesReader.canUseCurrentChunkStatistics()) {
          Statistics chunkStatistics = seriesReader.currentChunkStatistics();
          aggregateResult.updateResultFromStatistics(chunkStatistics);
          seriesReader.skipCurrentChunk();
          continue;
        }
        int loopTime = 0;
        while (seriesReader.hasNextPage()) {
          if (seriesReader.canUseCurrentPageStatistics()) {
            Statistics pageStatistic = seriesReader.currentPageStatistics();
            if (loopTime == 0) {
              assertEquals(260, pageStatistic.getStartTime());
              assertEquals(279, pageStatistic.getEndTime());
            } else if (loopTime == 1) {
              assertEquals(280, pageStatistic.getStartTime());
              assertEquals(299, pageStatistic.getEndTime());
            } else {
              assertEquals(380, pageStatistic.getStartTime());
              assertEquals(399, pageStatistic.getEndTime());
            }
            assertEquals(20, pageStatistic.getCount());
            aggregateResult.updateResultFromStatistics(pageStatistic);
            seriesReader.skipCurrentPage();
            continue;
          }

          while (seriesReader.hasNextOverlappedPage()) {
            BatchData nextOverlappedPageData = seriesReader.nextOverlappedPage();
            aggregateResult.updateResultFromPageData(nextOverlappedPageData);
            nextOverlappedPageData.resetBatchData();
            assertEquals(true, nextOverlappedPageData.hasCurrent());
          }
          loopTime++;
        }
      }
      assertEquals(500L, aggregateResult.getResult());
    } catch (IOException | QueryProcessException e) {
      e.printStackTrace();
      fail();
    }
  }
}
