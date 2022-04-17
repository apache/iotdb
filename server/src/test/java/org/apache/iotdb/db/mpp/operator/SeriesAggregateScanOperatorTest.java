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
package org.apache.iotdb.db.mpp.operator;

import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.path.MeasurementPath;
import org.apache.iotdb.db.mpp.common.FragmentInstanceId;
import org.apache.iotdb.db.mpp.common.PlanFragmentId;
import org.apache.iotdb.db.mpp.common.QueryId;
import org.apache.iotdb.db.mpp.execution.FragmentInstanceContext;
import org.apache.iotdb.db.mpp.execution.FragmentInstanceState;
import org.apache.iotdb.db.mpp.operator.source.SeriesAggregateScanOperator;
import org.apache.iotdb.db.mpp.operator.source.SeriesScanOperator;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.sql.statement.component.GroupByTimeComponent;
import org.apache.iotdb.db.query.aggregation.AggregationType;
import org.apache.iotdb.db.query.reader.series.SeriesReaderTestUtil;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import com.google.common.collect.Sets;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;

public class SeriesAggregateScanOperatorTest {

  private static final String SERIES_SCAN_OPERATOR_TEST_SG = "root.SeriesScanOperatorTest";
  private final List<String> deviceIds = new ArrayList<>();
  private final List<MeasurementSchema> measurementSchemas = new ArrayList<>();

  private final List<TsFileResource> seqResources = new ArrayList<>();
  private final List<TsFileResource> unSeqResources = new ArrayList<>();

  @Before
  public void setUp() throws MetadataException, IOException, WriteProcessException {
    SeriesReaderTestUtil.setUp(
        measurementSchemas, deviceIds, seqResources, unSeqResources, SERIES_SCAN_OPERATOR_TEST_SG);
  }

  @After
  public void tearDown() throws IOException {
    SeriesReaderTestUtil.tearDown(seqResources, unSeqResources);
  }

  @Test
  public void testAggregationUsingSequenceFileStatistics() throws IllegalPathException {
    SeriesAggregateScanOperator seriesAggregateScanOperator =
        initSeriesAggregateScanOperator(
            Collections.singletonList(AggregationType.COUNT), null, true, null);
    int count = 0;
    while (seriesAggregateScanOperator.hasNext()) {
      TsBlock resultTsBlock = seriesAggregateScanOperator.next();
      assertEquals(resultTsBlock.getColumn(0).getLong(0), 500);
      count++;
    }
    assertEquals(1, count);
  }

  @Test
  public void testAggregationUsingSequenceChunkStatistics() {}

  @Test
  public void testAggregationUsingSequencePageStatistics() {}

  @Test
  public void testAggregationUsingTsBlock() {}

  @Test
  public void testGroupByUsingSequenceFileStatistics() {}

  @Test
  public void testGroupByUsingSequenceChunkStatistics() {}

  @Test
  public void testGroupByUsingSequencePageStatistics() {}

  @Test
  public void testGroupByUsingTsBlock() {}

  public SeriesAggregateScanOperator initSeriesAggregateScanOperator(
      List<AggregationType> aggregateFuncList,
      Filter timeFilter,
      boolean ascending,
      GroupByTimeComponent groupByTimeParameter)
      throws IllegalPathException {
    MeasurementPath measurementPath =
        new MeasurementPath(SERIES_SCAN_OPERATOR_TEST_SG + ".device0.sensor0", TSDataType.INT32);
    Set<String> allSensors = Sets.newHashSet("sensor0");
    QueryId queryId = new QueryId("stub_query");
    AtomicReference<FragmentInstanceState> state =
        new AtomicReference<>(FragmentInstanceState.RUNNING);
    FragmentInstanceContext fragmentInstanceContext =
        new FragmentInstanceContext(
            new FragmentInstanceId(new PlanFragmentId(queryId, 0), "stub-instance"), state);
    PlanNodeId planNodeId = new PlanNodeId("1");
    fragmentInstanceContext.addOperatorContext(
        1, planNodeId, SeriesScanOperator.class.getSimpleName());

    SeriesAggregateScanOperator seriesAggregateScanOperator =
        new SeriesAggregateScanOperator(
            planNodeId,
            measurementPath,
            allSensors,
            fragmentInstanceContext.getOperatorContexts().get(0),
            aggregateFuncList,
            timeFilter,
            ascending,
            groupByTimeParameter);
    seriesAggregateScanOperator.initQueryDataSource(
        new QueryDataSource(seqResources, unSeqResources));
    return seriesAggregateScanOperator;
  }
}
