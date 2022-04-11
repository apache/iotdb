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
package org.apache.iotdb.db.mpp.execution;

import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.engine.storagegroup.DataRegion;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.path.MeasurementPath;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.mpp.buffer.StubSinkHandle;
import org.apache.iotdb.db.mpp.common.FragmentInstanceId;
import org.apache.iotdb.db.mpp.common.PlanFragmentId;
import org.apache.iotdb.db.mpp.common.QueryId;
import org.apache.iotdb.db.mpp.operator.process.LimitOperator;
import org.apache.iotdb.db.mpp.operator.process.TimeJoinOperator;
import org.apache.iotdb.db.mpp.operator.source.SeriesScanOperator;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.sql.statement.component.OrderBy;
import org.apache.iotdb.db.query.reader.series.SeriesReaderTestUtil;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.column.IntColumn;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.iotdb.db.mpp.schedule.FragmentInstanceTaskExecutor.EXECUTION_TIME_SLICE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class DataDriverTest {

  private static final String DATA_DRIVER_TEST_SG = "root.DataDriverTest";
  private final List<String> deviceIds = new ArrayList<>();
  private final List<MeasurementSchema> measurementSchemas = new ArrayList<>();

  private final List<TsFileResource> seqResources = new ArrayList<>();
  private final List<TsFileResource> unSeqResources = new ArrayList<>();

  @Before
  public void setUp() throws MetadataException, IOException, WriteProcessException {
    SeriesReaderTestUtil.setUp(
        measurementSchemas, deviceIds, seqResources, unSeqResources, DATA_DRIVER_TEST_SG);
  }

  @After
  public void tearDown() throws IOException {
    SeriesReaderTestUtil.tearDown(seqResources, unSeqResources);
  }

  @Test
  public void batchTest() {
    try {
      MeasurementPath measurementPath1 =
          new MeasurementPath(DATA_DRIVER_TEST_SG + ".device0.sensor0", TSDataType.INT32);
      Set<String> allSensors = new HashSet<>();
      allSensors.add("sensor0");
      allSensors.add("sensor1");
      QueryId queryId = new QueryId("stub_query");
      AtomicReference<FragmentInstanceState> state =
          new AtomicReference<>(FragmentInstanceState.RUNNING);
      FragmentInstanceContext fragmentInstanceContext =
          new FragmentInstanceContext(
              new FragmentInstanceId(new PlanFragmentId(queryId, 0), "stub-instance"), state);
      fragmentInstanceContext.addOperatorContext(
          1, new PlanNodeId("1"), SeriesScanOperator.class.getSimpleName());
      fragmentInstanceContext.addOperatorContext(
          2, new PlanNodeId("2"), SeriesScanOperator.class.getSimpleName());
      fragmentInstanceContext.addOperatorContext(
          3, new PlanNodeId("3"), TimeJoinOperator.class.getSimpleName());
      fragmentInstanceContext.addOperatorContext(
          4, new PlanNodeId("4"), LimitOperator.class.getSimpleName());
      SeriesScanOperator seriesScanOperator1 =
          new SeriesScanOperator(
              measurementPath1,
              allSensors,
              TSDataType.INT32,
              fragmentInstanceContext.getOperatorContexts().get(0),
              null,
              null,
              true);

      MeasurementPath measurementPath2 =
          new MeasurementPath(DATA_DRIVER_TEST_SG + ".device0.sensor1", TSDataType.INT32);
      SeriesScanOperator seriesScanOperator2 =
          new SeriesScanOperator(
              measurementPath2,
              allSensors,
              TSDataType.INT32,
              fragmentInstanceContext.getOperatorContexts().get(1),
              null,
              null,
              true);

      TimeJoinOperator timeJoinOperator =
          new TimeJoinOperator(
              fragmentInstanceContext.getOperatorContexts().get(2),
              Arrays.asList(seriesScanOperator1, seriesScanOperator2),
              OrderBy.TIMESTAMP_ASC,
              Arrays.asList(TSDataType.INT32, TSDataType.INT32));

      LimitOperator limitOperator =
          new LimitOperator(
              fragmentInstanceContext.getOperatorContexts().get(3), 250, timeJoinOperator);

      DataRegion dataRegion = Mockito.mock(DataRegion.class);

      List<PartialPath> pathList = ImmutableList.of(measurementPath1, measurementPath2);
      String deviceId = DATA_DRIVER_TEST_SG + ".device0";

      Mockito.when(dataRegion.query(pathList, deviceId, fragmentInstanceContext, null))
          .thenReturn(new QueryDataSource(seqResources, unSeqResources));

      DataDriverContext driverContext =
          new DataDriverContext(
              fragmentInstanceContext,
              pathList,
              null,
              dataRegion,
              ImmutableList.of(seriesScanOperator1, seriesScanOperator2));

      StubSinkHandle sinkHandle = new StubSinkHandle();

      try (Driver dataDriver = new DataDriver(limitOperator, sinkHandle, driverContext)) {
        assertEquals(fragmentInstanceContext.getId(), dataDriver.getInfo());

        assertFalse(dataDriver.isFinished());

        while (!dataDriver.isFinished()) {
          assertEquals(FragmentInstanceState.RUNNING, state.get());
          ListenableFuture<Void> blocked = dataDriver.processFor(EXECUTION_TIME_SLICE);
          assertTrue(blocked.isDone());
        }

        assertEquals(FragmentInstanceState.FINISHED, state.get());

        List<TsBlock> result = sinkHandle.getTsBlocks();
        assertEquals(13, result.size());

        for (int i = 0; i < 13; i++) {
          TsBlock tsBlock = result.get(i);
          assertEquals(2, tsBlock.getValueColumnCount());
          assertTrue(tsBlock.getColumn(0) instanceof IntColumn);
          assertTrue(tsBlock.getColumn(1) instanceof IntColumn);

          if (i < 12) {
            assertEquals(20, tsBlock.getPositionCount());
          } else {
            assertEquals(10, tsBlock.getPositionCount());
          }
          for (int j = 0; j < tsBlock.getPositionCount(); j++) {
            long expectedTime = j + 20L * i;
            assertEquals(expectedTime, tsBlock.getTimeByIndex(j));
            if (expectedTime < 200) {
              assertEquals(20000 + expectedTime, tsBlock.getColumn(0).getInt(j));
              assertEquals(20000 + expectedTime, tsBlock.getColumn(1).getInt(j));
            } else if (expectedTime < 260
                || (expectedTime >= 300 && expectedTime < 380)
                || expectedTime >= 400) {
              assertEquals(10000 + expectedTime, tsBlock.getColumn(0).getInt(j));
              assertEquals(10000 + expectedTime, tsBlock.getColumn(1).getInt(j));
            } else {
              assertEquals(expectedTime, tsBlock.getColumn(0).getInt(j));
              assertEquals(expectedTime, tsBlock.getColumn(1).getInt(j));
            }
          }
        }
      }
    } catch (IllegalPathException | QueryProcessException e) {
      e.printStackTrace();
      fail();
    }
  }
}
