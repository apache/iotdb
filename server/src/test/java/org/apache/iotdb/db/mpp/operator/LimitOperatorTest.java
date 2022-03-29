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
import org.apache.iotdb.db.mpp.operator.process.LimitOperator;
import org.apache.iotdb.db.mpp.operator.process.TimeJoinOperator;
import org.apache.iotdb.db.mpp.operator.source.SeriesScanOperator;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.sql.statement.component.OrderBy;
import org.apache.iotdb.db.query.reader.series.SeriesReaderTestUtil;
import org.apache.iotdb.db.utils.QueryUtils;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.column.IntColumn;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class LimitOperatorTest {

  private static final String TIME_JOIN_OPERATOR_TEST_SG = "root.LimitOperatorTest";
  private final List<String> deviceIds = new ArrayList<>();
  private final List<MeasurementSchema> measurementSchemas = new ArrayList<>();

  private final List<TsFileResource> seqResources = new ArrayList<>();
  private final List<TsFileResource> unSeqResources = new ArrayList<>();

  @Before
  public void setUp() throws MetadataException, IOException, WriteProcessException {
    SeriesReaderTestUtil.setUp(
        measurementSchemas, deviceIds, seqResources, unSeqResources, TIME_JOIN_OPERATOR_TEST_SG);
  }

  @After
  public void tearDown() throws IOException {
    SeriesReaderTestUtil.tearDown(seqResources, unSeqResources);
  }

  @Test
  public void batchTest() {
    try {
      MeasurementPath measurementPath1 =
          new MeasurementPath(TIME_JOIN_OPERATOR_TEST_SG + ".device0.sensor0", TSDataType.INT32);
      Set<String> allSensors = new HashSet<>();
      allSensors.add("sensor0");
      allSensors.add("sensor1");
      QueryId queryId = new QueryId("stub_query");
      FragmentInstanceContext fragmentInstanceContext =
          new FragmentInstanceContext(
              new FragmentInstanceId(new PlanFragmentId(queryId, 0), "stub-instance"));
      fragmentInstanceContext.addOperatorContext(
          1, new PlanNodeId("1"), SeriesScanOperator.class.getSimpleName());
      fragmentInstanceContext.addOperatorContext(
          2, new PlanNodeId("2"), SeriesScanOperator.class.getSimpleName());
      fragmentInstanceContext.addOperatorContext(
          3, new PlanNodeId("3"), TimeJoinOperator.class.getSimpleName());
      fragmentInstanceContext.addOperatorContext(
          4, new PlanNodeId("4"), LimitOperator.class.getSimpleName());
      QueryDataSource dataSource = new QueryDataSource(seqResources, unSeqResources);
      QueryUtils.fillOrderIndexes(dataSource, measurementPath1.getDevice(), true);
      SeriesScanOperator seriesScanOperator1 =
          new SeriesScanOperator(
              measurementPath1,
              allSensors,
              TSDataType.INT32,
              fragmentInstanceContext.getOperatorContexts().get(0),
              dataSource,
              null,
              null,
              true);

      MeasurementPath measurementPath2 =
          new MeasurementPath(TIME_JOIN_OPERATOR_TEST_SG + ".device0.sensor1", TSDataType.INT32);
      SeriesScanOperator seriesScanOperator2 =
          new SeriesScanOperator(
              measurementPath2,
              allSensors,
              TSDataType.INT32,
              fragmentInstanceContext.getOperatorContexts().get(1),
              dataSource,
              null,
              null,
              true);

      TimeJoinOperator timeJoinOperator =
          new TimeJoinOperator(
              fragmentInstanceContext.getOperatorContexts().get(2),
              Arrays.asList(seriesScanOperator1, seriesScanOperator2),
              OrderBy.TIMESTAMP_ASC,
              2,
              Arrays.asList(TSDataType.INT32, TSDataType.INT32));

      LimitOperator limitOperator =
          new LimitOperator(
              fragmentInstanceContext.getOperatorContexts().get(3), 100, timeJoinOperator);
      int count = 0;
      System.out.println("Time sensor0 sensor1");
      while (limitOperator.hasNext()) {
        TsBlock tsBlock = limitOperator.next();
        assertEquals(2, tsBlock.getValueColumnCount());
        assertTrue(tsBlock.getColumn(0) instanceof IntColumn);
        assertTrue(tsBlock.getColumn(1) instanceof IntColumn);
//        if (count < 12) {
//          assertEquals(20, tsBlock.getPositionCount());
//        } else {
//          assertEquals(10, tsBlock.getPositionCount());
//        }
        for (int i = 0; i < tsBlock.getPositionCount(); i++) {
          long expectedTime = i + 20L * count;
          System.out.println(
              expectedTime
                  + " \t "
                  + tsBlock.getColumn(0).getInt(i)
                  + " \t "
                  + tsBlock.getColumn(1).getInt(i));
          assertEquals(expectedTime, tsBlock.getTimeByIndex(i));
          if (expectedTime < 200) {
            assertEquals(20000 + expectedTime, tsBlock.getColumn(0).getInt(i));
            assertEquals(20000 + expectedTime, tsBlock.getColumn(1).getInt(i));
          } else if (expectedTime < 260
              || (expectedTime >= 300 && expectedTime < 380)
              || expectedTime >= 400) {
            assertEquals(10000 + expectedTime, tsBlock.getColumn(0).getInt(i));
            assertEquals(10000 + expectedTime, tsBlock.getColumn(1).getInt(i));
          } else {
            assertEquals(expectedTime, tsBlock.getColumn(0).getInt(i));
            assertEquals(expectedTime, tsBlock.getColumn(1).getInt(i));
          }
        }
        count++;
      }
//      assertEquals(13, count);
    } catch (IOException | IllegalPathException e) {
      e.printStackTrace();
      fail();
    }
  }
}
