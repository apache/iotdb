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
package org.apache.iotdb.db.mpp.execution.operator.schema;

import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.localconfignode.LocalConfigNode;
import org.apache.iotdb.db.metadata.schemaregion.ISchemaRegion;
import org.apache.iotdb.db.metadata.schemaregion.SchemaEngine;
import org.apache.iotdb.db.mpp.common.FragmentInstanceId;
import org.apache.iotdb.db.mpp.common.PlanFragmentId;
import org.apache.iotdb.db.mpp.common.QueryId;
import org.apache.iotdb.db.mpp.execution.driver.SchemaDriverContext;
import org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceStateMachine;
import org.apache.iotdb.db.mpp.execution.operator.OperatorContext;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.query.reader.series.SeriesReaderTestUtil;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;

import static org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceContext.createFragmentInstanceContext;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class CountMergeOperatorTest {
  private static final String COUNT_MERGE_OPERATOR_TEST_SG = "root.CountMergeOperatorTest";
  private final List<String> deviceIds = new ArrayList<>();
  private final List<MeasurementSchema> measurementSchemas = new ArrayList<>();

  private final List<TsFileResource> seqResources = new ArrayList<>();
  private final List<TsFileResource> unSeqResources = new ArrayList<>();

  @Before
  public void setUp() throws MetadataException, IOException, WriteProcessException {
    SeriesReaderTestUtil.setUp(
        measurementSchemas, deviceIds, seqResources, unSeqResources, COUNT_MERGE_OPERATOR_TEST_SG);
  }

  @After
  public void tearDown() throws IOException {
    SeriesReaderTestUtil.tearDown(seqResources, unSeqResources);
  }

  @Test
  public void testTimeSeriesCountOperator() {
    ExecutorService instanceNotificationExecutor =
        IoTDBThreadPoolFactory.newFixedThreadPool(1, "test-instance-notification");
    try {
      QueryId queryId = new QueryId("stub_query");
      FragmentInstanceId instanceId =
          new FragmentInstanceId(new PlanFragmentId(queryId, 0), "stub-instance");
      FragmentInstanceStateMachine stateMachine =
          new FragmentInstanceStateMachine(instanceId, instanceNotificationExecutor);
      FragmentInstanceContext fragmentInstanceContext =
          createFragmentInstanceContext(instanceId, stateMachine);
      PlanNodeId planNodeId = queryId.genPlanNodeId();
      OperatorContext operatorContext =
          fragmentInstanceContext.addOperatorContext(
              1, planNodeId, TimeSeriesCountOperator.class.getSimpleName());
      PartialPath partialPath = new PartialPath(COUNT_MERGE_OPERATOR_TEST_SG);
      ISchemaRegion schemaRegion =
          SchemaEngine.getInstance()
              .getSchemaRegion(
                  LocalConfigNode.getInstance().getBelongedSchemaRegionId(partialPath));
      operatorContext
          .getInstanceContext()
          .setDriverContext(new SchemaDriverContext(fragmentInstanceContext, schemaRegion));
      TimeSeriesCountOperator timeSeriesCountOperator =
          new TimeSeriesCountOperator(
              planNodeId,
              fragmentInstanceContext.getOperatorContexts().get(0),
              partialPath,
              true,
              null,
              null,
              false,
              Collections.emptyMap());
      TsBlock tsBlock = null;
      while (timeSeriesCountOperator.hasNext()) {
        tsBlock = timeSeriesCountOperator.next();
      }
      assertNotNull(tsBlock);
      assertEquals(100, tsBlock.getColumn(0).getInt(0));
      TimeSeriesCountOperator timeSeriesCountOperator2 =
          new TimeSeriesCountOperator(
              planNodeId,
              fragmentInstanceContext.getOperatorContexts().get(0),
              new PartialPath(COUNT_MERGE_OPERATOR_TEST_SG + ".device1.*"),
              false,
              null,
              null,
              false,
              Collections.emptyMap());
      tsBlock = timeSeriesCountOperator2.next();
      assertFalse(timeSeriesCountOperator2.hasNext());
      assertTrue(timeSeriesCountOperator2.isFinished());
      assertEquals(10, tsBlock.getColumn(0).getInt(0));
    } catch (MetadataException e) {
      e.printStackTrace();
      fail();
    } finally {
      instanceNotificationExecutor.shutdown();
    }
  }

  @Test
  public void testCountMergeOperator() {
    ExecutorService instanceNotificationExecutor =
        IoTDBThreadPoolFactory.newFixedThreadPool(1, "test-instance-notification");
    try {
      QueryId queryId = new QueryId("stub_query");
      FragmentInstanceId instanceId =
          new FragmentInstanceId(new PlanFragmentId(queryId, 0), "stub-instance");
      FragmentInstanceStateMachine stateMachine =
          new FragmentInstanceStateMachine(instanceId, instanceNotificationExecutor);
      FragmentInstanceContext fragmentInstanceContext =
          createFragmentInstanceContext(instanceId, stateMachine);
      PlanNodeId planNodeId = queryId.genPlanNodeId();
      OperatorContext operatorContext =
          fragmentInstanceContext.addOperatorContext(
              1, planNodeId, LevelTimeSeriesCountOperator.class.getSimpleName());
      ISchemaRegion schemaRegion =
          SchemaEngine.getInstance()
              .getSchemaRegion(
                  LocalConfigNode.getInstance()
                      .getBelongedSchemaRegionId(new PartialPath(COUNT_MERGE_OPERATOR_TEST_SG)));
      operatorContext
          .getInstanceContext()
          .setDriverContext(new SchemaDriverContext(fragmentInstanceContext, schemaRegion));
      LevelTimeSeriesCountOperator timeSeriesCountOperator1 =
          new LevelTimeSeriesCountOperator(
              planNodeId,
              fragmentInstanceContext.getOperatorContexts().get(0),
              new PartialPath(COUNT_MERGE_OPERATOR_TEST_SG),
              true,
              2,
              null,
              null,
              false);
      LevelTimeSeriesCountOperator timeSeriesCountOperator2 =
          new LevelTimeSeriesCountOperator(
              planNodeId,
              fragmentInstanceContext.getOperatorContexts().get(0),
              new PartialPath(COUNT_MERGE_OPERATOR_TEST_SG + ".device2"),
              true,
              2,
              null,
              null,
              false);
      CountMergeOperator countMergeOperator =
          new CountMergeOperator(
              planNodeId,
              fragmentInstanceContext.getOperatorContexts().get(0),
              Arrays.asList(timeSeriesCountOperator1, timeSeriesCountOperator2));
      TsBlock tsBlock = null;
      while (countMergeOperator.hasNext()) {
        tsBlock = countMergeOperator.next();
        assertFalse(countMergeOperator.hasNext());
      }
      assertNotNull(tsBlock);
      for (int i = 0; i < 10; i++) {
        String path = tsBlock.getColumn(0).getBinary(i).getStringValue();
        assertTrue(path.startsWith(COUNT_MERGE_OPERATOR_TEST_SG + ".device"));
        if (path.equals(COUNT_MERGE_OPERATOR_TEST_SG + ".device2")) {
          assertEquals(20, tsBlock.getColumn(1).getInt(i));
        } else {
          assertEquals(10, tsBlock.getColumn(1).getInt(i));
        }
      }
    } catch (MetadataException e) {
      e.printStackTrace();
      fail();
    } finally {
      instanceNotificationExecutor.shutdown();
    }
  }
}
