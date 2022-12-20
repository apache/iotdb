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
import org.apache.iotdb.db.mpp.common.header.ColumnHeaderConstant;
import org.apache.iotdb.db.mpp.execution.driver.SchemaDriverContext;
import org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceStateMachine;
import org.apache.iotdb.db.mpp.execution.operator.OperatorContext;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.query.reader.series.SeriesReaderTestUtil;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.column.BinaryColumn;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;

import static org.apache.iotdb.commons.conf.IoTDBConstant.COLUMN_DATABASE;
import static org.apache.iotdb.commons.conf.IoTDBConstant.COLUMN_DEVICES;
import static org.apache.iotdb.commons.conf.IoTDBConstant.COLUMN_IS_ALIGNED;
import static org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceContext.createFragmentInstanceContext;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Ignore
public class SchemaQueryScanOperatorTest {
  private static final String META_SCAN_OPERATOR_TEST_SG = "root.MetaScanOperatorTest";
  private final List<String> deviceIds = new ArrayList<>();
  private final List<MeasurementSchema> measurementSchemas = new ArrayList<>();

  private final List<TsFileResource> seqResources = new ArrayList<>();
  private final List<TsFileResource> unSeqResources = new ArrayList<>();

  @Before
  public void setUp() throws MetadataException, IOException, WriteProcessException {
    SeriesReaderTestUtil.setUp(
        measurementSchemas, deviceIds, seqResources, unSeqResources, META_SCAN_OPERATOR_TEST_SG);
  }

  @After
  public void tearDown() throws IOException {
    SeriesReaderTestUtil.tearDown(seqResources, unSeqResources);
  }

  @Test
  public void testDeviceMetaScanOperator() {
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
              1, planNodeId, SchemaQueryScanOperator.class.getSimpleName());
      PartialPath partialPath = new PartialPath(META_SCAN_OPERATOR_TEST_SG + ".device0");
      ISchemaRegion schemaRegion =
          SchemaEngine.getInstance()
              .getSchemaRegion(
                  LocalConfigNode.getInstance().getBelongedSchemaRegionId(partialPath));
      operatorContext.setDriverContext(
          new SchemaDriverContext(fragmentInstanceContext, schemaRegion));
      List<String> columns = Arrays.asList(COLUMN_DEVICES, COLUMN_DATABASE, COLUMN_IS_ALIGNED);
      DevicesSchemaScanOperator deviceMetaScanOperator =
          new DevicesSchemaScanOperator(
              planNodeId,
              fragmentInstanceContext.getOperatorContexts().get(0),
              10,
              0,
              partialPath,
              false,
              true);
      while (deviceMetaScanOperator.hasNext()) {
        TsBlock tsBlock = deviceMetaScanOperator.next();
        assertEquals(3, tsBlock.getValueColumnCount());
        assertTrue(tsBlock.getColumn(0) instanceof BinaryColumn);
        assertEquals(1, tsBlock.getPositionCount());
        for (int i = 0; i < tsBlock.getPositionCount(); i++) {
          Assert.assertEquals(0, tsBlock.getTimeByIndex(i));
          for (int j = 0; j < columns.size(); j++) {
            switch (j) {
              case 0:
                assertEquals(
                    tsBlock.getColumn(j).getBinary(i).toString(),
                    META_SCAN_OPERATOR_TEST_SG + ".device0");
                break;
              case 1:
                assertEquals(
                    tsBlock.getColumn(j).getBinary(i).toString(), META_SCAN_OPERATOR_TEST_SG);
                break;
              case 2:
                assertEquals("false", tsBlock.getColumn(j).getBinary(i).toString());
                break;
              default:
                break;
            }
          }
        }
      }
    } catch (MetadataException e) {
      e.printStackTrace();
      fail();
    } finally {
      instanceNotificationExecutor.shutdown();
    }
  }

  @Test
  public void testTimeSeriesMetaScanOperator() {
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
              1, planNodeId, SchemaQueryScanOperator.class.getSimpleName());
      PartialPath partialPath = new PartialPath(META_SCAN_OPERATOR_TEST_SG + ".device0.*");
      ISchemaRegion schemaRegion =
          SchemaEngine.getInstance()
              .getSchemaRegion(
                  LocalConfigNode.getInstance().getBelongedSchemaRegionId(partialPath));
      operatorContext.setDriverContext(
          new SchemaDriverContext(fragmentInstanceContext, schemaRegion));
      TimeSeriesSchemaScanOperator timeSeriesMetaScanOperator =
          new TimeSeriesSchemaScanOperator(
              planNodeId,
              fragmentInstanceContext.getOperatorContexts().get(0),
              10,
              0,
              partialPath,
              null,
              null,
              false,
              false,
              false,
              Collections.emptyMap());
      while (timeSeriesMetaScanOperator.hasNext()) {
        TsBlock tsBlock = timeSeriesMetaScanOperator.next();
        assertEquals(
            ColumnHeaderConstant.showTimeSeriesColumnHeaders.size(), tsBlock.getValueColumnCount());
        assertTrue(tsBlock.getColumn(0) instanceof BinaryColumn);
        assertEquals(10, tsBlock.getPositionCount());
        for (int i = 0; i < tsBlock.getPositionCount(); i++) {
          Assert.assertEquals(0, tsBlock.getTimeByIndex(i));
          for (int j = 0; j < ColumnHeaderConstant.showTimeSeriesColumnHeaders.size(); j++) {
            Binary binary =
                tsBlock.getColumn(j).isNull(i) ? null : tsBlock.getColumn(j).getBinary(i);
            String value = binary == null ? "null" : binary.toString();
            switch (j) {
              case 0:
                Assert.assertTrue(value.startsWith(META_SCAN_OPERATOR_TEST_SG + ".device0"));
                break;
              case 1:
                assertEquals("null", value);
                break;
              case 2:
                assertEquals(META_SCAN_OPERATOR_TEST_SG, value);
                break;
              case 3:
                assertEquals(TSDataType.INT32.toString(), value);
                break;
              case 4:
                assertEquals(TSEncoding.PLAIN.toString(), value);
                break;
              case 5:
                assertEquals(CompressionType.UNCOMPRESSED.toString(), value);
                break;
              case 6:
              case 7:
                assertEquals("null", value);
              default:
                break;
            }
          }
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
