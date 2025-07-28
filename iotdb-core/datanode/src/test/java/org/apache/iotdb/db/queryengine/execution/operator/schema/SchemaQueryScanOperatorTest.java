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
package org.apache.iotdb.db.queryengine.execution.operator.schema;

import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.SchemaConstant;
import org.apache.iotdb.commons.schema.column.ColumnHeader;
import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.common.PlanFragmentId;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.execution.driver.DriverContext;
import org.apache.iotdb.db.queryengine.execution.driver.SchemaDriverContext;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceStateMachine;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.execution.operator.schema.source.ISchemaSource;
import org.apache.iotdb.db.queryengine.execution.operator.schema.source.SchemaSourceFactory;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.schemaengine.schemaregion.ISchemaRegion;
import org.apache.iotdb.db.schemaengine.schemaregion.read.resp.info.IDeviceSchemaInfo;
import org.apache.iotdb.db.schemaengine.schemaregion.read.resp.info.ITimeSeriesSchemaInfo;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.column.BinaryColumn;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;

import static org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext.createFragmentInstanceContext;
import static org.apache.iotdb.db.queryengine.execution.operator.schema.SchemaOperatorTestUtil.EXCEPTION_MESSAGE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class SchemaQueryScanOperatorTest {
  private static final String META_SCAN_OPERATOR_TEST_SG = "root.MetaScanOperatorTest";

  @Test
  public void testDeviceSchemaScan() throws Exception {
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
      DriverContext driverContext = new DriverContext(fragmentInstanceContext, 0);
      PlanNodeId planNodeId = queryId.genPlanNodeId();
      OperatorContext operatorContext =
          driverContext.addOperatorContext(
              1, planNodeId, SchemaQueryScanOperator.class.getSimpleName());
      PartialPath partialPath = new PartialPath(META_SCAN_OPERATOR_TEST_SG + ".device0");
      ISchemaRegion schemaRegion = Mockito.mock(ISchemaRegion.class);
      Mockito.when(schemaRegion.getDatabaseFullPath()).thenReturn(META_SCAN_OPERATOR_TEST_SG);
      IDeviceSchemaInfo deviceSchemaInfo = Mockito.mock(IDeviceSchemaInfo.class);
      Mockito.when(deviceSchemaInfo.getFullPath())
          .thenReturn(META_SCAN_OPERATOR_TEST_SG + ".device0");
      Mockito.when(deviceSchemaInfo.isAligned()).thenReturn(false);
      Mockito.when(deviceSchemaInfo.getTemplateId()).thenReturn(-1);
      Mockito.when(deviceSchemaInfo.getPartialPath()).thenReturn(partialPath);
      operatorContext.setDriverContext(
          new SchemaDriverContext(fragmentInstanceContext, schemaRegion, 0));
      ISchemaSource<IDeviceSchemaInfo> deviceSchemaSource =
          SchemaSourceFactory.getDeviceSchemaSource(
              partialPath, false, 10, 0, true, null, SchemaConstant.ALL_MATCH_SCOPE);
      SchemaOperatorTestUtil.mockGetSchemaReader(
          deviceSchemaSource,
          Collections.singletonList(deviceSchemaInfo).iterator(),
          schemaRegion,
          true);
      //
      List<ColumnHeader> columns = deviceSchemaSource.getInfoQueryColumnHeaders();

      SchemaQueryScanOperator<IDeviceSchemaInfo> devicesSchemaScanOperator =
          new SchemaQueryScanOperator<>(
              planNodeId, driverContext.getOperatorContexts().get(0), deviceSchemaSource);
      //
      while (devicesSchemaScanOperator.hasNext()) {
        TsBlock tsBlock = devicesSchemaScanOperator.next();
        assertEquals(5, tsBlock.getValueColumnCount());
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
              case 3:
                assertNull(tsBlock.getColumn(j).getBinary(i));
                break;
              case 4:
                assertEquals(
                    tsBlock.getColumn(j).getBinary(i).toString(), IoTDBConstant.TTL_INFINITE);
                break;
              default:
                break;
            }
          }
        }
      }
      // Assert failure if exception occurs
      SchemaOperatorTestUtil.mockGetSchemaReader(
          deviceSchemaSource,
          Collections.singletonList(deviceSchemaInfo).iterator(),
          schemaRegion,
          false);
      try {
        SchemaQueryScanOperator<IDeviceSchemaInfo> devicesSchemaScanOperatorFailure =
            new SchemaQueryScanOperator<>(
                planNodeId, driverContext.getOperatorContexts().get(0), deviceSchemaSource);
        while (devicesSchemaScanOperatorFailure.hasNext()) {
          devicesSchemaScanOperatorFailure.next();
        }
        Assert.fail();
      } catch (RuntimeException e) {
        Assert.assertTrue(e.getMessage().contains(EXCEPTION_MESSAGE));
      }
    } catch (MetadataException e) {
      e.printStackTrace();
      fail();
    } finally {
      instanceNotificationExecutor.shutdown();
    }
  }

  @Test
  public void testTimeSeriesSchemaScan() throws Exception {
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
      DriverContext driverContext = new DriverContext(fragmentInstanceContext, 0);
      PlanNodeId planNodeId = queryId.genPlanNodeId();
      OperatorContext operatorContext =
          driverContext.addOperatorContext(
              1, planNodeId, SchemaQueryScanOperator.class.getSimpleName());
      PartialPath partialPath = new PartialPath(META_SCAN_OPERATOR_TEST_SG + ".device0.*");

      List<ITimeSeriesSchemaInfo> showTimeSeriesResults = new ArrayList<>();
      for (int i = 0; i < 10; i++) {
        ITimeSeriesSchemaInfo timeSeriesSchemaInfo = Mockito.mock(ITimeSeriesSchemaInfo.class);
        Mockito.when(timeSeriesSchemaInfo.getFullPath())
            .thenReturn(META_SCAN_OPERATOR_TEST_SG + ".device0." + "s" + i);
        Mockito.when(timeSeriesSchemaInfo.getAlias()).thenReturn(null);
        Mockito.when(timeSeriesSchemaInfo.getSchema())
            .thenReturn(
                new MeasurementSchema(
                    "s" + i, TSDataType.INT32, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED));
        Mockito.when(timeSeriesSchemaInfo.getTags()).thenReturn(null);
        Mockito.when(timeSeriesSchemaInfo.getAttributes()).thenReturn(null);
        showTimeSeriesResults.add(timeSeriesSchemaInfo);
      }

      ISchemaRegion schemaRegion = Mockito.mock(ISchemaRegion.class);
      Mockito.when(schemaRegion.getDatabaseFullPath()).thenReturn(META_SCAN_OPERATOR_TEST_SG);

      operatorContext.setDriverContext(
          new SchemaDriverContext(fragmentInstanceContext, schemaRegion, 0));
      ISchemaSource<ITimeSeriesSchemaInfo> timeSeriesSchemaSource =
          SchemaSourceFactory.getTimeSeriesSchemaScanSource(
              partialPath,
              false,
              10,
              0,
              null,
              Collections.emptyMap(),
              SchemaConstant.ALL_MATCH_SCOPE);
      SchemaOperatorTestUtil.mockGetSchemaReader(
          timeSeriesSchemaSource, showTimeSeriesResults.iterator(), schemaRegion, true);

      SchemaQueryScanOperator<ITimeSeriesSchemaInfo> timeSeriesMetaScanOperator =
          new SchemaQueryScanOperator<>(
              planNodeId, driverContext.getOperatorContexts().get(0), timeSeriesSchemaSource);
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
      // Assert failure if exception occurs
      SchemaOperatorTestUtil.mockGetSchemaReader(
          timeSeriesSchemaSource, showTimeSeriesResults.iterator(), schemaRegion, false);
      try {
        SchemaQueryScanOperator<ITimeSeriesSchemaInfo> timeSeriesMetaScanOperatorFailure =
            new SchemaQueryScanOperator<>(
                planNodeId, driverContext.getOperatorContexts().get(0), timeSeriesSchemaSource);
        while (timeSeriesMetaScanOperatorFailure.hasNext()) {
          timeSeriesMetaScanOperatorFailure.next();
        }
        Assert.fail();
      } catch (RuntimeException e) {
        Assert.assertTrue(e.getMessage().contains(EXCEPTION_MESSAGE));
      }
    } catch (MetadataException e) {
      e.printStackTrace();
      fail();
    } finally {
      instanceNotificationExecutor.shutdown();
    }
  }
}
