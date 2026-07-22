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

package org.apache.iotdb.db.queryengine.execution.operator.source.relational;

import org.apache.iotdb.calc.execution.filter.TopKRuntimeFilter;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.commons.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.AbstractTableScanOperator.AbstractTableScanOperatorParameter;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.SeriesScanOptions;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.DeviceEntry;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;
import org.apache.iotdb.db.storageengine.dataregion.read.QueryDataSource;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.read.common.type.TypeFactory;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ExternalTsFileTableScanOperatorRuntimeFilterTest {

  @Test
  public void shouldNotStopWhenNonLastDeviceExhausted() throws Exception {
    ExternalTsFileTableScanOperator operator =
        new TestExternalTsFileTableScanOperator(createParameter());
    QueryDataSource firstDeviceDataSource = Mockito.mock(QueryDataSource.class);
    Mockito.when(firstDeviceDataSource.hasValidResource()).thenReturn(false);
    operator.queryDataSource = firstDeviceDataSource;
    operator.currentDeviceIndex = 0;

    Assert.assertFalse(operator.shouldStopScanByRuntimeFilter());
  }

  @Test
  public void shouldUseCurrentDeviceQueryDataSourceOnLastDevice() throws Exception {
    ExternalTsFileTableScanOperator operator =
        new TestExternalTsFileTableScanOperator(createParameter());
    QueryDataSource firstDeviceDataSource = Mockito.mock(QueryDataSource.class);
    Mockito.when(firstDeviceDataSource.hasValidResource()).thenReturn(false);
    QueryDataSource lastDeviceDataSource = Mockito.mock(QueryDataSource.class);
    Mockito.when(lastDeviceDataSource.hasValidResource()).thenReturn(true);

    operator.currentDeviceIndex = 1;
    operator.queryDataSource = lastDeviceDataSource;

    Assert.assertFalse(operator.shouldStopScanByRuntimeFilter());

    Mockito.when(lastDeviceDataSource.hasValidResource()).thenReturn(false);
    Assert.assertTrue(operator.shouldStopScanByRuntimeFilter());
  }

  private static AbstractTableScanOperatorParameter createParameter() {
    OperatorContext operatorContext = Mockito.mock(OperatorContext.class);
    PlanNodeId sourceId = new PlanNodeId("external-scan");
    List<ColumnSchema> columnSchemas =
        Collections.singletonList(
            new ColumnSchema(
                "time", TypeFactory.getType(TSDataType.INT64), false, TsTableColumnCategory.TIME));
    int[] columnsIndexArray = new int[] {0};
    List<DeviceEntry> deviceEntries = Arrays.asList(mockDeviceEntry(), mockDeviceEntry());
    SeriesScanOptions seriesScanOptions =
        new SeriesScanOptions.Builder().withTopKRuntimeFilter(new TopKRuntimeFilter(false)).build();
    List<String> measurementColumnNames = Collections.emptyList();
    List<IMeasurementSchema> measurementSchemas = Collections.emptyList();
    Set<String> allSensors = new HashSet<>();
    allSensors.add("");
    return new AbstractTableScanOperatorParameter(
        allSensors,
        operatorContext,
        sourceId,
        columnSchemas,
        columnsIndexArray,
        deviceEntries,
        Ordering.ASC,
        seriesScanOptions,
        measurementColumnNames,
        measurementSchemas,
        1000);
  }

  private static DeviceEntry mockDeviceEntry() {
    DeviceEntry deviceEntry = Mockito.mock(DeviceEntry.class);
    IDeviceID deviceID = Mockito.mock(IDeviceID.class);
    Mockito.when(deviceEntry.getDeviceID()).thenReturn(deviceID);
    return deviceEntry;
  }

  private static final class TestExternalTsFileTableScanOperator
      extends ExternalTsFileTableScanOperator {

    private TestExternalTsFileTableScanOperator(AbstractTableScanOperatorParameter parameter) {
      super(parameter, 0);
    }

    @Override
    protected void constructAlignedSeriesScanUtil() {
      // Skip TsFile path construction in unit test.
    }
  }
}
