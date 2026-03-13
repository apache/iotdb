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

import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.AbstractTableScanOperator.AbstractTableScanOperatorParameter;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.SeriesScanOptions;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.DeviceEntry;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;

import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.type.TypeFactory;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class TableScanOperatorTest {

  @Test
  public void testCalculateMaxPeekMemory() {
    OperatorContext operatorContext = Mockito.mock(OperatorContext.class);

    PlanNodeId sourceId = new PlanNodeId("test");

    List<ColumnSchema> columnSchemas =
        Arrays.asList(
            new ColumnSchema(
                "time", TypeFactory.getType(TSDataType.INT64), false, TsTableColumnCategory.TIME),
            new ColumnSchema(
                "tag1", TypeFactory.getType(TSDataType.STRING), false, TsTableColumnCategory.TAG),
            new ColumnSchema(
                "attr1",
                TypeFactory.getType(TSDataType.STRING),
                false,
                TsTableColumnCategory.ATTRIBUTE),
            new ColumnSchema(
                "field1",
                TypeFactory.getType(TSDataType.BOOLEAN),
                false,
                TsTableColumnCategory.FIELD),
            new ColumnSchema(
                "field2",
                TypeFactory.getType(TSDataType.INT32),
                false,
                TsTableColumnCategory.FIELD));

    int[] columnsIndexArray = new int[] {0, 1, 2, 3, 4};
    List<DeviceEntry> deviceEntries = Collections.emptyList();
    Ordering scanOrder = Ordering.ASC;
    SeriesScanOptions seriesScanOptions = Mockito.mock(SeriesScanOptions.class);
    List<String> measurementColumnNames = Arrays.asList("field1", "field2");
    List<IMeasurementSchema> measurementSchemas = new ArrayList<>();

    Set<String> allSensors = new HashSet<>(measurementColumnNames);
    allSensors.add("");
    AbstractTableScanOperatorParameter parameter =
        new AbstractTableScanOperatorParameter(
            allSensors,
            operatorContext,
            sourceId,
            columnSchemas,
            columnsIndexArray,
            deviceEntries,
            scanOrder,
            seriesScanOptions,
            measurementColumnNames,
            measurementSchemas,
            1000);

    TableScanOperator operator = new TableScanOperator(parameter);

    long maxReturnSize =
        Math.min(
            TSFileDescriptor.getInstance().getConfig().getMaxTsBlockSizeInBytes(),
            (1L + 2) * TSFileDescriptor.getInstance().getConfig().getPageSizeInByte());
    long expectedMaxPeekMemory =
        Math.max(
            maxReturnSize,
            (1L + 2) * TSFileDescriptor.getInstance().getConfig().getPageSizeInByte());

    assertEquals(expectedMaxPeekMemory, operator.calculateMaxPeekMemory());
  }
}
