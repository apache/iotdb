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

package org.apache.iotdb.db.queryengine.execution.operator.process;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.parameter.InputLocation;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertTabletStatement;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.column.IntColumn;
import org.apache.tsfile.read.common.block.column.LongColumn;
import org.apache.tsfile.read.common.block.column.NullColumn;
import org.apache.tsfile.read.common.block.column.RunLengthEncodedColumn;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.DateUtils;
import org.junit.Test;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TableInsertTabletStatementGeneratorTest {

  @Test
  public void testProcessTsBlockConvertsCompatibleColumns() throws Exception {
    List<TSDataType> sourceTypes =
        Arrays.asList(
            TSDataType.INT32,
            TSDataType.TIMESTAMP,
            TSDataType.INT32,
            TSDataType.INT32,
            TSDataType.DATE);
    List<TSDataType> targetTypes =
        Arrays.asList(
            TSDataType.INT64,
            TSDataType.INT64,
            TSDataType.DOUBLE,
            TSDataType.TEXT,
            TSDataType.STRING);
    TableInsertTabletStatementGenerator generator = createGenerator(sourceTypes, targetTypes);
    int date = DateUtils.parseDateExpressionToInt(LocalDate.of(2024, 1, 1));

    assertEquals(
        1,
        generator.processTsBlock(
            createTsBlock(
                new IntColumn(1, Optional.empty(), new int[] {123}),
                new LongColumn(1, Optional.empty(), new long[] {456L}),
                new IntColumn(1, Optional.empty(), new int[] {789}),
                new IntColumn(1, Optional.empty(), new int[] {123}),
                new IntColumn(1, Optional.empty(), new int[] {date}, TSDataType.DATE)),
            0));

    InsertTabletStatement statement = generator.constructInsertTabletStatement();
    assertEquals(1, statement.getRowCount());
    assertArrayEquals(new long[] {1L}, statement.getTimes());
    assertArrayEquals(new long[] {123L}, (long[]) statement.getColumns()[0]);
    assertArrayEquals(new long[] {456L}, (long[]) statement.getColumns()[1]);
    assertArrayEquals(new double[] {789.0}, (double[]) statement.getColumns()[2], 0.0);
    assertEquals("123", ((Binary[]) statement.getColumns()[3])[0].toString());
    assertEquals("2024-01-01", ((Binary[]) statement.getColumns()[4])[0].toString());
    assertFalse(statement.getBitMaps()[3].isMarked(0));
    assertFalse(statement.getBitMaps()[4].isMarked(0));
  }

  @Test
  public void testAllNullColumnsRemainNullWithoutConversion() throws Exception {
    List<TSDataType> sourceTypes = Arrays.asList(TSDataType.DATE, TSDataType.DATE);
    List<TSDataType> targetTypes = Arrays.asList(TSDataType.TEXT, TSDataType.STRING);
    TableInsertTabletStatementGenerator generator = createGenerator(sourceTypes, targetTypes);

    assertEquals(
        1,
        generator.processTsBlock(
            createTsBlock(
                new NullColumn(1),
                new RunLengthEncodedColumn(
                    new IntColumn(1, Optional.of(new boolean[] {true}), new int[] {0}), 1)),
            0));

    InsertTabletStatement statement = generator.constructInsertTabletStatement();
    assertEquals(1, statement.getRowCount());
    assertTrue(statement.getBitMaps()[0].isMarked(0));
    assertTrue(statement.getBitMaps()[1].isMarked(0));
  }

  private static TableInsertTabletStatementGenerator createGenerator(
      List<TSDataType> sourceTypes, List<TSDataType> targetTypes) throws Exception {
    Map<String, InputLocation> locations = new LinkedHashMap<>();
    Map<String, TSDataType> targets = new LinkedHashMap<>();
    List<TSDataType> inputTypes = new ArrayList<>();
    List<TsTableColumnCategory> categories = new ArrayList<>();
    locations.put("time", new InputLocation(0, 0));
    for (int i = 0; i < sourceTypes.size(); i++) {
      String measurement = "s" + i;
      locations.put(measurement, new InputLocation(0, i + 1));
      targets.put(measurement, targetTypes.get(i));
      inputTypes.add(targetTypes.get(i));
      categories.add(TsTableColumnCategory.FIELD);
    }
    return new TableInsertTabletStatementGenerator(
        "testdb",
        new PartialPath("target_table"),
        locations,
        targets,
        inputTypes,
        categories,
        true,
        10,
        "time");
  }

  private static TsBlock createTsBlock(Column... sourceColumns) {
    LongColumn timeColumn = new LongColumn(1, Optional.empty(), new long[] {1L});
    Column[] valueColumns = new Column[sourceColumns.length + 1];
    valueColumns[0] = timeColumn;
    System.arraycopy(sourceColumns, 0, valueColumns, 1, sourceColumns.length);
    return new TsBlock(timeColumn, valueColumns);
  }
}
