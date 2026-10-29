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

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.parameter.InputLocation;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertTabletStatement;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.utils.Binary;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public class InsertTabletStatementGeneratorTest {

  @Test
  public void testOutOfOrderRowsAreSortedByTime() throws IllegalPathException {
    TreeInsertTabletStatementGenerator generator = createGenerator();

    TsBlockBuilder builder = new TsBlockBuilder(Collections.singletonList(TSDataType.INT32));
    append(builder, 100, 100);
    append(builder, 15, 15);
    generator.processTsBlock(builder.build(), 0);

    InsertTabletStatement statement = generator.constructInsertTabletStatement();

    Assert.assertArrayEquals(new long[] {15, 100}, statement.getTimes());
    Assert.assertArrayEquals(new int[] {15, 100}, (int[]) statement.getColumns()[0]);
  }

  @Test
  public void testNullValueMovesWithItsRow() throws IllegalPathException {
    TreeInsertTabletStatementGenerator generator = createGenerator();

    TsBlockBuilder builder = new TsBlockBuilder(Collections.singletonList(TSDataType.INT32));
    append(builder, 100, 100);
    builder.getTimeColumnBuilder().writeLong(15);
    builder.getColumnBuilder(0).appendNull();
    builder.declarePosition();
    generator.processTsBlock(builder.build(), 0);

    InsertTabletStatement statement = generator.constructInsertTabletStatement();

    Assert.assertArrayEquals(new long[] {15, 100}, statement.getTimes());
    Assert.assertArrayEquals(new int[] {0, 100}, (int[]) statement.getColumns()[0]);
    // row 0 (time 15) has no value, row 1 (time 100) does
    Assert.assertTrue(statement.getBitMaps()[0].isMarked(0));
    Assert.assertFalse(statement.getBitMaps()[0].isMarked(1));
  }

  @Test
  public void testSortedRowsAreKeptAsIs() throws IllegalPathException {
    TreeInsertTabletStatementGenerator generator = createGenerator();

    TsBlockBuilder builder = new TsBlockBuilder(Collections.singletonList(TSDataType.INT32));
    append(builder, 15, 15);
    append(builder, 100, 100);
    generator.processTsBlock(builder.build(), 0);

    InsertTabletStatement statement = generator.constructInsertTabletStatement();

    Assert.assertArrayEquals(new long[] {15, 100}, statement.getTimes());
    Assert.assertArrayEquals(new int[] {15, 100}, (int[]) statement.getColumns()[0]);
  }

  /**
   * A full batch ({@code rowCount == rowLimit}) takes the other branch of {@code
   * constructInsertTabletStatement}: the generator's own arrays are handed to the statement without
   * being copied first, so sorting has to keep the arrays and the statement consistent.
   */
  @Test
  public void testFullBatchOfMixedTypesIsSorted() throws IllegalPathException {
    String[] measurements = {"s1", "s2", "s3", "s4", "s5", "s6"};
    TSDataType[] dataTypes = {
      TSDataType.INT32,
      TSDataType.INT64,
      TSDataType.FLOAT,
      TSDataType.DOUBLE,
      TSDataType.BOOLEAN,
      TSDataType.TEXT
    };
    Map<String, InputLocation> inputLocations = new LinkedHashMap<>();
    for (int i = 0; i < measurements.length; i++) {
      inputLocations.put(measurements[i], new InputLocation(0, i));
    }
    Map<String, TSDataType> dataTypeMap = new LinkedHashMap<>();
    for (int i = 0; i < measurements.length; i++) {
      dataTypeMap.put(measurements[i], dataTypes[i]);
    }
    TreeInsertTabletStatementGenerator generator =
        new TreeInsertTabletStatementGenerator(
            new PartialPath("root.sg.d1"),
            inputLocations,
            dataTypeMap,
            Arrays.asList(dataTypes),
            false,
            3);

    TsBlockBuilder builder = new TsBlockBuilder(Arrays.asList(dataTypes));
    // row order: 30, 10, 20; the TEXT value of the row at time 10 is null
    appendMixedRow(builder, 30, false);
    appendMixedRow(builder, 10, true);
    appendMixedRow(builder, 20, false);
    generator.processTsBlock(builder.build(), 0);

    InsertTabletStatement statement = generator.constructInsertTabletStatement();

    Assert.assertEquals(3, statement.getRowCount());
    Assert.assertArrayEquals(new long[] {10, 20, 30}, statement.getTimes());
    Assert.assertArrayEquals(new int[] {10, 20, 30}, (int[]) statement.getColumns()[0]);
    Assert.assertArrayEquals(new long[] {10L, 20L, 30L}, (long[]) statement.getColumns()[1]);
    Assert.assertArrayEquals(
        new float[] {10.0F, 20.0F, 30.0F}, (float[]) statement.getColumns()[2], 0.0F);
    Assert.assertArrayEquals(
        new double[] {10.0, 20.0, 30.0}, (double[]) statement.getColumns()[3], 0.0);
    Assert.assertArrayEquals(
        new boolean[] {false, true, false}, (boolean[]) statement.getColumns()[4]);
    Binary[] textColumn = (Binary[]) statement.getColumns()[5];
    Assert.assertTrue(statement.getBitMaps()[5].isMarked(0));
    Assert.assertFalse(statement.getBitMaps()[5].isMarked(1));
    Assert.assertFalse(statement.getBitMaps()[5].isMarked(2));
    Assert.assertEquals("v20", textColumn[1].toString());
    Assert.assertEquals("v30", textColumn[2].toString());
  }

  private void appendMixedRow(TsBlockBuilder builder, long time, boolean textIsNull) {
    builder.getTimeColumnBuilder().writeLong(time);
    builder.getColumnBuilder(0).writeInt((int) time);
    builder.getColumnBuilder(1).writeLong(time);
    builder.getColumnBuilder(2).writeFloat(time);
    builder.getColumnBuilder(3).writeDouble(time);
    builder.getColumnBuilder(4).writeBoolean(time % 20 != 10);
    if (textIsNull) {
      builder.getColumnBuilder(5).appendNull();
    } else {
      builder
          .getColumnBuilder(5)
          .writeBinary(new Binary(("v" + time).getBytes(StandardCharsets.UTF_8)));
    }
    builder.declarePosition();
  }

  /**
   * The table-model generator writes the tag value of every row (not only of the first one), so a
   * tag, including a null tag carried by the bitmap, has to move together with the row it belongs
   * to.
   */
  @Test
  public void testTagColumnMovesWithItsRow() throws IllegalPathException {
    Map<String, InputLocation> inputLocations = new LinkedHashMap<>();
    inputLocations.put("time", new InputLocation(0, 0));
    inputLocations.put("id1", new InputLocation(0, 1));
    inputLocations.put("m1", new InputLocation(0, 2));
    Map<String, TSDataType> dataTypeMap = new LinkedHashMap<>();
    dataTypeMap.put("id1", TSDataType.STRING);
    dataTypeMap.put("m1", TSDataType.DOUBLE);
    TableInsertTabletStatementGenerator generator =
        new TableInsertTabletStatementGenerator(
            "root.sg",
            new PartialPath(new String[] {"table1"}),
            inputLocations,
            dataTypeMap,
            Arrays.asList(TSDataType.STRING, TSDataType.DOUBLE),
            Arrays.asList(TsTableColumnCategory.TAG, TsTableColumnCategory.FIELD),
            true,
            10,
            "time");

    TsBlockBuilder builder =
        new TsBlockBuilder(Arrays.asList(TSDataType.INT64, TSDataType.STRING, TSDataType.DOUBLE));
    appendTableRow(builder, 100, "b", 1.0);
    appendTableRow(builder, 15, "a", 2.0);
    appendTableRow(builder, 50, null, 3.0);
    generator.processTsBlock(builder.build(), 0);

    InsertTabletStatement statement = generator.constructInsertTabletStatement();

    Assert.assertEquals(2, statement.getColumns().length);
    Assert.assertArrayEquals(new long[] {15, 50, 100}, statement.getTimes());
    Binary[] tagColumn = (Binary[]) statement.getColumns()[0];
    Assert.assertEquals("a", tagColumn[0].toString());
    Assert.assertTrue(statement.getBitMaps()[0].isMarked(1));
    Assert.assertEquals("b", tagColumn[2].toString());
    Assert.assertArrayEquals(
        new double[] {2.0, 3.0, 1.0}, (double[]) statement.getColumns()[1], 0.0);
  }

  private void appendTableRow(TsBlockBuilder builder, long time, String tag, double value) {
    // the time column of the target table is carried as value column 0 of the input TsBlock (the
    // generator reads it through getValueColumns()[timeColumnIndex]), the block itself also needs
    // its own time column
    builder.getTimeColumnBuilder().writeLong(time);
    builder.getColumnBuilder(0).writeLong(time);
    if (tag == null) {
      builder.getColumnBuilder(1).appendNull();
    } else {
      builder.getColumnBuilder(1).writeBinary(new Binary(tag.getBytes(StandardCharsets.UTF_8)));
    }
    builder.getColumnBuilder(2).writeDouble(value);
    builder.declarePosition();
  }

  /**
   * The generator is reused through {@link InsertTabletStatementGenerator#reset()} after a write.
   */
  @Test
  public void testGeneratorIsReusableAfterReset() throws IllegalPathException {
    TreeInsertTabletStatementGenerator generator = createGenerator();

    TsBlockBuilder first = new TsBlockBuilder(Collections.singletonList(TSDataType.INT32));
    append(first, 100, 100);
    append(first, 15, 15);
    generator.processTsBlock(first.build(), 0);
    InsertTabletStatement firstStatement = generator.constructInsertTabletStatement();
    Assert.assertArrayEquals(new long[] {15, 100}, firstStatement.getTimes());

    generator.reset();
    TsBlockBuilder second = new TsBlockBuilder(Collections.singletonList(TSDataType.INT32));
    append(second, 200, 200);
    append(second, 150, 150);
    generator.processTsBlock(second.build(), 0);
    InsertTabletStatement secondStatement = generator.constructInsertTabletStatement();

    // the first statement is not affected by the reset, and the second batch is sorted as well
    Assert.assertArrayEquals(new long[] {15, 100}, firstStatement.getTimes());
    Assert.assertArrayEquals(new long[] {150, 200}, secondStatement.getTimes());
    Assert.assertArrayEquals(new int[] {150, 200}, (int[]) secondStatement.getColumns()[0]);
  }

  private TreeInsertTabletStatementGenerator createGenerator() throws IllegalPathException {
    return new TreeInsertTabletStatementGenerator(
        new PartialPath("root.sg.d1"),
        Collections.singletonMap("s1", new InputLocation(0, 0)),
        Collections.singletonMap("s1", TSDataType.INT32),
        Collections.singletonList(TSDataType.INT32),
        false,
        10);
  }

  private void append(TsBlockBuilder builder, long time, int value) {
    builder.getTimeColumnBuilder().writeLong(time);
    builder.getColumnBuilder(0).writeInt(value);
    builder.declarePosition();
  }
}
