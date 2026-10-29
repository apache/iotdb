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
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertTabletStatement;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;

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
