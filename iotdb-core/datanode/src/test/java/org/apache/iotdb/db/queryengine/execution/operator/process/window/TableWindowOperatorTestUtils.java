/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.queryengine.execution.operator.process.window;

import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.block.column.RunLengthEncodedColumn;

import java.util.Arrays;
import java.util.Collections;

import static org.apache.iotdb.db.queryengine.execution.operator.source.relational.TableScanOperator.TIME_COLUMN_TEMPLATE;

public class TableWindowOperatorTestUtils {
  public static TsBlock createIntsTsBlockWithoutNulls(int[] inputs) {
    TsBlockBuilder tsBlockBuilder = new TsBlockBuilder(Collections.singletonList(TSDataType.INT32));
    ColumnBuilder[] columnBuilders = tsBlockBuilder.getValueColumnBuilders();
    for (int input : inputs) {
      columnBuilders[0].writeInt(input);
      tsBlockBuilder.declarePosition();
    }

    return tsBlockBuilder.build(
        new RunLengthEncodedColumn(TIME_COLUMN_TEMPLATE, tsBlockBuilder.getPositionCount()));
  }

  public static TsBlock createIntsTsBlockWithoutNulls(int[] inputs, int offset) {
    TsBlockBuilder tsBlockBuilder =
        new TsBlockBuilder(Arrays.asList(TSDataType.INT32, TSDataType.INT32));
    ColumnBuilder[] columnBuilders = tsBlockBuilder.getValueColumnBuilders();
    for (int input : inputs) {
      columnBuilders[0].writeInt(input);
      columnBuilders[1].writeInt(offset);
      tsBlockBuilder.declarePosition();
    }

    return tsBlockBuilder.build(
        new RunLengthEncodedColumn(TIME_COLUMN_TEMPLATE, tsBlockBuilder.getPositionCount()));
  }

  public static TsBlock createIntsTsBlockWithoutNulls(
      int[] inputs, int startOffset, int endOffset) {
    TsBlockBuilder tsBlockBuilder =
        new TsBlockBuilder(Arrays.asList(TSDataType.INT32, TSDataType.INT32, TSDataType.INT32));
    ColumnBuilder[] columnBuilders = tsBlockBuilder.getValueColumnBuilders();
    for (int input : inputs) {
      columnBuilders[0].writeInt(input);
      columnBuilders[1].writeInt(startOffset);
      columnBuilders[2].writeInt(endOffset);
      tsBlockBuilder.declarePosition();
    }

    return tsBlockBuilder.build(
        new RunLengthEncodedColumn(TIME_COLUMN_TEMPLATE, tsBlockBuilder.getPositionCount()));
  }

  public static TsBlock createIntsTsBlockWithNulls(int[] inputs) {
    TsBlockBuilder tsBlockBuilder = new TsBlockBuilder(Collections.singletonList(TSDataType.INT32));
    ColumnBuilder[] columnBuilders = tsBlockBuilder.getValueColumnBuilders();
    for (int input : inputs) {
      if (input >= 0) {
        columnBuilders[0].writeInt(input);
      } else {
        // Mimic null value
        columnBuilders[0].appendNull();
      }
      tsBlockBuilder.declarePosition();
    }

    return tsBlockBuilder.build(
        new RunLengthEncodedColumn(TIME_COLUMN_TEMPLATE, tsBlockBuilder.getPositionCount()));
  }

  public static TsBlock createIntsTsBlockWithNulls(int[] inputs, int offset) {
    TsBlockBuilder tsBlockBuilder =
        new TsBlockBuilder(Arrays.asList(TSDataType.INT32, TSDataType.INT32));
    ColumnBuilder[] columnBuilders = tsBlockBuilder.getValueColumnBuilders();
    for (int input : inputs) {
      if (input >= 0) {
        columnBuilders[0].writeInt(input);
      } else {
        // Mimic null value
        columnBuilders[0].appendNull();
      }
      columnBuilders[1].writeInt(offset);
      tsBlockBuilder.declarePosition();
    }

    return tsBlockBuilder.build(
        new RunLengthEncodedColumn(TIME_COLUMN_TEMPLATE, tsBlockBuilder.getPositionCount()));
  }

  public static TsBlock createIntsTsBlockWithNulls(int[] inputs, int startOffset, int endOffset) {
    TsBlockBuilder tsBlockBuilder =
        new TsBlockBuilder(Arrays.asList(TSDataType.INT32, TSDataType.INT32, TSDataType.INT32));
    ColumnBuilder[] columnBuilders = tsBlockBuilder.getValueColumnBuilders();
    for (int input : inputs) {
      if (input >= 0) {
        columnBuilders[0].writeInt(input);
      } else {
        // Mimic null value
        columnBuilders[0].appendNull();
      }
      columnBuilders[1].writeInt(startOffset);
      columnBuilders[2].writeInt(endOffset);
      tsBlockBuilder.declarePosition();
    }

    return tsBlockBuilder.build(
        new RunLengthEncodedColumn(TIME_COLUMN_TEMPLATE, tsBlockBuilder.getPositionCount()));
  }
}
