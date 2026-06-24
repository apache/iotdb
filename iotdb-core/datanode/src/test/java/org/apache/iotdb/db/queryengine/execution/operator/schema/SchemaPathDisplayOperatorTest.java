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

import org.apache.iotdb.calc.execution.operator.Operator;
import org.apache.iotdb.common.rpc.thrift.TSchemaNode;
import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.commons.schema.node.MNodeType;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.utils.Binary;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class SchemaPathDisplayOperatorTest {

  @Test
  public void testChildPathsMergeDisplaysReservedNodesAndDeduplicatesRawPath() throws Exception {
    TsBlockBuilder childBuilder =
        new TsBlockBuilder(Arrays.asList(TSDataType.TEXT, TSDataType.TEXT));
    childBuilder.getTimeColumnBuilder().writeLong(0L);
    childBuilder
        .getColumnBuilder(0)
        .writeBinary(new Binary("root.sg.root", TSFileConfig.STRING_CHARSET));
    childBuilder
        .getColumnBuilder(1)
        .writeBinary(
            new Binary(
                String.valueOf(MNodeType.DEVICE.getNodeType()), TSFileConfig.STRING_CHARSET));
    childBuilder.declarePosition();

    Operator child = new FixedTsBlockOperator(childBuilder.build());
    NodeManageMemoryMergeOperator operator =
        new NodeManageMemoryMergeOperator(
            Mockito.mock(OperatorContext.class),
            new HashSet<>(
                Collections.singletonList(
                    new TSchemaNode("root.sg.root", MNodeType.DEVICE.getNodeType()))),
            child);

    TsBlock first = operator.next();
    assertEquals(1, first.getPositionCount());
    assertEquals("root.sg.`root`", first.getColumn(0).getBinary(0).toString());
    assertEquals("DEVICE", first.getColumn(1).getBinary(0).toString());

    TsBlock second = operator.next();
    assertEquals(0, second.getPositionCount());
    assertFalse(operator.hasNext());
  }

  @Test
  public void testChildNodesConvertDisplaysReservedTailNode() throws Exception {
    TsBlockBuilder childBuilder =
        new TsBlockBuilder(Arrays.asList(TSDataType.TEXT, TSDataType.TEXT));
    childBuilder.getTimeColumnBuilder().writeLong(0L);
    childBuilder
        .getColumnBuilder(0)
        .writeBinary(new Binary("root.sg.root", TSFileConfig.STRING_CHARSET));
    childBuilder
        .getColumnBuilder(1)
        .writeBinary(
            new Binary(
                String.valueOf(MNodeType.DEVICE.getNodeType()), TSFileConfig.STRING_CHARSET));
    childBuilder.declarePosition();

    NodePathsConvertOperator operator =
        new NodePathsConvertOperator(
            Mockito.mock(OperatorContext.class), new FixedTsBlockOperator(childBuilder.build()));

    TsBlock result = operator.next();
    assertEquals(1, result.getPositionCount());
    assertEquals("`root`", result.getColumn(0).getBinary(0).toString());
  }

  @Test
  public void testOrderByHeatMatchesDisplayedTimeseriesPath() throws Exception {
    TsBlockBuilder showTimeSeriesBuilder =
        new TsBlockBuilder(
            ColumnHeaderConstant.showTimeSeriesColumnHeaders.stream()
                .map(header -> header.getColumnType())
                .toList());
    writeShowTimeSeriesRow(showTimeSeriesBuilder, "root.sg.a.s1");
    writeShowTimeSeriesRow(showTimeSeriesBuilder, "root.sg.`root`.s1");

    TsBlockBuilder lastQueryBuilder =
        new TsBlockBuilder(Arrays.asList(TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT));
    lastQueryBuilder.getTimeColumnBuilder().writeLong(100L);
    lastQueryBuilder
        .getColumnBuilder(0)
        .writeBinary(new Binary("root.sg.a.s1", TSFileConfig.STRING_CHARSET));
    lastQueryBuilder.getColumnBuilder(1).writeBinary(new Binary("1", TSFileConfig.STRING_CHARSET));
    lastQueryBuilder
        .getColumnBuilder(2)
        .writeBinary(new Binary(TSDataType.INT32.name(), TSFileConfig.STRING_CHARSET));
    lastQueryBuilder.declarePosition();
    lastQueryBuilder.getTimeColumnBuilder().writeLong(200L);
    lastQueryBuilder
        .getColumnBuilder(0)
        .writeBinary(new Binary("root.sg.`root`.s1", TSFileConfig.STRING_CHARSET));
    lastQueryBuilder.getColumnBuilder(1).writeBinary(new Binary("2", TSFileConfig.STRING_CHARSET));
    lastQueryBuilder
        .getColumnBuilder(2)
        .writeBinary(new Binary(TSDataType.INT32.name(), TSFileConfig.STRING_CHARSET));
    lastQueryBuilder.declarePosition();

    SchemaQueryOrderByHeatOperator operator =
        new SchemaQueryOrderByHeatOperator(
            Mockito.mock(OperatorContext.class),
            Arrays.asList(
                new FixedTsBlockOperator(showTimeSeriesBuilder.build()),
                new FixedTsBlockOperator(lastQueryBuilder.build())));

    TsBlock result = null;
    while (operator.hasNext()) {
      result = operator.next();
      if (result != null && !result.isEmpty()) {
        break;
      }
    }

    assertEquals(2, result.getPositionCount());
    assertEquals("root.sg.`root`.s1", result.getColumn(0).getBinary(0).toString());
    assertEquals("root.sg.a.s1", result.getColumn(0).getBinary(1).toString());
  }

  private static void writeShowTimeSeriesRow(TsBlockBuilder builder, String timeseries) {
    builder.getTimeColumnBuilder().writeLong(0L);
    builder.getColumnBuilder(0).writeBinary(new Binary(timeseries, TSFileConfig.STRING_CHARSET));
    builder.getColumnBuilder(1).appendNull();
    builder.getColumnBuilder(2).writeBinary(new Binary("root.sg", TSFileConfig.STRING_CHARSET));
    builder
        .getColumnBuilder(3)
        .writeBinary(new Binary(TSDataType.INT32.name(), TSFileConfig.STRING_CHARSET));
    builder.getColumnBuilder(4).writeBinary(new Binary("PLAIN", TSFileConfig.STRING_CHARSET));
    builder
        .getColumnBuilder(5)
        .writeBinary(new Binary("UNCOMPRESSED", TSFileConfig.STRING_CHARSET));
    builder.getColumnBuilder(6).appendNull();
    builder.getColumnBuilder(7).appendNull();
    builder.getColumnBuilder(8).appendNull();
    builder.getColumnBuilder(9).appendNull();
    builder.getColumnBuilder(10).appendNull();
    builder.declarePosition();
  }

  private static class FixedTsBlockOperator implements Operator {
    private final TsBlock block;
    private boolean consumed;

    private FixedTsBlockOperator(TsBlock block) {
      this.block = block;
    }

    @Override
    public OperatorContext getOperatorContext() {
      return Mockito.mock(OperatorContext.class);
    }

    @Override
    public TsBlock next() {
      if (consumed) {
        return null;
      }
      consumed = true;
      return block;
    }

    @Override
    public boolean hasNext() {
      return !consumed;
    }

    @Override
    public void close() {}

    @Override
    public boolean isFinished() {
      return consumed;
    }

    @Override
    public long calculateMaxPeekMemory() {
      return 0;
    }

    @Override
    public long calculateMaxReturnSize() {
      return 0;
    }

    @Override
    public long calculateRetainedSizeAfterCallingNext() {
      return 0;
    }

    @Override
    public long ramBytesUsed() {
      return 0;
    }

    @Override
    public ListenableFuture<?> isBlocked() {
      return NOT_BLOCKED;
    }
  }
}
