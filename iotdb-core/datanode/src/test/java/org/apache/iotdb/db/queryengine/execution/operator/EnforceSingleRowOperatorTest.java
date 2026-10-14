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
package org.apache.iotdb.db.queryengine.execution.operator;

import org.apache.iotdb.calc.execution.operator.CommonOperatorContext;
import org.apache.iotdb.calc.execution.operator.Operator;
import org.apache.iotdb.calc.execution.operator.process.EnforceSingleRowOperator;
import org.apache.iotdb.commons.exception.SemanticException;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class EnforceSingleRowOperatorTest {
  @Test
  public void testEmptyInputProducesOneTypedNullRow() throws Exception {
    List<TSDataType> types =
        Arrays.asList(
            TSDataType.INT32,
            TSDataType.INT64,
            TSDataType.FLOAT,
            TSDataType.DOUBLE,
            TSDataType.BOOLEAN,
            TSDataType.TEXT,
            TSDataType.STRING,
            TSDataType.TIMESTAMP,
            TSDataType.DATE,
            TSDataType.BLOB);
    Operator child = mock(Operator.class);
    when(child.isFinished()).thenReturn(true);
    EnforceSingleRowOperator operator =
        new EnforceSingleRowOperator(mock(CommonOperatorContext.class), child, types);
    assertFalse(operator.isFinished());
    assertTrue(operator.hasNext());
    TsBlock result = operator.next();
    assertEquals(1, result.getPositionCount());
    assertEquals(types.size(), result.getValueColumnCount());
    for (int i = 0; i < types.size(); i++) {
      assertTrue(result.getColumn(i).isNull(0));
    }
    assertTrue(operator.isFinished());
    assertFalse(operator.hasNext());
    assertTrue(operator.calculateMaxReturnSize() >= result.getRetainedSizeInBytes());
    assertTrue(operator.calculateMaxPeekMemory() >= result.getRetainedSizeInBytes());
    verify(child, never()).nextWithTimer();
    operator.close();
    verify(child).close();
  }

  @Test
  public void testEmptyBlocksBeforeEndOfInput() throws Exception {
    Operator child = mock(Operator.class);
    when(child.isFinished()).thenReturn(false);
    when(child.nextWithTimer()).thenReturn(null, block());
    EnforceSingleRowOperator operator = operator(child);
    assertNull(operator.next());
    assertTrue(operator.next().isEmpty());
    assertFalse(operator.isFinished());
    when(child.isFinished()).thenReturn(true);
    TsBlock result = operator.next();
    assertEquals(1, result.getPositionCount());
    assertTrue(result.getColumn(0).isNull(0));
    assertTrue(operator.isFinished());
  }

  @Test
  public void testSingleRowAndTrailingEmptyBlocks() throws Exception {
    Operator child = mock(Operator.class);
    TsBlock row = block(42);
    when(child.nextWithTimer()).thenReturn(row, block(), null);
    EnforceSingleRowOperator operator = operator(child);
    assertSame(row, operator.next());
    assertFalse(operator.isFinished());
    assertTrue(operator.next().isEmpty());
    assertNull(operator.next());
    when(child.isFinished()).thenReturn(true);
    assertTrue(operator.isFinished());
    assertFalse(operator.hasNext());
  }

  @Test
  public void testMultipleRowsInOneBlock() throws Exception {
    Operator child = mock(Operator.class);
    when(child.nextWithTimer()).thenReturn(block(1, 2));
    assertThrows(SemanticException.class, () -> operator(child).next());
  }

  @Test
  public void testMultipleRowsAcrossBlocks() throws Exception {
    Operator child = mock(Operator.class);
    when(child.nextWithTimer()).thenReturn(block(1), block(2));
    EnforceSingleRowOperator operator = operator(child);
    assertEquals(1, operator.next().getColumn(0).getInt(0));
    assertTrue(operator.hasNext());
    assertThrows(SemanticException.class, operator::next);
  }

  private static EnforceSingleRowOperator operator(Operator child) {
    return new EnforceSingleRowOperator(
        mock(CommonOperatorContext.class), child, Collections.singletonList(TSDataType.INT32));
  }

  private static TsBlock block(int... values) {
    TsBlockBuilder builder = new TsBlockBuilder(Collections.singletonList(TSDataType.INT32));
    for (int value : values) {
      builder.getTimeColumnBuilder().writeLong(0);
      builder.getColumnBuilder(0).writeInt(value);
      builder.declarePosition();
    }
    return builder.build();
  }
}
