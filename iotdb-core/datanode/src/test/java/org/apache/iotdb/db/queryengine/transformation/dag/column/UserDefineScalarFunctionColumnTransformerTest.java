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

package org.apache.iotdb.db.queryengine.transformation.dag.column;

import org.apache.iotdb.calc.execution.relational.ColumnTransformerBuilder;
import org.apache.iotdb.calc.transformation.dag.column.leaf.IdentityColumnTransformer;
import org.apache.iotdb.calc.transformation.dag.column.udf.UserDefineScalarFunctionTransformer;
import org.apache.iotdb.udf.api.IoTDBLocal;
import org.apache.iotdb.udf.api.relational.ScalarFunction;
import org.apache.iotdb.udf.api.relational.access.Record;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.column.DoubleColumn;
import org.apache.tsfile.read.common.block.column.TimeColumn;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.Optional;

import static org.apache.tsfile.read.common.type.DoubleType.DOUBLE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class UserDefineScalarFunctionColumnTransformerTest {
  @Test
  public void testSelectionSkipsRejectedRowsWithCachedAndUncachedInputs() throws Exception {
    for (boolean cached : new boolean[] {false, true}) {
      IoTDBLocal local = Mockito.mock(IoTDBLocal.class);
      ColumnTransformerBuilder.Context context =
          Mockito.mock(ColumnTransformerBuilder.Context.class);
      Mockito.when(context.getIoTDBLocalFactory()).thenReturn((s, f, q, d) -> local);
      Mockito.when(context.getFragmentInstanceId()).thenReturn("test-fragment");
      Mockito.when(context.getOuterGlobalQueryId()).thenReturn("test-query");
      Mockito.when(context.getOuterQueryDeadlineMs()).thenReturn(Long.MAX_VALUE);
      ScalarFunction function = Mockito.mock(ScalarFunction.class);
      Mockito.when(function.evaluate(Mockito.any(Record.class), Mockito.eq(local)))
          .thenAnswer(
              invocation -> {
                double value = ((Record) invocation.getArgument(0)).getDouble(0);
                assertTrue(value > 0);
                return value + 1;
              });
      IdentityColumnTransformer input = new IdentityColumnTransformer(DOUBLE, 0);
      input.addReferenceCount();
      input.initFromTsBlock(
          new TsBlock(
              new TimeColumn(2, new long[2]),
              new DoubleColumn(2, Optional.empty(), new double[] {-2, 3})));
      if (cached) {
        input.tryEvaluate();
      }
      UserDefineScalarFunctionTransformer transformer =
          new UserDefineScalarFunctionTransformer(
              DOUBLE, function, Collections.singletonList(input), null, context);
      try {
        Column result = transformer.getColumnWithSelection(new boolean[] {false, true});
        assertEquals(2, result.getPositionCount());
        assertTrue(result.isNull(0));
        assertEquals(4.0, result.getDouble(1), 0.0);
        Column empty = transformer.getColumnWithSelection(new boolean[] {false, false});
        assertTrue(empty.isNull(0));
        assertTrue(empty.isNull(1));
        Mockito.verify(function, Mockito.times(1))
            .evaluate(Mockito.any(Record.class), Mockito.eq(local));
      } finally {
        transformer.close();
      }
    }
  }
}
