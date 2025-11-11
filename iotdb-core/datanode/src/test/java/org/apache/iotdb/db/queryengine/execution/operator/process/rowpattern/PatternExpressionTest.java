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

package org.apache.iotdb.db.queryengine.execution.operator.process.rowpattern;

import org.apache.iotdb.db.queryengine.execution.operator.process.rowpattern.expression.ArithmeticOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.rowpattern.expression.BinaryComputation;
import org.apache.iotdb.db.queryengine.execution.operator.process.rowpattern.expression.ComparisonOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.rowpattern.expression.Computation;
import org.apache.iotdb.db.queryengine.execution.operator.process.rowpattern.expression.ReferenceComputation;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class PatternExpressionTest {

  @Test
  public void testComplexExpression() {
    // (#0 + #1) < #2
    Computation addition =
        new BinaryComputation(
            new ReferenceComputation(0), new ReferenceComputation(1), ArithmeticOperator.ADD);

    Computation fullExpression =
        new BinaryComputation(addition, new ReferenceComputation(2), ComparisonOperator.LESS_THAN);

    List<Object> values = Arrays.asList(3, 4, 10); // 3 + 4 < 10 -> true
    assertEquals(true, fullExpression.evaluate(values));

    values = Arrays.asList(6, 5, 10); // 6 + 5 < 10 -> false
    assertEquals(false, fullExpression.evaluate(values));
  }

  @Test
  public void testStringExpression() {
    Computation expr =
        new BinaryComputation(
            new ReferenceComputation(0), new ReferenceComputation(1), ComparisonOperator.EQUAL);
    List<Object> values = Arrays.asList("hello", "hello");
    assertEquals(true, expr.evaluate(values));
  }
}
