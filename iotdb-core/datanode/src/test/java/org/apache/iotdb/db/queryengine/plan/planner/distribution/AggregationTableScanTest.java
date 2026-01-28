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

package org.apache.iotdb.db.queryengine.plan.planner.distribution;

import org.apache.iotdb.common.rpc.thrift.TAggregationType;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.AccumulatorFactory;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.FirstAccumulator;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.FirstByAccumulator;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.FirstByDescAccumulator;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.FirstDescAccumulator;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.LastAccumulator;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.LastByAccumulator;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.LastByDescAccumulator;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.LastDescAccumulator;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.TableAccumulator;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SymbolReference;

import org.apache.tsfile.enums.TSDataType;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertTrue;

public class AggregationTableScanTest {

  private static final String TIME_COL = "time";
  private static final String S1_COL = "s1";
  private static final String S2_COL = "s2";

  @Test
  public void testFirstAccumulator() {
    // Case 1: Ascending scan ordered by time -> Use optimized FirstAccumulator
    verifyAccumulator("first", TAggregationType.FIRST, true, true, FirstAccumulator.class);

    // Case 2: Descending scan ordered by time -> Use FirstDescAccumulator
    verifyAccumulator("first", TAggregationType.FIRST, false, true, FirstDescAccumulator.class);

    // Case 3: Ordered by non-time column -> Fallback to FirstDescAccumulator
    verifyAccumulator("first", TAggregationType.FIRST, true, false, FirstDescAccumulator.class);
  }

  @Test
  public void testLastAccumulator() {
    // Case 1: Ascending scan ordered by time -> Use LastAccumulator
    verifyAccumulator("last", TAggregationType.LAST, true, true, LastAccumulator.class);

    // Case 2: Descending scan ordered by time -> Use LastDescAccumulator
    verifyAccumulator("last", TAggregationType.LAST, false, true, LastDescAccumulator.class);

    // Case 3: Ordered by non-time column -> Use LastAccumulator
    verifyAccumulator("last", TAggregationType.LAST, false, false, LastAccumulator.class);
  }

  @Test
  public void testFirstByAccumulator() {
    // Case 1: Ascending scan ordered by time -> Use optimized FirstByAccumulator
    verifyByAccumulator(
        "first_by", TAggregationType.FIRST_BY, true, true, FirstByAccumulator.class);

    // Case 2: Descending scan ordered by time -> Use FirstByDescAccumulator
    verifyByAccumulator(
        "first_by", TAggregationType.FIRST_BY, false, true, FirstByDescAccumulator.class);

    // Case 3: Ordered by non-time column -> Fallback to FirstByDescAccumulator
    verifyByAccumulator(
        "first_by", TAggregationType.FIRST_BY, true, false, FirstByDescAccumulator.class);
  }

  @Test
  public void testLastByAccumulator() {
    // Case 1: Ascending scan ordered by time -> Use LastByAccumulator
    verifyByAccumulator("last_by", TAggregationType.LAST_BY, true, true, LastByAccumulator.class);

    // Case 2: Descending scan ordered by time -> Use LastByDescAccumulator
    verifyByAccumulator(
        "last_by", TAggregationType.LAST_BY, false, true, LastByDescAccumulator.class);

    // Case 3: Ordered by non-time column -> Use LastByAccumulator
    verifyByAccumulator("last_by", TAggregationType.LAST_BY, false, false, LastByAccumulator.class);
  }

  private void verifyAccumulator(
      String funcName,
      TAggregationType aggType,
      boolean ascending,
      boolean isTimeOrder,
      Class<?> expectedClass) {
    List<Expression> inputExpressions =
        Arrays.asList(
            new SymbolReference(S1_COL), new SymbolReference(isTimeOrder ? TIME_COL : S2_COL));
    List<TSDataType> inputDataTypes = Collections.singletonList(TSDataType.INT32);

    doCreateAndAssert(
        funcName, aggType, inputDataTypes, inputExpressions, ascending, expectedClass);
  }

  private void verifyByAccumulator(
      String funcName,
      TAggregationType aggType,
      boolean ascending,
      boolean isTimeOrder,
      Class<?> expectedClass) {

    List<Expression> inputExpressions =
        Arrays.asList(
            new SymbolReference(S1_COL),
            new SymbolReference(S2_COL),
            new SymbolReference(isTimeOrder ? TIME_COL : S1_COL));
    List<TSDataType> inputDataTypes = Arrays.asList(TSDataType.INT32, TSDataType.INT64);

    doCreateAndAssert(
        funcName, aggType, inputDataTypes, inputExpressions, ascending, expectedClass);
  }

  private void doCreateAndAssert(
      String funcName,
      TAggregationType aggType,
      List<TSDataType> types,
      List<Expression> expressions,
      boolean ascending,
      Class<?> expectedClass) {

    Map<String, String> inputAttribute = new HashMap<>();
    boolean isAggTableScan = true;
    Set<String> measurementColumnNames = new HashSet<>(Collections.singletonList(S1_COL));
    boolean distinct = false;

    TableAccumulator accumulator =
        AccumulatorFactory.createAccumulator(
            funcName,
            aggType,
            types,
            expressions,
            inputAttribute,
            ascending,
            isAggTableScan,
            TIME_COL,
            measurementColumnNames,
            distinct);

    String msg =
        String.format(
            "Func: %s, Asc: %s, Expressions: %s. Expected: %s, Actual: %s",
            funcName,
            ascending,
            expressions,
            expectedClass.getSimpleName(),
            accumulator.getClass().getSimpleName());

    assertTrue(msg, expectedClass.isInstance(accumulator));
  }
}
