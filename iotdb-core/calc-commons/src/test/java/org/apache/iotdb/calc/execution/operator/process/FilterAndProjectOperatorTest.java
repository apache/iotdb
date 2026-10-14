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

package org.apache.iotdb.calc.execution.operator.process;

import org.apache.iotdb.calc.execution.operator.CommonOperatorContext;
import org.apache.iotdb.calc.execution.operator.Operator;
import org.apache.iotdb.calc.plan.planner.memory.MemoryReservationManager;
import org.apache.iotdb.calc.transformation.dag.column.ColumnTransformer;
import org.apache.iotdb.calc.transformation.dag.column.FailFunctionColumnTransformer;
import org.apache.iotdb.calc.transformation.dag.column.TableCaseWhenThenColumnTransformer;
import org.apache.iotdb.calc.transformation.dag.column.binary.ArithmeticAdditionColumnTransformer;
import org.apache.iotdb.calc.transformation.dag.column.binary.CompareGreaterThanColumnTransformer;
import org.apache.iotdb.calc.transformation.dag.column.binary.CompareNonEqualColumnTransformer;
import org.apache.iotdb.calc.transformation.dag.column.leaf.ConstantColumnTransformer;
import org.apache.iotdb.calc.transformation.dag.column.leaf.IdentityColumnTransformer;
import org.apache.iotdb.calc.transformation.dag.column.leaf.LeafColumnTransformer;
import org.apache.iotdb.calc.transformation.dag.column.leaf.NullColumnTransformer;
import org.apache.iotdb.calc.transformation.dag.column.multi.CoalesceColumnTransformer;
import org.apache.iotdb.calc.transformation.dag.column.multi.MappableUDFColumnTransformer;
import org.apache.iotdb.calc.transformation.dag.column.ternary.BetweenColumnTransformer;
import org.apache.iotdb.calc.transformation.dag.column.unary.scalar.CastFunctionColumnTransformer;
import org.apache.iotdb.calc.transformation.dag.udf.UDTFExecutor;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.column.BinaryColumn;
import org.apache.tsfile.read.common.block.column.BooleanColumn;
import org.apache.tsfile.read.common.block.column.DoubleColumn;
import org.apache.tsfile.read.common.block.column.RunLengthEncodedColumn;
import org.apache.tsfile.read.common.block.column.TimeColumn;
import org.apache.tsfile.utils.Binary;
import org.junit.Test;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

import static org.apache.tsfile.read.common.type.BooleanType.BOOLEAN;
import static org.apache.tsfile.read.common.type.DoubleType.DOUBLE;
import static org.apache.tsfile.read.common.type.IntType.INT32;
import static org.apache.tsfile.read.common.type.StringType.STRING;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class FilterAndProjectOperatorTest {

  @Test
  public void testProjectionDoesNotReuseColumnsAcrossDifferentSizedBlocks() throws Exception {
    Graph graph = new Graph();
    ColumnTransformer input = graph.input(0);
    ColumnTransformer caseWhen =
        graph.caseWhen(graph.greaterThan(input, 0), graph.constant(1), graph.nullValue());
    try (Operator operator =
        project(graph, new TsBlock[] {block(1), block(-2, 3), block(4)}, caseWhen, input)) {
      assertBlock(operator.next(), new Double[] {1.0}, new Double[] {1.0});
      assertBlock(operator.next(), new Double[] {null, 1.0}, new Double[] {-2.0, 3.0});
      assertBlock(operator.next(), new Double[] {1.0}, new Double[] {4.0});
    }
  }

  @Test
  public void testProjectionDoesNotReuseColumnsAcrossEqualSizedBlocks() throws Exception {
    Graph graph = new Graph();
    ColumnTransformer input = graph.input(0);
    ColumnTransformer caseWhen =
        graph.caseWhen(graph.greaterThan(input, 0), graph.constant(1), graph.nullValue());
    try (Operator operator =
        project(graph, new TsBlock[] {block(1, 2), block(-3, 4)}, caseWhen, input)) {
      assertBlock(operator.next(), new Double[] {1.0, 1.0}, new Double[] {1.0, 2.0});
      assertBlock(operator.next(), new Double[] {null, 1.0}, new Double[] {-3.0, 4.0});
    }
  }

  @Test
  public void testProjectionHandlesEmptyBlocks() throws Exception {
    Graph graph = new Graph();
    ColumnTransformer input = graph.input(0);
    ColumnTransformer caseWhen =
        graph.caseWhen(graph.greaterThan(input, 0), graph.constant(1), graph.nullValue());
    try (Operator operator =
        project(graph, new TsBlock[] {block(1), block(), block(-2)}, caseWhen, input)) {
      assertBlock(operator.next(), new Double[] {1.0}, new Double[] {1.0});
      assertBlock(operator.next(), new Double[0], new Double[0]);
      assertBlock(operator.next(), new Double[] {null}, new Double[] {-2.0});
    }
  }

  @Test
  public void testProjectionClearsSharedIntermediateExpressions() throws Exception {
    Graph graph = new Graph();
    ColumnTransformer shared = graph.add(graph.input(0), 0);
    ColumnTransformer caseWhen =
        graph.caseWhen(graph.greaterThan(shared, 0), graph.constant(1), graph.nullValue());
    try (Operator operator =
        project(graph, new TsBlock[] {block(1), block(-2)}, caseWhen, graph.add(shared, 1))) {
      assertBlock(operator.next(), new Double[] {1.0}, new Double[] {2.0});
      assertBlock(operator.next(), new Double[] {null}, new Double[] {-1.0});
    }
  }

  @Test
  public void testFilterDoesNotReuseColumnsAcrossBlocks() throws Exception {
    Graph filter = new Graph();
    ColumnTransformer input = filter.input(0);
    ColumnTransformer caseWhen =
        filter.caseWhen(filter.greaterThan(input, 0), filter.constant(1), filter.constant(0));
    ColumnTransformer predicate = filter.greaterThan(filter.add(caseWhen, input), 0);
    Graph projection = new Graph();
    try (Operator operator =
        operator(
            new TsBlock[] {block(1), block(-2, 3), block(-4)},
            filter,
            predicate,
            Collections.emptyList(),
            projection,
            projection.input(0))) {
      assertBlock(operator.next(), new Double[] {1.0});
      assertBlock(operator.next(), new Double[] {3.0});
      assertBlock(operator.next(), new Double[0]);
    }
  }

  @Test
  public void testFilterCommonExpressionsDoNotCrossBlocks() throws Exception {
    for (boolean runLengthEncodedTime : new boolean[] {false, true}) {
      Graph filter = new Graph();
      ColumnTransformer shared = filter.add(filter.input(0), 0);
      ColumnTransformer caseWhen =
          filter.caseWhen(filter.greaterThan(shared, 0), filter.constant(1), filter.constant(0));
      ColumnTransformer predicate = filter.greaterThan(caseWhen, 0);
      Graph projection = new Graph();
      try (Operator operator =
          operator(
              new TsBlock[] {
                block(runLengthEncodedTime, 1),
                block(runLengthEncodedTime, -2, 3),
                block(runLengthEncodedTime, 4, -5)
              },
              filter,
              predicate,
              Collections.singletonList(shared),
              projection,
              projection.input(0),
              projection.input(1))) {
        assertBlock(operator.next(), new Double[] {1.0}, new Double[] {1.0});
        assertBlock(operator.next(), new Double[] {3.0}, new Double[] {3.0});
        assertBlock(operator.next(), new Double[] {4.0}, new Double[] {4.0});
      }
    }
  }

  @Test
  public void testCommonCastOnlyEvaluatesRowsPassingCaseFilter() throws Exception {
    for (boolean runLengthEncodedTime : new boolean[] {false, true}) {
      Graph filter = new Graph();
      ColumnTransformer input = filter.leaf(new IdentityColumnTransformer(STRING, 0));
      ColumnTransformer invalid =
          filter.leaf(
              new ConstantColumnTransformer(
                  STRING,
                  new BinaryColumn(
                      1,
                      Optional.empty(),
                      new Binary[] {new Binary("n/a", TSFileConfig.STRING_CHARSET)})));
      ColumnTransformer guard =
          new CompareNonEqualColumnTransformer(BOOLEAN, reference(input), reference(invalid));
      ColumnTransformer common =
          new CastFunctionColumnTransformer(INT32, reference(input), ZoneId.of("UTC"));
      ColumnTransformer otherwise =
          filter.leaf(
              new ConstantColumnTransformer(
                  BOOLEAN, new BooleanColumn(1, Optional.empty(), new boolean[] {false})));
      ColumnTransformer predicate =
          new TableCaseWhenThenColumnTransformer(
              BOOLEAN,
              Collections.singletonList(reference(guard)),
              Collections.singletonList(reference(filter.greaterThan(common, 10))),
              reference(otherwise));
      IdentityColumnTransformer output = new IdentityColumnTransformer(INT32, 1);
      reference(common);
      try (Operator operator =
          new FilterAndProjectOperator(
              new TestOperatorContext(),
              new ValuesOperator(
                  new TestOperatorContext(),
                  Arrays.asList(
                      stringBlock(runLengthEncodedTime, "n/a", "5"),
                      stringBlock(runLengthEncodedTime, "n/a", "50", "5"),
                      stringBlock(runLengthEncodedTime, "n/a", "60"))),
              Arrays.asList(TSDataType.STRING, TSDataType.INT32),
              filter.leaves,
              reference(predicate),
              Collections.singletonList(common),
              Collections.singletonList(output),
              Collections.singletonList(reference(output)),
              false,
              true)) {
        assertEquals(0, operator.next().getPositionCount());
        TsBlock firstMatch = operator.next();
        assertEquals(1, firstMatch.getPositionCount());
        assertEquals(50, firstMatch.getColumn(0).getInt(0));
        TsBlock secondMatch = operator.next();
        assertEquals(1, secondMatch.getPositionCount());
        assertEquals(60, secondMatch.getColumn(0).getInt(0));
      }
    }
  }

  @Test
  public void testCommonMultiArgumentExpressionRespectsCaseFilter() throws Exception {
    Graph filter = new Graph();
    ColumnTransformer common =
        new CoalesceColumnTransformer(
            DOUBLE, Arrays.asList(reference(filter.input(0)), reference(filter.constant(0))));
    ColumnTransformer caseWhen =
        filter.caseWhen(filter.greaterThan(common, 0), filter.constant(1), filter.constant(0));
    Graph projection = new Graph();
    try (Operator operator =
        operator(
            new TsBlock[] {block(-2, 3), block(4, -5)},
            filter,
            filter.greaterThan(caseWhen, 0),
            Collections.singletonList(common),
            projection,
            projection.input(1))) {
      assertBlock(operator.next(), new Double[] {3.0});
      assertBlock(operator.next(), new Double[] {4.0});
    }
  }

  @Test
  public void testBinaryPreservesSiblingColumnBeforeCase() throws Exception {
    Graph graph = new Graph();
    ColumnTransformer input = graph.input(0);
    ColumnTransformer caseWhen =
        graph.caseWhen(graph.greaterThan(input, 0), graph.constant(2), graph.constant(3));
    try (Operator operator =
        project(graph, new TsBlock[] {block(1, -2)}, graph.add(input, caseWhen))) {
      assertBlock(operator.next(), new Double[] {3.0, 1.0});
    }
  }

  @Test
  public void testMultiArgumentFunctionPreservesSiblingColumnBeforeCase() throws Exception {
    for (boolean selected : new boolean[] {false, true}) {
      Graph graph = new Graph();
      ColumnTransformer input = graph.input(0);
      ColumnTransformer caseWhen =
          graph.caseWhen(graph.greaterThan(input, 0), graph.constant(2), graph.constant(3));
      ColumnTransformer output =
          new CoalesceColumnTransformer(
              DOUBLE, Arrays.asList(reference(input), reference(caseWhen)));
      if (selected) {
        output = graph.caseWhen(graph.greaterThan(graph.constant(1), 0), output, graph.nullValue());
      }
      try (Operator operator = project(graph, new TsBlock[] {block(1, -2)}, output)) {
        assertBlock(operator.next(), new Double[] {1.0, -2.0});
      }
    }
  }

  @Test
  public void testTernaryPreservesSiblingColumnsBeforeCase() throws Exception {
    for (boolean selected : new boolean[] {false, true}) {
      Graph graph = new Graph();
      ColumnTransformer input = graph.input(0);
      ColumnTransformer caseWhen =
          graph.caseWhen(graph.greaterThan(input, 0), graph.constant(2), graph.constant(3));
      ColumnTransformer between =
          new BetweenColumnTransformer(
              BOOLEAN, reference(input), reference(graph.constant(-5)), reference(caseWhen), false);
      ColumnTransformer output = graph.caseWhen(between, graph.constant(1), graph.constant(0));
      if (selected) {
        output = graph.caseWhen(graph.greaterThan(graph.constant(1), 0), output, graph.nullValue());
      }
      try (Operator operator = project(graph, new TsBlock[] {block(1, -2)}, output)) {
        assertBlock(operator.next(), new Double[] {1.0, 1.0});
      }
    }
  }

  @Test
  public void testMappableUdfPreservesSiblingColumnsBeforeCase() throws Exception {
    for (boolean selected : new boolean[] {false, true}) {
      Graph graph = new Graph();
      ColumnTransformer input = graph.input(0);
      ColumnTransformer caseWhen =
          graph.caseWhen(graph.greaterThan(input, 0), graph.constant(2), graph.constant(3));
      ColumnTransformer output = sumUdf(input, caseWhen);
      if (selected) {
        output = graph.caseWhen(graph.greaterThan(graph.constant(1), 0), output, graph.nullValue());
      }
      try (Operator operator = project(graph, new TsBlock[] {block(1, -2)}, output)) {
        assertBlock(operator.next(), new Double[] {3.0, 1.0});
      }
    }
  }

  @Test
  public void testProjectionClearsCachedDescendantsOfNestedCase() throws Exception {
    Graph graph = new Graph();
    ColumnTransformer shared = graph.add(graph.input(0), 0);
    ColumnTransformer innerCase =
        graph.caseWhen(graph.greaterThan(shared, 0), graph.constant(1), graph.constant(0));
    ColumnTransformer outerCase =
        graph.caseWhen(graph.greaterThan(innerCase, 0), graph.constant(2), graph.constant(0));
    // Simulate an intermediate cache retained from the previous input block. Clearing only the
    // outer CASE itself must not allow this cached value to reach the new block's WHEN condition.
    shared.initializeColumnCache(doubleColumn(1));
    try (Operator operator = project(graph, new TsBlock[] {block(-2)}, outerCase)) {
      assertBlock(operator.next(), new Double[] {0.0});
    }
  }

  @Test
  public void testThenExpressionIsRecomputedWithoutSelectionForLaterOutput() throws Exception {
    Graph graph = new Graph();
    ColumnTransformer input = graph.input(0);
    ColumnTransformer shared = graph.add(input, 1);
    ColumnTransformer caseWhen =
        graph.caseWhen(graph.greaterThan(input, 0), shared, graph.constant(0));
    try (Operator operator =
        project(graph, new TsBlock[] {block(-2, 3), block(-4, 5)}, caseWhen, shared)) {
      assertBlock(operator.next(), new Double[] {0.0, 4.0}, new Double[] {-1.0, 4.0});
      assertBlock(operator.next(), new Double[] {0.0, 6.0}, new Double[] {-3.0, 6.0});
    }
  }

  @Test
  public void testCaseStillSkipsUnusedFailBranch() throws Exception {
    Graph graph = new Graph();
    ColumnTransformer caseWhen =
        graph.caseWhen(
            graph.greaterThan(graph.input(0), 0),
            graph.constant(1),
            new FailFunctionColumnTransformer(DOUBLE, "The unused ELSE branch was evaluated"));
    try (Operator operator = project(graph, new TsBlock[] {block(1, 2), block(3)}, caseWhen)) {
      assertBlock(operator.next(), new Double[] {1.0, 1.0});
      assertBlock(operator.next(), new Double[] {1.0});
    }
  }

  @Test
  public void testProjectionReusesSharedExpressionsWithinEachBlock() throws Exception {
    Graph graph = new Graph();
    CountingIdentityColumnTransformer input = new CountingIdentityColumnTransformer();
    graph.leaves.add(input);
    ColumnTransformer shared = graph.add(input, 0);
    try (Operator operator =
        project(graph, new TsBlock[] {block(1), block(2)}, shared, graph.add(shared, 1))) {
      assertBlock(operator.next(), new Double[] {1.0}, new Double[] {2.0});
      assertEquals(1, input.evaluationCount);
      assertBlock(operator.next(), new Double[] {2.0}, new Double[] {3.0});
      assertEquals(2, input.evaluationCount);
    }
  }

  @Test
  public void testCopiedOperatorReusesCollectedTransformers() throws Exception {
    Graph graph = new Graph();
    CountingIdentityColumnTransformer input = new CountingIdentityColumnTransformer();
    graph.leaves.add(input);
    try (FilterAndProjectOperator original = project(graph, new TsBlock[0], graph.add(input, 1));
        FilterAndProjectOperator copy =
            new FilterAndProjectOperator(
                original,
                new ValuesOperator(new TestOperatorContext(), Arrays.asList(block(1), block(2))))) {
      assertEquals(1, input.childTraversalCount);
      assertBlock(copy.next(), new Double[] {2.0});
      assertBlock(copy.next(), new Double[] {3.0});
      assertEquals(1, input.childTraversalCount);
    }
  }

  @Test
  public void testProjectionCollectsSharedDagOnlyOnce() throws Exception {
    Graph graph = new Graph();
    CountingIdentityColumnTransformer input = new CountingIdentityColumnTransformer();
    graph.leaves.add(input);
    ColumnTransformer shared = input;
    // Each level references the same child twice. A traversal without deduplication would visit
    // the input 4096 times per root, although this DAG contains only 13 distinct transformers.
    for (int i = 0; i < 12; i++) {
      shared = graph.add(shared, shared);
    }
    try (Operator operator =
        project(graph, new TsBlock[] {block(1), block(2)}, shared, graph.add(shared, 1))) {
      assertEquals(1, input.childTraversalCount);
      assertBlock(operator.next(), new Double[] {4096.0}, new Double[] {4097.0});
      assertEquals(1, input.childTraversalCount);
      assertBlock(operator.next(), new Double[] {8192.0}, new Double[] {8193.0});
      assertEquals(1, input.childTraversalCount);
    }
  }

  @Test
  public void testFilterAndCommonExpressionsCollectSharedDagOnlyOnce() throws Exception {
    Graph filter = new Graph();
    CountingIdentityColumnTransformer input = new CountingIdentityColumnTransformer();
    filter.leaves.add(input);
    ColumnTransformer shared = filter.add(input, 1);
    ColumnTransformer predicate = filter.greaterThan(shared, 0);
    Graph projection = new Graph();
    try (Operator operator =
        operator(
            new TsBlock[] {block(1), block(-2, 3)},
            filter,
            predicate,
            Collections.singletonList(shared),
            projection,
            projection.input(0),
            projection.input(1))) {
      assertEquals(1, input.childTraversalCount);
      assertBlock(operator.next(), new Double[] {1.0}, new Double[] {2.0});
      assertEquals(1, input.childTraversalCount);
      assertBlock(operator.next(), new Double[] {3.0}, new Double[] {4.0});
      assertEquals(1, input.childTraversalCount);
    }
  }

  @Test
  public void testMappableUdfClearsCaseDescendantsAcrossBlocks() throws Exception {
    Graph graph = new Graph();
    ColumnTransformer input = graph.input(0);
    ColumnTransformer caseWhen =
        graph.caseWhen(graph.greaterThan(input, 0), graph.constant(1), graph.constant(0));
    ColumnTransformer udf = sumUdf(caseWhen, input);
    try (Operator operator =
        project(graph, new TsBlock[] {block(1), block(-2, 3), block(4)}, udf)) {
      assertBlock(operator.next(), new Double[] {2.0});
      assertBlock(operator.next(), new Double[] {-2.0, 4.0});
      assertBlock(operator.next(), new Double[] {5.0});
    }
  }

  private static ColumnTransformer sumUdf(ColumnTransformer left, ColumnTransformer right) {
    return new MappableUDFColumnTransformer(
        DOUBLE,
        new ColumnTransformer[] {reference(left), reference(right)},
        new UDTFExecutor("test", ZoneId.of("UTC")) {
          @Override
          public void execute(Column[] columns, ColumnBuilder builder) {
            for (int i = 0; i < columns[0].getPositionCount(); i++) {
              builder.writeDouble(columns[0].getDouble(i) + columns[1].getDouble(i));
            }
          }

          @Override
          public void beforeDestroy() {
            // No UDF resources are allocated by this test executor.
          }
        });
  }

  @Test
  public void testReusingCasePreservesPendingSiblingColumn() throws Exception {
    Graph graph = new Graph();
    ColumnTransformer input = graph.input(0);
    ColumnTransformer sharedCase =
        graph.caseWhen(graph.greaterThan(input, 0), graph.constant(1), graph.constant(0));
    ColumnTransformer nestedCase =
        graph.caseWhen(graph.greaterThan(sharedCase, 0), graph.constant(2), graph.constant(3));
    // The first output leaves sharedCase cached. In the second output, input is evaluated before
    // nestedCase reuses sharedCase. Its cleanup must preserve input until the addition reads it.
    try (Operator operator =
        project(
            graph,
            new TsBlock[] {block(-3, 0, 2), block(4, -1)},
            sharedCase,
            graph.add(input, nestedCase))) {
      assertBlock(operator.next(), new Double[] {0.0, 0.0, 1.0}, new Double[] {0.0, 3.0, 4.0});
      assertBlock(operator.next(), new Double[] {1.0, 0.0}, new Double[] {6.0, 2.0});
    }
  }

  private static FilterAndProjectOperator project(
      Graph graph, TsBlock[] blocks, ColumnTransformer... outputs) {
    return operator(blocks, null, null, Collections.emptyList(), graph, outputs);
  }

  private static FilterAndProjectOperator operator(
      TsBlock[] blocks,
      Graph filter,
      ColumnTransformer predicate,
      List<ColumnTransformer> common,
      Graph projection,
      ColumnTransformer... outputs) {
    List<ColumnTransformer> projectOutputs = new ArrayList<>();
    for (ColumnTransformer output : outputs) {
      projectOutputs.add(reference(output));
    }
    for (ColumnTransformer transformer : common) {
      reference(transformer);
    }
    return new FilterAndProjectOperator(
        new TestOperatorContext(),
        new ValuesOperator(new TestOperatorContext(), Arrays.asList(blocks)),
        Collections.nCopies(1 + common.size(), TSDataType.DOUBLE),
        filter == null ? Collections.emptyList() : filter.leaves,
        predicate == null ? null : reference(predicate),
        common,
        projection.leaves,
        projectOutputs,
        false,
        predicate != null);
  }

  private static ColumnTransformer reference(ColumnTransformer transformer) {
    transformer.addReferenceCount();
    return transformer;
  }

  private static DoubleColumn doubleColumn(double... values) {
    return new DoubleColumn(values.length, Optional.empty(), values);
  }

  private static TsBlock block(double... values) {
    return block(false, values);
  }

  private static TsBlock stringBlock(boolean runLengthEncodedTime, String... values) {
    Binary[] binaries =
        Arrays.stream(values)
            .map(value -> new Binary(value, TSFileConfig.STRING_CHARSET))
            .toArray(Binary[]::new);
    return new TsBlock(
        block(runLengthEncodedTime, new double[values.length]).getTimeColumn(),
        new BinaryColumn(values.length, Optional.empty(), binaries));
  }

  private static TsBlock block(boolean runLengthEncodedTime, double... values) {
    Column timeColumn =
        runLengthEncodedTime
            ? new RunLengthEncodedColumn(new TimeColumn(1, new long[] {0}), values.length)
            : new TimeColumn(values.length, new long[values.length]);
    return new TsBlock(timeColumn, doubleColumn(values));
  }

  private static void assertBlock(TsBlock actual, Double[]... expectedColumns) {
    assertEquals(expectedColumns.length, actual.getValueColumnCount());
    assertEquals(expectedColumns[0].length, actual.getPositionCount());
    for (int columnIndex = 0; columnIndex < expectedColumns.length; columnIndex++) {
      Column column = actual.getColumn(columnIndex);
      Double[] expected = expectedColumns[columnIndex];
      assertEquals(expected.length, column.getPositionCount());
      for (int row = 0; row < expected.length; row++) {
        if (expected[row] == null) {
          assertTrue(column.isNull(row));
        } else {
          assertFalse(column.isNull(row));
          assertEquals(expected[row], column.getDouble(row), 0.0);
        }
      }
    }
  }

  private static class Graph {
    private final List<LeafColumnTransformer> leaves = new ArrayList<>();

    private ColumnTransformer input(int index) {
      return leaf(new IdentityColumnTransformer(DOUBLE, index));
    }

    private ColumnTransformer constant(double value) {
      return leaf(new ConstantColumnTransformer(DOUBLE, doubleColumn(value)));
    }

    private ColumnTransformer nullValue() {
      return leaf(new NullColumnTransformer());
    }

    private ColumnTransformer leaf(LeafColumnTransformer transformer) {
      leaves.add(transformer);
      return transformer;
    }

    private ColumnTransformer add(ColumnTransformer input, double value) {
      return add(input, constant(value));
    }

    private ColumnTransformer add(ColumnTransformer left, ColumnTransformer right) {
      return new ArithmeticAdditionColumnTransformer(DOUBLE, reference(left), reference(right));
    }

    private ColumnTransformer greaterThan(ColumnTransformer input, double value) {
      return new CompareGreaterThanColumnTransformer(
          BOOLEAN, reference(input), reference(constant(value)));
    }

    private ColumnTransformer caseWhen(
        ColumnTransformer when, ColumnTransformer then, ColumnTransformer otherwise) {
      return new TableCaseWhenThenColumnTransformer(
          DOUBLE,
          Collections.singletonList(reference(when)),
          Collections.singletonList(reference(then)),
          reference(otherwise));
    }
  }

  private static class CountingIdentityColumnTransformer extends IdentityColumnTransformer {
    private int evaluationCount;
    private int childTraversalCount;

    private CountingIdentityColumnTransformer() {
      super(DOUBLE, 0);
    }

    @Override
    protected void evaluate() {
      evaluationCount++;
      super.evaluate();
    }

    @Override
    protected void forEachChild(Consumer<ColumnTransformer> action) {
      childTraversalCount++;
    }
  }

  private static class TestOperatorContext extends CommonOperatorContext {
    private TestOperatorContext() {
      super(0, new PlanNodeId("test"), "test");
    }

    @Override
    public MemoryReservationManager getMemoryReservationContext() {
      return null;
    }

    @Override
    public int getFragmentId() {
      return 0;
    }

    @Override
    public int getPipelineId() {
      return 0;
    }

    @Override
    public long ramBytesUsed() {
      return 0;
    }
  }
}
