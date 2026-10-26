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

package org.apache.iotdb.calc.transformation.dag.column;

import org.apache.iotdb.calc.execution.operator.CommonOperatorContext;
import org.apache.iotdb.calc.execution.operator.Operator;
import org.apache.iotdb.calc.execution.operator.process.FilterAndProjectOperator;
import org.apache.iotdb.calc.plan.planner.memory.MemoryReservationManager;
import org.apache.iotdb.calc.transformation.dag.column.binary.CompareGreaterThanColumnTransformer;
import org.apache.iotdb.calc.transformation.dag.column.binary.LogicOrColumnTransformer;
import org.apache.iotdb.calc.transformation.dag.column.leaf.ConstantColumnTransformer;
import org.apache.iotdb.calc.transformation.dag.column.leaf.IdentityColumnTransformer;
import org.apache.iotdb.calc.transformation.dag.column.leaf.LeafColumnTransformer;
import org.apache.iotdb.calc.transformation.dag.column.unary.IsNullColumnTransformer;
import org.apache.iotdb.calc.transformation.dag.column.unary.scalar.CastFunctionColumnTransformer;
import org.apache.iotdb.calc.transformation.dag.column.unary.scalar.DiffColumnTransformer;
import org.apache.iotdb.calc.transformation.dag.column.unary.scalar.DiffFunctionColumnTransformer;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.column.TimeColumn;
import org.apache.tsfile.utils.Binary;
import org.junit.Test;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.tsfile.read.common.type.BooleanType.BOOLEAN;
import static org.apache.tsfile.read.common.type.DoubleType.DOUBLE;
import static org.apache.tsfile.read.common.type.StringType.STRING;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class DiffCacheLifecycleTest {

  @Test
  public void testBinaryFullEvaluationOwnsNextInputStateRegardlessOfMaskOrder() {
    for (boolean fullFirst : new boolean[] {true, false}) {
      Fixture fixture = binary();
      fixture.init(block(new Double[] {1.0, 2.0, 3.0}, new Boolean[] {true, true, true}));
      if (fullFirst) {
        assertValues(fixture.full(), null, 1.0, 1.0);
      }
      assertValues(fixture.selected(new boolean[3]), null, null, null);
      if (!fullFirst) {
        assertValues(fixture.full(), null, 1.0, 1.0);
      }
      fixture.init(block(new Double[] {4.0, 5.0, 6.0}, new Boolean[] {true, true, true}));
      assertValues(fixture.full(), 1.0, 1.0, 1.0);
    }
  }

  @Test
  public void testFreshInputsDoNotReuseMaskedChildCaches() {
    Fixture fixture = binary();
    // Model inputs shared by a stateless SET item and DIFF: getColumn need not release them.
    fixture.input.addReferenceCount();
    fixture.flags.addReferenceCount();
    fixture.init(block(new Double[] {1.0, 9.0, 5.0}, new Boolean[] {false, false, false}));
    boolean[] selection = {false, true, false};
    fixture.input.evaluateWithSelection(selection);
    fixture.flags.evaluateWithSelection(selection);
    fixture.diff.tryEvaluateWithFreshInputs();
    assertValues(fixture.diff.getColumn(), null, 8.0, -4.0);
  }

  @Test
  public void testFullSelectionOwnsNextInputState() {
    Fixture fixture = binary();
    fixture.init(block(new Double[] {1.0, 2.0, 3.0}, new Boolean[] {true, true, true}));
    assertValues(fixture.selected(new boolean[] {true, true, true}), null, 1.0, 1.0);
    assertValues(fixture.selected(new boolean[3]), null, null, null);
    fixture.init(block(new Double[] {4.0}, new Boolean[] {true}));
    assertValues(fixture.full(), 1.0);
  }

  @Test
  public void testFullStateDoesNotLeakIntoLaterCaseOnlyInputs() {
    Fixture fixture = binary();
    fixture.init(block(new Double[] {1.0, 3.0}, new Boolean[] {true, true}));
    assertValues(fixture.full(), null, 2.0);
    assertValues(fixture.selected(new boolean[2]), null, null);
    fixture.init(block(new Double[] {10.0, 20.0}, new Boolean[] {true, true}));
    assertValues(fixture.selected(new boolean[] {false, true}), null, null);
    fixture.init(block(new Double[] {25.0}, new Boolean[] {true}));
    assertValues(fixture.full(), 5.0);
  }

  @Test
  public void testBinaryCaseAndFullProjectionPreserveStateInBothOrders() throws Exception {
    for (boolean caseFirst : new boolean[] {true, false}) {
      List<LeafColumnTransformer> leaves = new ArrayList<>();
      ColumnTransformer input = input(leaves, DOUBLE, 0);
      ColumnTransformer diff = newDiff(input, input(leaves, BOOLEAN, 1));
      ColumnTransformer caseResult =
          caseWhen(greater(input, constant(leaves, 6)), diff, constant(leaves, 100));
      try (Operator operator =
          projection(
              leaves,
              caseFirst ? Arrays.asList(caseResult, diff) : Arrays.asList(diff, caseResult),
              block(new Double[] {1.0, 2.0, 3.0}, new Boolean[] {true, true, true}),
              block(new Double[] {4.0, 5.0, 6.0}, new Boolean[] {true, true, true}),
              block(new Double[] {7.0}, new Boolean[] {true}))) {
        int diffIndex = caseFirst ? 1 : 0;
        int caseIndex = 1 - diffIndex;
        TsBlock first = operator.next();
        assertValues(first.getColumn(diffIndex), null, 1.0, 1.0);
        assertValues(first.getColumn(caseIndex), 100.0, 100.0, 100.0);
        TsBlock second = operator.next();
        assertValues(second.getColumn(diffIndex), 1.0, 1.0, 1.0);
        assertValues(second.getColumn(caseIndex), 100.0, 100.0, 100.0);
        TsBlock third = operator.next();
        assertValues(third.getColumn(diffIndex), 1.0);
        assertValues(third.getColumn(caseIndex), 1.0);
      }
    }
  }

  @Test
  public void testAddingCaseOrTermDoesNotDropNextBlockFirstRow() throws Exception {
    for (boolean withCase : new boolean[] {false, true}) {
      List<LeafColumnTransformer> leaves = new ArrayList<>();
      ColumnTransformer input = input(leaves, DOUBLE, 0);
      ColumnTransformer diff = newDiff(input, input(leaves, BOOLEAN, 1));
      ColumnTransformer predicate = new IsNullColumnTransformer(BOOLEAN, reference(diff), true);
      if (withCase) {
        ColumnBuilder falseBuilder = BOOLEAN.createColumnBuilder(1);
        falseBuilder.writeBoolean(false);
        ConstantColumnTransformer no = new ConstantColumnTransformer(BOOLEAN, falseBuilder.build());
        leaves.add(no);
        ColumnTransformer caseResult =
            new TableCaseWhenThenColumnTransformer(
                BOOLEAN,
                Collections.singletonList(reference(greater(input, constant(leaves, 5)))),
                Collections.singletonList(reference(greater(diff, constant(leaves, 0)))),
                reference(no));
        predicate =
            new LogicOrColumnTransformer(BOOLEAN, reference(predicate), reference(caseResult));
      }
      IdentityColumnTransformer output = new IdentityColumnTransformer(DOUBLE, 0);
      try (Operator operator =
          new FilterAndProjectOperator(
              new TestContext(),
              new BlocksOperator(
                  block(new Double[] {1.0, 3.0, 4.0}, new Boolean[] {true, true, true}),
                  block(new Double[] {10.0, 11.0}, new Boolean[] {true, true})),
              Arrays.asList(TSDataType.DOUBLE, TSDataType.BOOLEAN),
              leaves,
              reference(predicate),
              Collections.emptyList(),
              Collections.singletonList(output),
              Collections.singletonList(reference(output)),
              false,
              true)) {
        assertValues(operator.next().getColumn(0), 3.0, 4.0);
        assertValues(operator.next().getColumn(0), 10.0, 11.0);
      }
    }
  }

  @Test
  public void testUnaryConsumedCacheDoesNotAdvanceStateAgain() {
    Fixture fixture = unary(true);
    fixture.init(block(1.0, 3.0, 6.0));
    Column first = fixture.full();
    assertValues(first, null, 2.0, 3.0);
    assertEquals(0, fixture.diff.getColumnCachePositionCount());
    assertValues(fixture.full(), null, 2.0, 3.0);
    fixture.diff.clearCache();
    assertValues(fixture.full(), null, 2.0, 3.0);
    assertValues(fixture.full(), null, 2.0, 3.0);
  }

  @Test
  public void testUnaryChangingMasksUsesCompleteInput() {
    Fixture fixture = unary(true);
    fixture.init(block(1.0, 3.0, 6.0, 10.0));
    fixture.input.evaluateWithSelection(new boolean[] {true, false, true, false});
    for (boolean[] mask : masks(4)) {
      fixture.diff.clearCache();
      assertValues(fixture.selected(mask), null, 2.0, 3.0, 4.0);
      assertValues(fixture.full(), null, 2.0, 3.0, 4.0);
    }
  }

  @Test
  public void testBinaryConsumedCacheDoesNotAdvanceStateAgain() {
    Fixture fixture = binary();
    fixture.init(block(new Double[] {1.0, 3.0, 6.0}, new Boolean[] {true, false, true}));
    Column first = fixture.full();
    assertValues(first, null, 2.0, 3.0);
    assertEquals(0, fixture.diff.getColumnCachePositionCount());
    assertValues(fixture.full(), null, 2.0, 3.0);
    fixture.diff.clearCache();
    assertValues(fixture.full(), null, 2.0, 3.0);
    assertValues(fixture.full(), null, 2.0, 3.0);
  }

  @Test
  public void testBinaryChangingMasksPreservesSelectedInputs() {
    Fixture fixture = binary();
    fixture.init(
        block(new Double[] {1.0, null, 3.0, 6.0}, new Boolean[] {true, true, false, true}));
    boolean[] partial = {true, false, true, false};
    fixture.input.evaluateWithSelection(partial);
    fixture.flags.evaluateWithSelection(partial);
    Double[][] expected = {
      {null, null, null, null}, {null, null, null, null}, {null, null, 2.0, 3.0}
    };
    boolean[][] masks = masks(4);
    for (int i = 0; i < masks.length; i++) {
      // As on master, selected evaluation replaces child caches using the requested mask.
      assertValues(fixture.selected(masks[i]), expected[i]);
      fixture.diff.clearCache();
      assertValues(fixture.full(), null, null, 2.0, 3.0);
    }
  }

  @Test
  public void testBinarySelectedRowsPreserveStateAtInputBoundaries() {
    for (boolean selectedLastRow : new boolean[] {false, true}) {
      Fixture fixture = binary();
      fixture.init(block(new Double[] {1.0, 3.0, 6.0}, new Boolean[] {true, true, true}));
      boolean[] mask = {true, true, selectedLastRow};
      assertValues(fixture.selected(mask), null, 2.0, selectedLastRow ? 3.0 : null);
      fixture.diff.clearCache();
      assertValues(fixture.selected(mask), null, 2.0, selectedLastRow ? 3.0 : null);
      fixture.init(block(new Double[] {10.0, 11.0}, new Boolean[] {true, true}));
      assertValues(fixture.full(), selectedLastRow ? 4.0 : null, 1.0);
    }
  }

  @Test
  public void testUnaryNullPolicyAndStateSurviveInputBoundaries() {
    for (boolean ignoreNull : new boolean[] {true, false}) {
      Fixture fixture = unary(ignoreNull);
      fixture.init(block(1.0, 3.0, null));
      assertValues(fixture.selected(new boolean[3]), null, 2.0, null);
      fixture.init(block(10.0, null, 20.0, null));
      assertValues(fixture.full(), ignoreNull ? 7.0 : null, null, ignoreNull ? 10.0 : null, null);
      fixture.init(block(30.0, 32.0));
      assertValues(fixture.selected(new boolean[] {false, true}), ignoreNull ? 10.0 : null, 2.0);
      fixture.init(block());
      assertValues(fixture.selected(new boolean[0]));
      fixture.init(block(40.0, 41.0));
      assertValues(fixture.full(), 8.0, 1.0);
    }
  }

  @Test
  public void testDynamicNullFlagsAndStateSurviveInputBoundaries() {
    for (Boolean ignoreNull : new Boolean[] {true, false, null}) {
      Fixture fixture = binary();
      fixture.init(block(new Double[] {1.0, 3.0, null}, new Boolean[] {false, null, ignoreNull}));
      assertValues(fixture.selected(new boolean[] {true, true, true}), null, 2.0, null);
      fixture.init(
          block(new Double[] {10.0, null, 20.0, null}, new Boolean[] {false, true, false, null}));
      assertValues(fixture.full(), Boolean.TRUE.equals(ignoreNull) ? 7.0 : null, null, 10.0, null);
      fixture.init(block(new Double[] {30.0, 32.0}, new Boolean[] {true, false}));
      assertValues(fixture.selected(new boolean[] {true, true}), null, 2.0);
      fixture.init(block(new Double[0], new Boolean[0]));
      assertValues(fixture.full());
      fixture.init(block(new Double[] {40.0, 41.0}, new Boolean[] {false, true}));
      assertValues(fixture.full(), 8.0, 1.0);
    }
  }

  @Test
  public void testNestedDiffResultsAreRetainedIndependently() {
    IdentityColumnTransformer input = new IdentityColumnTransformer(DOUBLE, 0);
    IdentityColumnTransformer flags = new IdentityColumnTransformer(BOOLEAN, 1);
    ColumnTransformer inner = new DiffColumnTransformer(DOUBLE, reference(input), reference(flags));
    ColumnTransformer outer = new DiffFunctionColumnTransformer(DOUBLE, reference(inner), true);
    reference(outer);
    ColumnTransformer[] nodes =
        ColumnTransformer.collectTransformers(Collections.singletonList(outer));
    for (ColumnTransformer node : nodes) {
      node.clearCacheForNewInput();
    }
    TsBlock first = block(new Double[] {1.0, 3.0, 6.0}, new Boolean[] {true, false, true});
    input.initFromTsBlock(first);
    flags.initFromTsBlock(first);
    outer.evaluateWithSelection(new boolean[3]);
    assertValues(outer.getColumn(), null, null, 1.0);
    outer.clearCache();
    outer.tryEvaluate();
    assertValues(outer.getColumn(), null, null, 1.0);
    for (ColumnTransformer node : nodes) {
      node.clearCacheForNewInput();
    }
    TsBlock second = block(new Double[] {10.0, 11.0}, new Boolean[] {false, true});
    input.initFromTsBlock(second);
    flags.initFromTsBlock(second);
    outer.evaluateWithSelection(new boolean[] {false, true});
    assertValues(outer.getColumn(), 1.0, -3.0);
    outer.tryEvaluate();
    assertValues(outer.getColumn(), 1.0, -3.0);
  }

  @Test
  public void testAlreadyReturnedColumnsSurviveReevaluationAndNewInputs() {
    for (Fixture fixture : Arrays.asList(unary(true), binary())) {
      fixture.init(block(new Double[] {1.0, 3.0, 6.0}, new Boolean[] {true, false, true}));
      Column first = fixture.full();
      fixture.selected(new boolean[] {false, true, false});
      fixture.diff.clearCache();
      assertValues(fixture.full(), null, 2.0, 3.0);
      fixture.init(block(new Double[] {10.0, 11.0}, new Boolean[] {true, true}));
      assertValues(fixture.full(), 4.0, 1.0);
      assertValues(first, null, 2.0, 3.0);
    }
  }

  @Test
  public void testBinarySelectionPreservesOuterCaseCastGuard() throws Exception {
    List<LeafColumnTransformer> leaves = new ArrayList<>();
    ColumnTransformer text = input(leaves, STRING, 0);
    ColumnTransformer flags = input(leaves, BOOLEAN, 1);
    ColumnTransformer valid = input(leaves, BOOLEAN, 2);
    ColumnTransformer cast =
        new CastFunctionColumnTransformer(DOUBLE, reference(text), ZoneId.of("UTC"));
    ColumnTransformer diff = newDiff(cast, flags);
    ColumnTransformer result = caseWhen(valid, diff, constant(leaves, 0));
    ColumnBuilder strings = STRING.createColumnBuilder(3);
    for (String value : new String[] {"n/a", "5", "8"}) {
      strings.writeBinary(new Binary(value, java.nio.charset.StandardCharsets.UTF_8));
    }
    ColumnBuilder flagsBuilder = BOOLEAN.createColumnBuilder(3);
    ColumnBuilder validBuilder = BOOLEAN.createColumnBuilder(3);
    for (int i = 0; i < 3; i++) {
      flagsBuilder.writeBoolean(true);
      validBuilder.writeBoolean(i > 0);
    }
    TsBlock block =
        new TsBlock(
            new TimeColumn(3, new long[3]),
            strings.build(),
            flagsBuilder.build(),
            validBuilder.build());
    try (Operator operator = projection(leaves, Collections.singletonList(result), block, block)) {
      assertValues(operator.next().getColumn(0), 0.0, null, 3.0);
      assertValues(operator.next().getColumn(0), 0.0, null, 3.0);
    }
  }

  @Test
  public void testUnaryCaseFilterAndCommonProjectionAdvanceStateOnce() throws Exception {
    assertCaseFilterAndCommonProjection(false);
  }

  @Test
  public void testDynamicFlagCaseFilterAndCommonProjectionAdvanceStateOnce() throws Exception {
    assertCaseFilterAndCommonProjection(true);
  }

  @Test
  public void testTwoCasesAndFullProjectionPreserveEachSelection() throws Exception {
    for (boolean dynamic : new boolean[] {false, true}) {
      List<LeafColumnTransformer> leaves = new ArrayList<>();
      ColumnTransformer input = input(leaves, DOUBLE, 0);
      ColumnTransformer flags = dynamic ? input(leaves, BOOLEAN, 1) : null;
      ColumnTransformer diff = newDiff(input, flags);
      ColumnTransformer first =
          caseWhen(greater(input, constant(leaves, 2)), diff, constant(leaves, 0));
      ColumnTransformer second =
          caseWhen(greater(input, constant(leaves, 2)), constant(leaves, 0), diff);
      try (Operator operator =
          projection(
              leaves,
              Arrays.asList(first, second, diff),
              block(new Double[] {1.0, 3.0, 6.0}, new Boolean[] {true, false, true}),
              block(new Double[] {10.0, 11.0}, new Boolean[] {false, true}))) {
        TsBlock firstBlock = operator.next();
        assertValues(firstBlock.getColumn(0), 0.0, dynamic ? null : 2.0, 3.0);
        assertValues(firstBlock.getColumn(1), null, 0.0, 0.0);
        assertValues(firstBlock.getColumn(2), null, 2.0, 3.0);
        TsBlock secondBlock = operator.next();
        assertValues(secondBlock.getColumn(0), 4.0, 1.0);
        assertValues(secondBlock.getColumn(1), 0.0, 0.0);
        assertValues(secondBlock.getColumn(2), 4.0, 1.0);
      }
    }
  }

  @Test
  public void testDiffPreservesMasterInputSelection() throws Exception {
    for (boolean dynamic : new boolean[] {false, true}) {
      List<LeafColumnTransformer> leaves = new ArrayList<>();
      ColumnTransformer input = input(leaves, DOUBLE, 0);
      ColumnTransformer flags = dynamic ? input(leaves, BOOLEAN, 1) : null;
      ColumnTransformer diff = newDiff(input, flags);
      ColumnTransformer result = caseWhen(greater(input, constant(leaves, 0)), input, diff);
      try (Operator operator =
          projection(
              leaves,
              Collections.singletonList(result),
              block(
                  new Double[] {1.0, -2.0, 3.0, -4.0}, new Boolean[] {true, false, true, false}))) {
        assertValues(
            operator.next().getColumn(0), 1.0, dynamic ? null : -3.0, 3.0, dynamic ? null : -7.0);
      }
    }
  }

  @Test
  public void testBinaryCaseFlagsKeepMasterSelectedEvaluation() {
    List<LeafColumnTransformer> leaves = new ArrayList<>();
    IdentityColumnTransformer input = new IdentityColumnTransformer(DOUBLE, 0);
    leaves.add(input);
    ColumnBuilder trueBuilder = BOOLEAN.createColumnBuilder(1);
    trueBuilder.writeBoolean(true);
    ConstantColumnTransformer yes = new ConstantColumnTransformer(BOOLEAN, trueBuilder.build());
    ColumnBuilder falseBuilder = BOOLEAN.createColumnBuilder(1);
    falseBuilder.writeBoolean(false);
    ConstantColumnTransformer no = new ConstantColumnTransformer(BOOLEAN, falseBuilder.build());
    leaves.add(yes);
    leaves.add(no);
    ColumnTransformer flags =
        new TableCaseWhenThenColumnTransformer(
            BOOLEAN,
            Collections.singletonList(reference(greater(input, constant(leaves, 0)))),
            Collections.singletonList(reference(yes)),
            reference(no));
    ColumnTransformer diff = new DiffColumnTransformer(DOUBLE, reference(input), reference(flags));
    reference(diff);
    ColumnTransformer[] nodes =
        ColumnTransformer.collectTransformers(Collections.singletonList(diff));
    TsBlock[] blocks = {block(1.0, 3.0, null, 6.0), block(10.0, null, 14.0), block(20.0, 21.0)};
    Double[][] expected = {{null, 2.0, null, null}, {4.0, null, null}, {6.0, 1.0}};
    for (int i = 0; i < blocks.length; i++) {
      for (ColumnTransformer node : nodes) {
        node.clearCacheForNewInput();
      }
      for (LeafColumnTransformer leaf : leaves) {
        leaf.initFromTsBlock(blocks[i]);
      }
      diff.evaluateWithSelection(masks(blocks[i].getPositionCount())[2]);
      assertValues(diff.getColumn(), expected[i]);
      diff.clearCache();
      diff.evaluateWithSelection(masks(blocks[i].getPositionCount())[2]);
      assertValues(diff.getColumn(), expected[i]);
    }
    diff.close();
  }

  private static void assertCaseFilterAndCommonProjection(boolean dynamic) throws Exception {
    List<LeafColumnTransformer> filterLeaves = new ArrayList<>();
    ColumnTransformer input = input(filterLeaves, DOUBLE, 0);
    ColumnTransformer flags = dynamic ? input(filterLeaves, BOOLEAN, 1) : null;
    ColumnTransformer diff = newDiff(input, flags);
    ColumnTransformer predicate =
        greater(
            caseWhen(
                greater(diff, constant(filterLeaves, 1)),
                constant(filterLeaves, 1),
                constant(filterLeaves, 0)),
            constant(filterLeaves, 0));
    IdentityColumnTransformer output = new IdentityColumnTransformer(DOUBLE, 2);
    try (Operator operator =
        new FilterAndProjectOperator(
            new TestContext(),
            new BlocksOperator(
                block(new Double[] {1.0, 3.0, 6.0}, new Boolean[] {true, false, true}),
                block(new Double[] {10.0, 11.0}, new Boolean[] {false, true})),
            Arrays.asList(TSDataType.DOUBLE, TSDataType.BOOLEAN, TSDataType.DOUBLE),
            filterLeaves,
            reference(predicate),
            Collections.singletonList(reference(diff)),
            Collections.singletonList(output),
            Collections.singletonList(reference(output)),
            false,
            true)) {
      assertValues(operator.next().getColumn(0), 2.0, 3.0);
      assertValues(operator.next().getColumn(0), 4.0);
    }
  }

  private static Operator projection(
      List<LeafColumnTransformer> leaves, List<ColumnTransformer> outputs, TsBlock... blocks) {
    List<ColumnTransformer> roots = new ArrayList<>();
    for (ColumnTransformer output : outputs) {
      roots.add(reference(output));
    }
    return new FilterAndProjectOperator(
        new TestContext(),
        new BlocksOperator(blocks),
        Collections.emptyList(),
        Collections.emptyList(),
        null,
        Collections.emptyList(),
        leaves,
        roots,
        false,
        false);
  }

  private static Fixture unary(boolean ignoreNull) {
    return new Fixture(false, ignoreNull);
  }

  private static Fixture binary() {
    return new Fixture(true, true);
  }

  private static ColumnTransformer newDiff(ColumnTransformer input, ColumnTransformer flags) {
    return flags == null
        ? new DiffFunctionColumnTransformer(DOUBLE, reference(input), true)
        : new DiffColumnTransformer(DOUBLE, reference(input), reference(flags));
  }

  private static ColumnTransformer reference(ColumnTransformer transformer) {
    transformer.addReferenceCount();
    return transformer;
  }

  private static ColumnTransformer input(
      List<LeafColumnTransformer> leaves, org.apache.tsfile.read.common.type.Type type, int index) {
    IdentityColumnTransformer input = new IdentityColumnTransformer(type, index);
    leaves.add(input);
    return input;
  }

  private static ColumnTransformer constant(List<LeafColumnTransformer> leaves, double value) {
    ConstantColumnTransformer constant =
        new ConstantColumnTransformer(DOUBLE, block(value).getColumn(0));
    leaves.add(constant);
    return constant;
  }

  private static ColumnTransformer greater(ColumnTransformer left, ColumnTransformer right) {
    return new CompareGreaterThanColumnTransformer(BOOLEAN, reference(left), reference(right));
  }

  private static ColumnTransformer caseWhen(
      ColumnTransformer when, ColumnTransformer then, ColumnTransformer otherwise) {
    return new TableCaseWhenThenColumnTransformer(
        DOUBLE,
        Collections.singletonList(reference(when)),
        Collections.singletonList(reference(then)),
        reference(otherwise));
  }

  private static boolean[][] masks(int size) {
    boolean[] partial = new boolean[size];
    boolean[] all = new boolean[size];
    for (int i = 0; i < size; i++) {
      partial[i] = (i & 1) == 1;
      all[i] = true;
    }
    return new boolean[][] {new boolean[size], partial, all};
  }

  private static TsBlock block(Double... values) {
    return block(values, null);
  }

  private static TsBlock block(Double[] values, Boolean[] flags) {
    ColumnBuilder numbers = DOUBLE.createColumnBuilder(values.length);
    ColumnBuilder booleans = BOOLEAN.createColumnBuilder(values.length);
    for (int i = 0; i < values.length; i++) {
      if (values[i] == null) {
        numbers.appendNull();
      } else {
        numbers.writeDouble(values[i]);
      }
      if (flags == null || flags[i] == null) {
        booleans.appendNull();
      } else {
        booleans.writeBoolean(flags[i]);
      }
    }
    return new TsBlock(
        new TimeColumn(values.length, new long[values.length]), numbers.build(), booleans.build());
  }

  private static void assertValues(Column column, Double... expected) {
    assertEquals(expected.length, column.getPositionCount());
    for (int i = 0; i < expected.length; i++) {
      if (expected[i] == null) {
        assertTrue(column.isNull(i));
      } else {
        assertFalse(column.isNull(i));
        assertEquals(expected[i], column.getDouble(i), 0.0);
      }
    }
  }

  private static class Fixture {
    final IdentityColumnTransformer input = new IdentityColumnTransformer(DOUBLE, 0);
    final IdentityColumnTransformer flags;
    final ColumnTransformer diff;
    final ColumnTransformer[] nodes;

    Fixture(boolean dynamic, boolean ignoreNull) {
      flags = dynamic ? new IdentityColumnTransformer(BOOLEAN, 1) : null;
      diff =
          dynamic
              ? new DiffColumnTransformer(DOUBLE, reference(input), reference(flags))
              : new DiffFunctionColumnTransformer(DOUBLE, reference(input), ignoreNull);
      reference(diff);
      nodes = ColumnTransformer.collectTransformers(Collections.singletonList(diff));
    }

    void init(TsBlock block) {
      for (ColumnTransformer node : nodes) {
        node.clearCacheForNewInput();
      }
      input.initFromTsBlock(block);
      if (flags != null) {
        flags.initFromTsBlock(block);
      }
    }

    Column full() {
      diff.tryEvaluate();
      return diff.getColumn();
    }

    Column selected(boolean[] mask) {
      diff.evaluateWithSelection(mask);
      return diff.getColumn();
    }
  }

  private static class TestContext extends CommonOperatorContext {
    TestContext() {
      super(0, new PlanNodeId("diff-cache-lifecycle"), "diff-cache-lifecycle");
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

  private static class BlocksOperator implements Operator {
    private final TsBlock[] blocks;
    private final CommonOperatorContext context = new TestContext();
    private int position;

    BlocksOperator(TsBlock... blocks) {
      this.blocks = blocks;
    }

    @Override
    public CommonOperatorContext getOperatorContext() {
      return context;
    }

    @Override
    public TsBlock next() {
      return blocks[position++];
    }

    @Override
    public boolean hasNext() {
      return position < blocks.length;
    }

    @Override
    public boolean isFinished() {
      return position >= blocks.length;
    }

    @Override
    public void close() {}

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
  }
}
