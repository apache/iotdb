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

import org.apache.iotdb.db.queryengine.plan.relational.planner.rowpattern.IrPatternAlternationOptimizer;
import org.apache.iotdb.db.queryengine.plan.relational.planner.rowpattern.IrRowPattern;
import org.apache.iotdb.db.queryengine.plan.relational.planner.rowpattern.IrRowPatternFlattener;

import org.junit.Assert;
import org.junit.Test;

import java.util.Optional;

import static org.apache.iotdb.db.queryengine.plan.relational.planner.rowpattern.Patterns.alternation;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.rowpattern.Patterns.concatenation;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.rowpattern.Patterns.empty;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.rowpattern.Patterns.excluded;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.rowpattern.Patterns.label;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.rowpattern.Patterns.permutation;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.rowpattern.Patterns.plusQuantified;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.rowpattern.Patterns.questionMarkQuantified;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.rowpattern.Patterns.rangeQuantified;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.rowpattern.Patterns.starQuantified;

public class IrRowPatternOptimizationTest {
  @Test
  public void testFlattenAlternation() {
    assertFlattened(
        alternation(label("A"), alternation(label("B"), label("C"))),
        alternation(label("A"), label("B"), label("C")));

    assertFlattened(
        alternation(
            alternation(
                label("A"),
                alternation(
                    alternation(label("B"), label("C")), alternation(label("D"), label("E")))),
            alternation(label("F"), label("G"))),
        alternation(
            label("A"), label("B"), label("C"), label("D"), label("E"), label("F"), label("G")));
  }

  @Test
  public void testOptimizeAlternation() {
    assertOptimized(
        alternation(alternation(label("A"), empty()), label("B")),
        alternation(questionMarkQuantified(label("A"), true), label("B")));

    assertOptimized(
        alternation(alternation(empty(), label("A")), label("B")),
        alternation(questionMarkQuantified(label("A"), false), label("B")));
  }

  @Test
  public void testFlattenAndOptimizeAlternation() {
    assertFlattenedOptimized(
        alternation(alternation(label("A"), label("B")), label("C")),
        alternation(label("A"), label("B"), label("C")));

    assertFlattenedOptimized(
        alternation(alternation(label("A"), label("B")), empty()),
        alternation(label("A"), questionMarkQuantified(label("B"), true)));

    assertFlattenedOptimized(
        alternation(alternation(empty(), label("A")), empty()),
        questionMarkQuantified(label("A"), false));

    assertFlattenedOptimized(alternation(empty(), empty()), empty());

    assertFlattenedOptimized(
        alternation(
            alternation(
                label("A"),
                alternation(
                    alternation(empty(), alternation(alternation(label("B"), empty()), label("C"))),
                    empty())),
            label("D")),
        alternation(questionMarkQuantified(label("A"), true), label("B"), label("C"), label("D")));
  }

  @Test
  public void testFlattenConcatenation() {
    assertFlattened(
        concatenation(concatenation(label("A"), label("B")), label("C")),
        concatenation(label("A"), label("B"), label("C")));

    assertFlattened(
        concatenation(concatenation(concatenation(label("A"), label("B")), empty()), label("C")),
        concatenation(label("A"), label("B"), label("C")));

    assertFlattened(
        concatenation(concatenation(concatenation(empty(), label("A")), label("B")), label("C")),
        concatenation(label("A"), label("B"), label("C")));

    assertFlattened(
        concatenation(
            concatenation(
                concatenation(
                    concatenation(
                        concatenation(concatenation(empty(), label("A")), empty()), label("B")),
                    empty()),
                label("C")),
            empty()),
        concatenation(label("A"), label("B"), label("C")));

    assertFlattened(concatenation(empty(), label("A")), label("A"));
    assertFlattened(concatenation(label("A"), empty()), label("A"));
    assertFlattened(concatenation(concatenation(empty(), empty()), empty()), empty());

    assertFlattened(
        concatenation(label("A"), concatenation(label("B"), label("C"))),
        concatenation(label("A"), label("B"), label("C")));

    assertFlattened(
        concatenation(
            concatenation(
                label("A"),
                concatenation(
                    concatenation(concatenation(label("B"), label("C")), label("D")), label("E"))),
            concatenation(label("F"), label("G"))),
        concatenation(
            label("A"), label("B"), label("C"), label("D"), label("E"), label("F"), label("G")));
  }

  @Test
  public void testFlattenPermutation() {
    assertFlattened(
        permutation(label("A"), label("B"), label("C")),
        permutation(label("A"), label("B"), label("C")));

    assertFlattened(
        permutation(label("A"), label("B"), empty()), permutation(label("A"), label("B")));

    assertFlattened(
        permutation(empty(), label("A"), empty(), label("B"), empty(), label("C"), empty()),
        permutation(label("A"), label("B"), label("C")));

    assertFlattened(permutation(empty(), label("A")), label("A"));
    assertFlattened(permutation(label("A"), empty()), label("A"));
    assertFlattened(permutation(empty(), empty(), empty()), empty());
  }

  @Test
  public void testFlattenAndOptimize() {
    assertFlattenedOptimized(
        alternation(
            concatenation(empty(), alternation(label("A"), label("B"))),
            alternation(
                concatenation(concatenation(empty(), empty()), empty()),
                alternation(label("C"), empty()))),
        alternation(label("A"), questionMarkQuantified(label("B"), true), label("C")));
  }

  @Test
  public void testRemoveNestedExclusions() {
    assertFlattened(excluded(excluded(excluded(excluded(label("A"))))), excluded(label("A")));
  }

  @Test
  public void testEmptyPattern() {
    assertFlattened(empty(), empty());
    assertOptimized(empty(), empty());
  }

  @Test
  public void testFlattenQuantifiedEmptyPattern() {
    assertFlattened(starQuantified(empty(), true), empty());
    assertFlattened(plusQuantified(empty(), true), empty());
    assertFlattened(
        questionMarkQuantified(starQuantified(plusQuantified(empty(), true), true), true), empty());

    assertFlattened(rangeQuantified(empty(), 1, Optional.of(2), true), empty());
    assertFlattened(rangeQuantified(empty(), 0, Optional.empty(), false), empty());
  }

  private void assertFlattened(IrRowPattern pattern, IrRowPattern expected) {
    IrRowPattern flattened = IrRowPatternFlattener.optimize(pattern);
    Assert.assertEquals(expected, flattened);
  }

  private void assertOptimized(IrRowPattern pattern, IrRowPattern expected) {
    IrRowPattern optimized = IrPatternAlternationOptimizer.optimize(pattern);
    Assert.assertEquals(expected, optimized);
  }

  private void assertFlattenedOptimized(IrRowPattern pattern, IrRowPattern expected) {
    IrRowPattern flattened = IrRowPatternFlattener.optimize(pattern);
    IrRowPattern optimized = IrPatternAlternationOptimizer.optimize(flattened);
    Assert.assertEquals(expected, optimized);
  }
}
