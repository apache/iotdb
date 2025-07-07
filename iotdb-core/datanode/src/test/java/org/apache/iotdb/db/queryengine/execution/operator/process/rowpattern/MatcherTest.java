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

import org.apache.iotdb.db.queryengine.execution.operator.process.rowpattern.matcher.ArrayView;
import org.apache.iotdb.db.queryengine.execution.operator.process.rowpattern.matcher.IrRowPatternToProgramRewriter;
import org.apache.iotdb.db.queryengine.execution.operator.process.rowpattern.matcher.MatchResult;
import org.apache.iotdb.db.queryengine.execution.operator.process.rowpattern.matcher.Matcher;
import org.apache.iotdb.db.queryengine.execution.operator.process.rowpattern.matcher.Program;
import org.apache.iotdb.db.queryengine.execution.operator.process.window.partition.Partition;
import org.apache.iotdb.db.queryengine.plan.relational.planner.rowpattern.IrLabel;
import org.apache.iotdb.db.queryengine.plan.relational.planner.rowpattern.IrRowPattern;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.read.common.block.TsBlock;
import org.junit.Test;

import java.util.Map;

import static org.apache.iotdb.db.queryengine.plan.relational.planner.rowpattern.Patterns.alternation;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.rowpattern.Patterns.concatenation;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.rowpattern.Patterns.end;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.rowpattern.Patterns.excluded;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.rowpattern.Patterns.label;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.rowpattern.Patterns.permutation;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.rowpattern.Patterns.questionMarkQuantified;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.rowpattern.Patterns.starQuantified;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.rowpattern.Patterns.start;
import static org.mockito.Mockito.mock;

public class MatcherTest {
  private static final Map<IrLabel, Integer> LABEL_MAPPING =
      ImmutableMap.of(
          new IrLabel("A"), 0,
          new IrLabel("B"), 1,
          new IrLabel("C"), 2,
          new IrLabel("D"), 3,
          new IrLabel("E"), 4);

  @Test
  public void testLabels() {
    checkMatch(concatenation(label("A"), label("B")), "ABCD", new char[] {'A', 'B'});
    checkMatch(
        concatenation(starQuantified(label("A"), true), label("B")),
        "AABCD",
        new char[] {'A', 'A', 'B'});
    checkMatch(
        concatenation(starQuantified(label("A"), true), label("B")), "BCD", new char[] {'B'});
    checkMatch(concatenation(start(), label("A"), label("B")), "ABCD", new char[] {'A', 'B'});
    checkMatch(concatenation(label("A"), label("B"), end()), "AB", new char[] {'A', 'B'});
    checkMatch(concatenation(excluded(label("A")), label("B")), "ABCD", new char[] {'A', 'B'});
    checkMatch(
        alternation(concatenation(label("A"), label("B")), concatenation(label("B"), label("C"))),
        "ABCD",
        new char[] {'A', 'B'});
    checkMatch(permutation(label("A"), label("B"), label("C")), "ABCD", new char[] {'A', 'B', 'C'});
    checkMatch(
        concatenation(label("A"), questionMarkQuantified(label("B"), true)),
        "ABCD",
        new char[] {'A', 'B'});
    checkMatch(
        concatenation(label("A"), questionMarkQuantified(label("B"), false)),
        "ABCD",
        new char[] {'A'});
  }

  private void checkMatch(IrRowPattern pattern, String input, char[] expectedLabels) {
    MatchResult result = match(pattern, input);
    if (!result.isMatched()) {
      throw new AssertionError("Pattern did not match.");
    }

    int[] mappedExpected = new int[expectedLabels.length];
    for (int i = 0; i < expectedLabels.length; i++) {
      mappedExpected[i] = LABEL_MAPPING.get(new IrLabel(String.valueOf(expectedLabels[i])));
    }

    int[] actualLabels = result.getLabels().toArray();
    if (!java.util.Arrays.equals(actualLabels, mappedExpected)) {
      throw new AssertionError(
          "Expected labels: "
              + java.util.Arrays.toString(mappedExpected)
              + ", but got: "
              + java.util.Arrays.toString(actualLabels));
    }
  }

  @Test
  public void testExclusionCaptures() {
    checkMatch(concatenation(excluded(label("A")), label("B")), "ABCD", new int[] {0, 1});
    checkMatch(excluded(concatenation(label("A"), label("B"))), "ABCD", new int[] {0, 2});
    checkMatch(concatenation(label("A"), excluded(label("B"))), "ABCD", new int[] {1, 2});
    checkMatch(
        concatenation(label("A"), starQuantified(excluded(label("B")), true)),
        "ABBBCD",
        new int[] {1, 2, 2, 3, 3, 4});
    checkMatch(
        concatenation(label("A"), excluded(starQuantified(label("B"), true))),
        "ABBBCD",
        new int[] {1, 4});
    checkMatch(
        concatenation(
            label("A"),
            starQuantified(excluded(label("B")), true),
            label("C"),
            excluded(starQuantified(label("D"), true))),
        "ABBCDDDE",
        new int[] {1, 2, 2, 3, 4, 7});
    checkMatch(
        concatenation(
            label("A"),
            starQuantified(
                concatenation(
                    excluded(concatenation(label("B"), label("C"))),
                    label("D"),
                    excluded(label("E"))),
                true)),
        "ABCDEBCDEBCDE",
        new int[] {1, 3, 4, 5, 5, 7, 8, 9, 9, 11, 12, 13});
  }

  private void checkMatch(IrRowPattern pattern, String input, int[] expectedCaptures) {
    MatchResult result = match(pattern, input);
    if (!result.isMatched()) {
      throw new AssertionError("Pattern did not match.");
    }

    int[] actualCaptures = result.getExclusions().toArray();
    if (!java.util.Arrays.equals(actualCaptures, expectedCaptures)) {
      throw new AssertionError(
          "Expected captures: "
              + java.util.Arrays.toString(expectedCaptures)
              + ", but got: "
              + java.util.Arrays.toString(actualCaptures));
    }
  }

  private static MatchResult match(IrRowPattern pattern, String input) {
    Program program = IrRowPatternToProgramRewriter.rewrite(pattern, LABEL_MAPPING);

    Matcher matcher = new Matcher(program, ImmutableList.of());

    int[] mappedInput = new int[input.length()];
    for (int i = 0; i < input.length(); i++) {
      mappedInput[i] = LABEL_MAPPING.get(new IrLabel(String.valueOf(input.charAt(i))));
    }

    return matcher.run(new testPatternVariableRecognizer(mappedInput));
  }

  private static class testPatternVariableRecognizer extends PatternVariableRecognizer {
    private final int[] input;

    public testPatternVariableRecognizer(int[] input) {
      super(0, 0, 0, 0, 1, ImmutableList.of(), createDummyPartition());
      this.input = input;
    }

    @Override
    public int getInputLength() {
      return input.length;
    }

    @Override
    public boolean isMatchingAtPartitionStart() {
      return true;
    }

    /**
     * Determines whether identifying the current row as `label` is valid, where the `label` is the
     * last pattern variable in matchedLabels.
     *
     * <p>In the Test, each row has an explicit label, i.e., input[position]. Therefore, it only
     * needs to check whether input[position] equals the label. In the actual matching process of
     * `evaluateLabel`, it is necessary to determine whether the current row can be recognized as
     * the label based on the definition of the label in the DEFINE clause.
     */
    @Override
    public boolean evaluateLabel(ArrayView matchedLabels, PatternAggregator[] patternAggregators) {
      int position = matchedLabels.length() - 1;
      return input[position] == matchedLabels.get(position);
    }

    private static Partition createDummyPartition() {
      Column dummyColumn = mock(Column.class);
      TsBlock dummyBlock = new TsBlock(1, dummyColumn);
      return new Partition(ImmutableList.of(dummyBlock), 0, 1);
    }
  }
}
