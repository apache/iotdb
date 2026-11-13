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

package org.apache.iotdb.library;

import org.apache.iotdb.library.match.PatternExecutor;
import org.apache.iotdb.library.match.UDAFDTWMatch;
import org.apache.iotdb.library.match.UDAFPatternMatch;
import org.apache.iotdb.library.match.model.DTWMatchResult;
import org.apache.iotdb.library.match.model.DTWState;
import org.apache.iotdb.library.match.model.PatternContext;
import org.apache.iotdb.library.match.model.PatternResult;
import org.apache.iotdb.library.match.model.PatternState;
import org.apache.iotdb.library.match.model.Point;
import org.apache.iotdb.udf.api.State;
import org.apache.iotdb.udf.api.customizer.config.UDAFConfigurations;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.udf.api.exception.UDFAttributeNotProvidedException;
import org.apache.iotdb.udf.api.exception.UDFInputSeriesNumberNotValidException;
import org.apache.iotdb.udf.api.exception.UDFParameterNotValidException;
import org.apache.iotdb.udf.api.type.Type;
import org.apache.iotdb.udf.api.utils.ResultValue;

import org.apache.commons.lang3.StringUtils;
import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnEncoding;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.column.BinaryColumnBuilder;
import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class UDAFPatternTest {
  private final PatternExecutor executor = new PatternExecutor();

  @Test
  public void testPatternExecutor() {
    List<Point> sourcePoints = new ArrayList<>();
    List<Point> queryPoints = new ArrayList<>();

    try (InputStream input = getClass().getClassLoader().getResourceAsStream("patternData")) {
      try (BufferedReader reader = new BufferedReader(new InputStreamReader(input))) {
        String line;
        while ((line = reader.readLine()) != null) {
          if (line.startsWith("#") || StringUtils.isEmpty(line)) {
            continue;
          }
          sourcePoints.add(
              new Point(
                  Double.parseDouble(line.split(",")[0]), Double.parseDouble(line.split(",")[1])));
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    try (InputStream input = getClass().getClassLoader().getResourceAsStream("patternPart")) {
      try (BufferedReader reader = new BufferedReader(new InputStreamReader(input))) {
        String line;
        while ((line = reader.readLine()) != null) {
          if (line.startsWith("#") || StringUtils.isEmpty(line)) {
            continue;
          }
          queryPoints.add(
              new Point(
                  Double.parseDouble(line.split(",")[0]), Double.parseDouble(line.split(",")[1])));
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    List<Point> sourcePointsExtract = executor.scalePoint(sourcePoints);

    List<Point> queryPointsExtract = executor.extractPoints(queryPoints);

    executor.setPoints(queryPointsExtract);
    PatternContext ctx = new PatternContext();
    ctx.setDataPoints(sourcePointsExtract);
    List<PatternResult> results = executor.executeQuery(ctx);
    Assert.assertNotNull(results);
    Assert.assertEquals(1, results.size());
  }

  @Test
  public void testParameterValidator() {
    UDAFPatternMatch patternMatch = new UDAFPatternMatch();
    List<String> stringList = new ArrayList<>();
    List<Type> typeList = new ArrayList<>();
    Map<String, String> userAttributes = new HashMap<>();
    userAttributes.put("timePattern", "1,2,3");
    userAttributes.put("valuePattern", "1.0,2.0");
    userAttributes.put("threshold", "100");

    UDFParameterValidator validator =
        new UDFParameterValidator(new UDFParameters(stringList, typeList, userAttributes));

    Assert.assertThrows(
        UDFInputSeriesNumberNotValidException.class, () -> patternMatch.validate(validator));

    stringList.add("s1");
    typeList.add(Type.FLOAT);
    userAttributes.clear();
    Assert.assertThrows(
        "Illegal parameter, timePattern must be long,long...",
        UDFParameterNotValidException.class,
        () -> patternMatch.validate(validator));

    userAttributes.put("timePattern", "1,3,2");
    Assert.assertThrows(
        "Illegal parameter, valuePattern must be double,double...",
        UDFParameterNotValidException.class,
        () -> patternMatch.validate(validator));

    userAttributes.put("valuePattern", "1.0,2.0");
    Assert.assertThrows(
        "Illegal parameter, timePattern size must equals valuePattern size",
        UDFParameterNotValidException.class,
        () -> patternMatch.validate(validator));

    userAttributes.remove("valuePattern");
    userAttributes.put("valuePattern", "1.0,2.0,3.0");

    Assert.assertThrows(
        "Illegal parameter, timePattern value must be in ascending order.",
        UDFParameterNotValidException.class,
        () -> patternMatch.validate(validator));

    userAttributes.remove("timePattern");
    userAttributes.put("timePattern", "1,2,3");

    Assert.assertThrows(
        "attribute threshold is required but was not provided.",
        UDFAttributeNotProvidedException.class,
        () -> patternMatch.validate(validator));

    userAttributes.put("threshold", "100");

    try {
      patternMatch.validate(validator);
    } catch (Exception e) {
      Assert.fail("Should not throw exception");
    }

    userAttributes.put("threshold", "Infinity");
    Assert.assertThrows(
        "Illegal parameter, threshold must be a finite non-negative number.",
        UDFParameterNotValidException.class,
        () -> patternMatch.validate(validator));

    userAttributes.put("threshold", "100");
    userAttributes.put("valuePattern", "1.0,NaN,3.0");
    Assert.assertThrows(
        "Illegal parameter, valuePattern values must be finite.",
        UDFParameterNotValidException.class,
        () -> patternMatch.validate(validator));
  }

  @Test
  public void testDTWMatchValidatesFiniteParameters() {
    Map<String, String> attributes = new HashMap<>();
    attributes.put("pattern", "1.0,NaN");
    attributes.put("threshold", "100");

    Assert.assertThrows(
        UDFParameterNotValidException.class,
        () ->
            new UDAFDTWMatch()
                .validate(
                    new UDFParameterValidator(
                        new UDFParameters(
                            Collections.singletonList("s1"),
                            Collections.singletonList(Type.DOUBLE),
                            attributes))));

    attributes.put("pattern", "1.0,2.0");
    attributes.put("threshold", "Infinity");
    Assert.assertThrows(
        UDFParameterNotValidException.class,
        () ->
            new UDAFDTWMatch()
                .validate(
                    new UDFParameterValidator(
                        new UDFParameters(
                            Collections.singletonList("s1"),
                            Collections.singletonList(Type.DOUBLE),
                            attributes))));
  }

  @Test
  public void testDTWMatchUsesSinglePointDistance() throws Exception {
    UDAFDTWMatch dtwMatch = new UDAFDTWMatch();
    UDFParameters parameters = createDtwParameters("10.0", "1");
    dtwMatch.validate(new UDFParameterValidator(parameters));
    dtwMatch.beforeStart(parameters, new UDAFConfigurations());
    DTWState state = (DTWState) dtwMatch.createState();
    state.reset();

    Column[] columns =
        new Column[] {
          new TestColumn(TSDataType.DOUBLE, new long[0], new double[] {1.0}, new boolean[] {false}),
          new TestColumn(TSDataType.INT64, new long[] {1L}, new double[0], new boolean[] {false})
        };
    dtwMatch.addInput(state, columns, null);

    Assert.assertTrue(state.getMatchResults().isEmpty());

    UDAFDTWMatch exactMatch = new UDAFDTWMatch();
    UDFParameters exactParameters = createDtwParameters("1.0", "0");
    exactMatch.validate(new UDFParameterValidator(exactParameters));
    exactMatch.beforeStart(exactParameters, new UDAFConfigurations());
    DTWState exactState = (DTWState) exactMatch.createState();
    exactState.reset();

    exactMatch.addInput(exactState, columns, null);

    Assert.assertEquals(1, exactState.getMatchResults().size());
  }

  @Test
  public void testDTWMatchRejectsEmptyPatternElements() {
    for (String pattern : new String[] {"", ",", ",,", "1.0,", ",1.0", "1.0,,2.0"}) {
      Assert.assertThrows(
          pattern,
          UDFParameterNotValidException.class,
          () ->
              new UDAFDTWMatch()
                  .validate(new UDFParameterValidator(createDtwParameters(pattern, "1"))));
    }
  }

  @Test
  public void testMatchUDAFsSkipNullAndInvalidRows() throws Exception {
    Column[] columns = buildPatternInputColumns();

    UDAFPatternMatch patternMatch = new UDAFPatternMatch();
    UDFParameters patternParameters = createPatternParameters();
    patternMatch.validate(new UDFParameterValidator(patternParameters));
    patternMatch.beforeStart(patternParameters, new UDAFConfigurations());
    PatternState patternState = (PatternState) patternMatch.createState();
    patternState.reset();
    patternMatch.addInput(patternState, columns, null);

    Assert.assertEquals(Arrays.asList(1L, 5L), patternState.getTimeBuffer());
    Assert.assertEquals(Arrays.asList(1.0, 3.0), patternState.getValueBuffer());

    UDAFDTWMatch dtwMatch = new UDAFDTWMatch();
    UDFParameters dtwParameters = createDtwParameters();
    dtwMatch.validate(new UDFParameterValidator(dtwParameters));
    dtwMatch.beforeStart(dtwParameters, new UDAFConfigurations());
    DTWState dtwState = (DTWState) dtwMatch.createState();
    dtwState.reset();
    dtwMatch.addInput(dtwState, columns, null);

    Assert.assertArrayEquals(new Long[] {1L, 5L}, dtwState.getTimeBuffer());
    Assert.assertArrayEquals(new Double[] {1.0, 3.0}, dtwState.getValueBuffer());
  }

  @Test
  public void testPatternMatchEmptyEffectiveInputOutputsNull() throws Exception {
    UDAFPatternMatch patternMatch = new UDAFPatternMatch();
    UDFParameters parameters = createPatternParameters();
    patternMatch.validate(new UDFParameterValidator(parameters));
    patternMatch.beforeStart(parameters, new UDAFConfigurations());
    State state = patternMatch.createState();
    state.reset();

    BinaryColumnBuilder resultBuilder = new BinaryColumnBuilder(null, 1);
    patternMatch.outputFinal(state, new ResultValue(resultBuilder));

    Assert.assertEquals(1, resultBuilder.getPositionCount());
    Assert.assertTrue(resultBuilder.build().isNull(0));
  }

  @Test
  public void testPatternMatchFlatInputFallsBackToDtw() throws Exception {
    UDAFPatternMatch patternMatch = new UDAFPatternMatch();
    UDFParameters parameters = createPatternParameters("1,2", "5.0,5.0", "0");
    patternMatch.validate(new UDFParameterValidator(parameters));
    patternMatch.beforeStart(parameters, new UDAFConfigurations());
    PatternState state = (PatternState) patternMatch.createState();
    state.reset();
    state.updateBuffer(1L, 5.0);
    state.updateBuffer(2L, 5.0);

    BinaryColumnBuilder resultBuilder = new BinaryColumnBuilder(null, 1);
    patternMatch.outputFinal(state, new ResultValue(resultBuilder));

    Assert.assertEquals(1, resultBuilder.getPositionCount());
    Assert.assertFalse(resultBuilder.build().isNull(0));
  }

  @Test
  public void testPatternMatchNonMonotonicInputFallsBackToDtw() throws Exception {
    UDAFPatternMatch patternMatch = new UDAFPatternMatch();
    UDFParameters parameters = createPatternParameters("1,2,3", "1.0,2.0,3.0", "0");
    patternMatch.validate(new UDFParameterValidator(parameters));
    patternMatch.beforeStart(parameters, new UDAFConfigurations());
    PatternState state = (PatternState) patternMatch.createState();
    state.reset();
    state.updateBuffer(1L, 1.0);
    state.updateBuffer(3L, 2.0);
    state.updateBuffer(2L, 3.0);

    BinaryColumnBuilder resultBuilder = new BinaryColumnBuilder(null, 1);
    patternMatch.outputFinal(state, new ResultValue(resultBuilder));

    Assert.assertEquals(1, resultBuilder.getPositionCount());
    Assert.assertFalse(resultBuilder.build().isNull(0));
  }

  @Test
  public void testMatchUDAFsCombineEmptyStates() throws Exception {
    UDAFPatternMatch patternMatch = new UDAFPatternMatch();
    UDFParameters patternParameters = createPatternParameters();
    patternMatch.validate(new UDFParameterValidator(patternParameters));
    patternMatch.beforeStart(patternParameters, new UDAFConfigurations());
    PatternState patternTarget = (PatternState) patternMatch.createState();
    patternTarget.reset();
    PatternState patternEmptySource = (PatternState) patternMatch.createState();
    patternEmptySource.reset();

    patternMatch.combineState(patternTarget, patternEmptySource);

    Assert.assertTrue(patternTarget.getTimeBuffer().isEmpty());
    Assert.assertTrue(patternTarget.getValueBuffer().isEmpty());

    PatternState patternSource = (PatternState) patternMatch.createState();
    patternSource.reset();
    patternSource.updateBuffer(1L, 1.0);
    patternMatch.combineState(patternTarget, patternSource);

    Assert.assertEquals(Collections.singletonList(1L), patternTarget.getTimeBuffer());
    Assert.assertEquals(Collections.singletonList(1.0), patternTarget.getValueBuffer());

    UDAFDTWMatch dtwMatch = new UDAFDTWMatch();
    UDFParameters dtwParameters = createDtwParameters();
    dtwMatch.validate(new UDFParameterValidator(dtwParameters));
    dtwMatch.beforeStart(dtwParameters, new UDAFConfigurations());
    DTWState dtwTarget = (DTWState) dtwMatch.createState();
    dtwTarget.reset();
    DTWState dtwEmptySource = (DTWState) dtwMatch.createState();
    dtwEmptySource.reset();

    dtwMatch.combineState(dtwTarget, dtwEmptySource);

    Assert.assertArrayEquals(new Long[0], dtwTarget.getTimeBuffer());
    Assert.assertArrayEquals(new Double[0], dtwTarget.getValueBuffer());

    DTWState dtwSource = (DTWState) dtwMatch.createState();
    dtwSource.reset();
    dtwSource.updateBuffer(1L, 1.0);
    dtwSource.updateBuffer(2L, 3.0);
    dtwMatch.combineState(dtwTarget, dtwSource);
    dtwMatch.outputFinal(dtwTarget, new ResultValue(new BinaryColumnBuilder(null, 1)));

    Assert.assertArrayEquals(new Long[] {1L, 2L}, dtwTarget.getTimeBuffer());
    Assert.assertArrayEquals(new Double[] {1.0, 3.0}, dtwTarget.getValueBuffer());
  }

  @Test
  public void testDTWMatchCombineRetainsHistoricalMatches() throws Exception {
    UDAFDTWMatch match = new UDAFDTWMatch();
    match.beforeStart(createDtwParameters("1", "0"), new UDAFConfigurations());
    DTWState target = dtwState(match, new long[] {1}, new double[] {1});
    DTWState source = dtwState(match, new long[] {2, 3, 4}, new double[] {1, 1, 1});
    byte[] sourceBefore = source.serialize();

    match.combineState(target, source);

    assertDtwMatches(match, target, new long[][] {{1, 1}, {2, 2}, {3, 3}, {4, 4}});
    Assert.assertArrayEquals(sourceBefore, source.serialize());
  }

  @Test
  public void testDTWMatchCombineIncludesBoundaryAndSourceMatches() throws Exception {
    UDAFDTWMatch match = new UDAFDTWMatch();
    match.beforeStart(createDtwParameters("1,2,3", "0"), new UDAFConfigurations());
    DTWState target = dtwState(match, new long[] {1}, new double[] {1});
    DTWState source = dtwState(match, new long[] {2, 3, 4, 5, 6}, new double[] {2, 3, 1, 2, 3});

    match.combineState(target, source);

    assertDtwMatches(match, target, new long[][] {{1, 3}, {4, 6}});
    Assert.assertArrayEquals(new Long[] {4L, 5L, 6L}, target.getTimeBuffer());
  }

  @Test
  public void testDTWMatchCombineDoesNotBridgeDiscardedSourcePrefix() throws Exception {
    UDAFDTWMatch match = new UDAFDTWMatch();
    match.beforeStart(createDtwParameters("1,2", "0"), new UDAFConfigurations());
    DTWState target = dtwState(match, new long[] {1}, new double[] {1});
    DTWState source = dtwState(match, new long[] {2, 3, 4, 5}, new double[] {9, 9, 2, 9});

    match.combineState(target, source);

    assertDtwMatches(match, target, new long[0][]);
  }

  @Test
  public void testDTWMatchCombineInterleavedStatesInEitherOrder() throws Exception {
    for (boolean reverse : new boolean[] {false, true}) {
      UDAFDTWMatch match = new UDAFDTWMatch();
      match.beforeStart(createDtwParameters("1,2,3", "0"), new UDAFConfigurations());
      DTWState odd = dtwState(match, new long[] {1, 3, 5}, new double[] {1, 3, 2});
      DTWState even = dtwState(match, new long[] {2, 4, 6}, new double[] {2, 1, 3});
      DTWState target = reverse ? even : odd;
      DTWState source = reverse ? odd : even;

      match.combineState(target, source);

      assertDtwMatches(match, target, new long[][] {{1, 3}, {4, 6}});
    }
  }

  @Test
  public void testDTWMatchCombineGroupingAndContinuation() throws Exception {
    for (boolean combineRightFirst : new boolean[] {false, true}) {
      UDAFDTWMatch match = new UDAFDTWMatch();
      match.beforeStart(createDtwParameters("1,2,3", "0"), new UDAFConfigurations());
      DTWState first = dtwState(match, new long[] {1, 2}, new double[] {1, 2});
      DTWState second = dtwState(match, new long[] {3, 4}, new double[] {3, 1});
      DTWState third = dtwState(match, new long[] {5, 6}, new double[] {2, 3});
      if (combineRightFirst) {
        match.combineState(second, third);
        match.combineState(first, second);
      } else {
        match.combineState(first, second);
        match.combineState(first, third);
      }
      assertDtwMatches(match, first, new long[][] {{1, 3}, {4, 6}});

      addDtwInput(match, first, new long[] {7, 8, 9}, new double[] {1, 2, 3});

      assertDtwMatches(match, first, new long[][] {{1, 3}, {4, 6}, {7, 9}});
    }
  }

  @Test
  public void testDTWStateRoundTripRetainsInputForMergeAndReplacesOldState() throws Exception {
    UDAFDTWMatch match = new UDAFDTWMatch();
    match.beforeStart(createDtwParameters("1,2,3", "0"), new UDAFConfigurations());
    DTWState source = dtwState(match, new long[] {2, 3, 4, 5, 6}, new double[] {2, 3, 1, 2, 3});
    DTWState restored = (DTWState) match.createState();
    restored.deserialize(source.serialize());
    Assert.assertArrayEquals(source.serialize(), restored.serialize());
    restored.deserialize(source.serialize());
    Assert.assertArrayEquals(source.serialize(), restored.serialize());

    DTWState target = dtwState(match, new long[] {1}, new double[] {1});
    match.combineState(target, restored);
    assertDtwMatches(match, target, new long[][] {{1, 3}, {4, 6}});

    DTWState empty = dtwState(match, new long[0], new double[0]);
    restored.deserialize(empty.serialize());
    Assert.assertArrayEquals(empty.serialize(), restored.serialize());
    assertDtwMatches(match, restored, new long[0][]);
  }

  @Test
  public void testDTWMergedStateRoundTripAndContinuationBeforeOutput() throws Exception {
    UDAFDTWMatch match = new UDAFDTWMatch();
    match.beforeStart(createDtwParameters("1,2,3", "0"), new UDAFConfigurations());
    DTWState target = dtwState(match, new long[0], new double[0]);
    // Merge a sequence of small partitions without observing the intermediate matches.
    for (int i : new int[] {8, 6, 4, 2, 7, 5, 3, 1}) {
      DTWState source = dtwState(match, new long[] {i}, new double[] {(i - 1) % 3 + 1});
      byte[] originalSource = source.serialize();
      match.combineState(target, source);
      Assert.assertArrayEquals(originalSource, source.serialize());
      DTWState restored = (DTWState) match.createState();
      restored.deserialize(target.serialize());
      Assert.assertArrayEquals(target.serialize(), restored.serialize());
      target = restored;
    }
    addDtwInput(match, target, new long[] {9}, new double[] {3});

    assertDtwMatches(match, target, new long[][] {{1, 3}, {4, 6}, {7, 9}});
    // Observing the same state again must not duplicate matches.
    assertDtwMatches(match, target, new long[][] {{1, 3}, {4, 6}, {7, 9}});
  }

  @Test
  public void testDTWMatchOrdersRawInputAcrossBatches() throws Exception {
    UDAFDTWMatch match = new UDAFDTWMatch();
    match.beforeStart(createDtwParameters("1,2,3", "0"), new UDAFConfigurations());
    DTWState target = dtwState(match, new long[] {6, 5, 4}, new double[] {3, 2, 1});
    addDtwInput(match, target, new long[] {3, 2, 1}, new double[] {3, 2, 1});

    DTWState restored = (DTWState) match.createState();
    restored.deserialize(target.serialize());
    assertDtwMatches(match, target, new long[][] {{1, 3}, {4, 6}});
    assertDtwMatches(match, restored, new long[][] {{1, 3}, {4, 6}});
    assertDtwMatches(match, target, new long[][] {{1, 3}, {4, 6}});
  }

  @Test
  public void testDTWMatchRebuildsAfterLateContinuationAndReplacement() throws Exception {
    UDAFDTWMatch match = new UDAFDTWMatch();
    match.beforeStart(createDtwParameters("1,2,3", "0"), new UDAFConfigurations());
    DTWState target = dtwState(match, new long[] {1, 3}, new double[] {1, 3});
    match.combineState(target, dtwState(match, new long[] {4}, new double[] {9}));
    assertDtwMatches(match, target, new long[0][]);

    addDtwInput(match, target, new long[] {2}, new double[] {2});
    assertDtwMatches(match, target, new long[][] {{1, 3}});

    // Replacing a historical value must also remove a match that is no longer valid.
    addDtwInput(match, target, new long[] {2}, new double[] {9});
    DTWState restored = (DTWState) match.createState();
    restored.deserialize(target.serialize());
    assertDtwMatches(match, target, new long[0][]);
    assertDtwMatches(match, restored, new long[0][]);
  }

  @Test
  public void testDTWMatchRetainsNonzeroDistanceAcrossMerge() throws Exception {
    for (String threshold : new String[] {"1.99", "2"}) {
      UDAFDTWMatch match = new UDAFDTWMatch();
      match.beforeStart(createDtwParameters("0,10", threshold), new UDAFConfigurations());
      DTWState target = dtwState(match, new long[] {1}, new double[] {1});
      DTWState source = dtwState(match, new long[] {2}, new double[] {11});
      match.combineState(target, source);
      BinaryColumnBuilder output = new BinaryColumnBuilder(null, 1);
      match.outputFinal(target, new ResultValue(output));

      Assert.assertEquals(1, output.getPositionCount());
      if ("2".equals(threshold)) {
        Assert.assertFalse(output.build().isNull(0));
        Assert.assertEquals(1, target.getMatchResults().size());
        Assert.assertArrayEquals(
            new DTWMatchResult(2, 1, 2).toByteArray(),
            target.getMatchResults().get(0).toByteArray());
      } else {
        Assert.assertTrue(output.build().isNull(0));
        Assert.assertTrue(target.getMatchResults().isEmpty());
      }
    }
  }

  private static DTWState dtwState(UDAFDTWMatch match, long[] times, double[] values) {
    DTWState state = (DTWState) match.createState();
    state.reset();
    addDtwInput(match, state, times, values);
    return state;
  }

  private static void addDtwInput(
      UDAFDTWMatch match, DTWState state, long[] times, double[] values) {
    match.addInput(
        state,
        new Column[] {
          new TestColumn(TSDataType.DOUBLE, new long[0], values, new boolean[times.length]),
          new TestColumn(TSDataType.INT64, times, new double[0], new boolean[times.length])
        },
        null);
  }

  private static void assertDtwMatches(
      UDAFDTWMatch match, DTWState state, long[][] expectedBounds) {
    BinaryColumnBuilder output = new BinaryColumnBuilder(null, 1);
    match.outputFinal(state, new ResultValue(output));
    Assert.assertEquals(1, output.getPositionCount());
    Assert.assertEquals(expectedBounds.length == 0, output.build().isNull(0));
    Assert.assertEquals(expectedBounds.length, state.getMatchResults().size());
    for (int i = 0; i < expectedBounds.length; i++) {
      Assert.assertArrayEquals(
          new DTWMatchResult(0, expectedBounds[i][0], expectedBounds[i][1]).toByteArray(),
          state.getMatchResults().get(i).toByteArray());
    }
  }

  private static UDFParameters createPatternParameters() {
    return createPatternParameters("1,2", "1.0,2.0", "100");
  }

  private static UDFParameters createPatternParameters(
      String timePattern, String valuePattern, String threshold) {
    Map<String, String> attributes = new HashMap<>();
    attributes.put("timePattern", timePattern);
    attributes.put("valuePattern", valuePattern);
    attributes.put("threshold", threshold);
    return new UDFParameters(
        Collections.singletonList("s1"), Collections.singletonList(Type.DOUBLE), attributes);
  }

  private static UDFParameters createDtwParameters() {
    return createDtwParameters("1.0,3.0", "100");
  }

  private static UDFParameters createDtwParameters(String pattern, String threshold) {
    Map<String, String> attributes = new HashMap<>();
    attributes.put("pattern", pattern);
    attributes.put("threshold", threshold);
    return new UDFParameters(
        Collections.singletonList("s1"), Collections.singletonList(Type.DOUBLE), attributes);
  }

  private static Column[] buildPatternInputColumns() {
    return new Column[] {
      new TestColumn(
          TSDataType.DOUBLE,
          new long[0],
          new double[] {1.0, 0.0, Double.NaN, Double.POSITIVE_INFINITY, 3.0},
          new boolean[] {false, true, false, false, false}),
      new TestColumn(
          TSDataType.INT64,
          new long[] {1L, 2L, 3L, 4L, 5L},
          new double[0],
          new boolean[] {false, false, false, false, false})
    };
  }

  private static class TestColumn implements Column {

    private final TSDataType dataType;
    private final long[] longValues;
    private final double[] doubleValues;
    private final boolean[] nulls;

    private TestColumn(
        TSDataType dataType, long[] longValues, double[] doubleValues, boolean[] nulls) {
      this.dataType = dataType;
      this.longValues = longValues;
      this.doubleValues = doubleValues;
      this.nulls = nulls;
    }

    @Override
    public TSDataType getDataType() {
      return dataType;
    }

    @Override
    public ColumnEncoding getEncoding() {
      return ColumnEncoding.INT32_ARRAY;
    }

    @Override
    public Column convertTo(TSDataType targetType) {
      if (targetType == dataType) {
        return this;
      }
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean arePositionsEqual(int position, Column other, int otherPosition) {
      if (isNull(position) || other.isNull(otherPosition)) {
        return isNull(position) && other.isNull(otherPosition);
      }
      if (dataType == TSDataType.INT64) {
        return getLong(position) == other.getLong(otherPosition);
      }
      return Double.compare(getDouble(position), other.getDouble(otherPosition)) == 0;
    }

    @Override
    public long getLong(int position) {
      return longValues[position];
    }

    @Override
    public double getDouble(int position) {
      return doubleValues[position];
    }

    @Override
    public boolean mayHaveNull() {
      return true;
    }

    @Override
    public boolean isNull(int position) {
      return nulls[position];
    }

    @Override
    public boolean[] isNull() {
      return nulls;
    }

    @Override
    public void setNull(int start, int end) {
      throw new UnsupportedOperationException();
    }

    @Override
    public int getPositionCount() {
      return nulls.length;
    }

    @Override
    public long getRetainedSizeInBytes() {
      return 0;
    }

    @Override
    public long getSizeInBytes() {
      return 0;
    }

    @Override
    public Column getRegion(int positionOffset, int length) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Column getRegionCopy(int positionOffset, int length) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Column subColumn(int fromIndex) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Column subColumnCopy(int fromIndex) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Column getPositions(int[] positions, int offset, int length) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Column copyPositions(int[] positions, int offset, int length) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void reverse() {
      throw new UnsupportedOperationException();
    }

    @Override
    public int getInstanceSize() {
      return 0;
    }

    @Override
    public void setPositionCount(int count) {
      throw new UnsupportedOperationException();
    }
  }
}
