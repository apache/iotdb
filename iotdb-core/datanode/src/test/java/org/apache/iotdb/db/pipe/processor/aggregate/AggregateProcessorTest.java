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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iotdb.db.pipe.processor.aggregate;

import org.apache.iotdb.commons.consensus.index.ProgressIndex;
import org.apache.iotdb.commons.consensus.index.ProgressIndexType;
import org.apache.iotdb.commons.consensus.index.impl.TimeWindowStateProgressIndex;
import org.apache.iotdb.commons.queryengine.utils.TimestampPrecisionUtils;
import org.apache.iotdb.db.pipe.processor.aggregate.operator.aggregatedresult.AggregatedResultOperator;
import org.apache.iotdb.db.pipe.processor.aggregate.operator.aggregatedresult.standardstatistics.AverageOperator;
import org.apache.iotdb.db.pipe.processor.aggregate.operator.aggregatedresult.standardstatistics.ClearanceFactorOperator;
import org.apache.iotdb.db.pipe.processor.aggregate.operator.aggregatedresult.standardstatistics.FormFactorOperator;
import org.apache.iotdb.db.pipe.processor.aggregate.operator.aggregatedresult.standardstatistics.PeakOperator;
import org.apache.iotdb.db.pipe.processor.aggregate.operator.aggregatedresult.standardstatistics.PulseFactorOperator;
import org.apache.iotdb.db.pipe.processor.aggregate.operator.intermediateresult.CustomizedReadableIntermediateResults;
import org.apache.iotdb.db.pipe.processor.aggregate.operator.intermediateresult.IntermediateResultOperator;
import org.apache.iotdb.db.pipe.processor.aggregate.operator.intermediateresult.sametype.numeric.AbsoluteMaxOperator;
import org.apache.iotdb.db.pipe.processor.aggregate.operator.intermediateresult.sametype.numeric.IntegralPoweredSumOperator;
import org.apache.iotdb.db.pipe.processor.aggregate.operator.intermediateresult.specifictype.doubletype.FractionPoweredSumOperator;
import org.apache.iotdb.db.pipe.processor.aggregate.operator.intermediateresult.specifictype.integertype.CountOperator;
import org.apache.iotdb.db.pipe.processor.aggregate.window.datastructure.TimeSeriesWindow;
import org.apache.iotdb.db.pipe.processor.aggregate.window.datastructure.WindowOutput;
import org.apache.iotdb.db.pipe.processor.aggregate.window.processor.TumblingWindowingProcessor;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.exception.PipeParameterNotValidException;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.PublicBAOS;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.apache.iotdb.commons.pipe.config.constant.PipeProcessorConstant.PROCESSOR_OPERATORS_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeProcessorConstant.PROCESSOR_OUTPUT_MEASUREMENTS_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeProcessorConstant.PROCESSOR_SLIDING_BOUNDARY_TIME_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeProcessorConstant.PROCESSOR_SLIDING_SECONDS_KEY;

public class AggregateProcessorTest {

  @Test(expected = PipeParameterNotValidException.class)
  public void testRejectsDuplicateOperatorNamesIgnoringCase() throws Exception {
    validate("avg,AVG", "");
  }

  @Test(expected = PipeParameterNotValidException.class)
  public void testRejectsEmptyOperatorNames() throws Exception {
    validate("avg,", "");
  }

  @Test(expected = PipeParameterNotValidException.class)
  public void testRejectsDuplicateOutputMeasurements() throws Exception {
    validate("avg,max", "result,result");
  }

  @Test(expected = PipeParameterNotValidException.class)
  public void testRejectsOutputMeasurementCollidingWithDefaultName() throws Exception {
    validate("avg,max", "max");
  }

  @Test(expected = PipeParameterNotValidException.class)
  public void testRejectsEmptyOutputMeasurements() throws Exception {
    validate("avg,max", "result,");
  }

  @Test(expected = PipeParameterNotValidException.class)
  public void testRejectsMoreOutputMeasurementsThanOperators() throws Exception {
    validate("avg,max", "avg,max,extra");
  }

  @Test
  public void testStripDatabasePrefixTreatsRegexCharactersLiterally() {
    Assert.assertEquals(
        "device", AggregateProcessor.stripDatabasePrefix("root.db$.device", "root.db$."));
    Assert.assertEquals(
        "root.dbx.device", AggregateProcessor.stripDatabasePrefix("root.dbx.device", "root.db$."));
  }

  @Test
  public void testStripDatabasePrefixOnlyAtTheBeginning() {
    Assert.assertEquals(
        "device.root.db.device",
        AggregateProcessor.stripDatabasePrefix("device.root.db.device", "root.db."));
  }

  @Test
  public void testSecondsToMillisecondsSaturatesOnOverflow() {
    Assert.assertEquals(Long.MAX_VALUE, AggregateProcessor.secondsToMilliseconds(Long.MAX_VALUE));
    Assert.assertEquals(Long.MIN_VALUE, AggregateProcessor.secondsToMilliseconds(Long.MIN_VALUE));
  }

  @Test
  public void testDeduplicatesMinimumTimestamp() {
    final List<WindowOutput> outputs =
        new ArrayList<>(
            Arrays.asList(
                new WindowOutput().setTimestamp(Long.MIN_VALUE),
                new WindowOutput().setTimestamp(Long.MIN_VALUE),
                new WindowOutput().setTimestamp(0)));

    final List<WindowOutput> distinctOutputs =
        AggregateProcessor.sortAndDeduplicateOutputs(outputs);

    Assert.assertEquals(2, distinctOutputs.size());
    Assert.assertEquals(Long.MIN_VALUE, distinctOutputs.get(0).getTimestamp());
    Assert.assertEquals(0L, distinctOutputs.get(1).getTimestamp());
  }

  @Test
  public void testMinimumTimestampCreatesFirstWindow() throws Exception {
    final TumblingWindowingProcessor windowingProcessor =
        createWindowingProcessor(Long.MIN_VALUE, 1);
    final TimeSeriesRuntimeState state =
        new TimeSeriesRuntimeState(
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap(),
            windowingProcessor);

    state.updateWindows(Long.MIN_VALUE, 1, 0);

    final List<WindowOutput> outputs = state.forceOutput();
    Assert.assertEquals(1, outputs.size());
    Assert.assertEquals(Long.MIN_VALUE, outputs.get(0).getTimestamp());
  }

  @Test
  public void testLegacyWindowStatePromotesCountAndMomentSums() throws Exception {
    final TumblingWindowingProcessor windowingProcessor = createWindowingProcessor(0, 1);
    final Map<String, Supplier<IntermediateResultOperator>> intermediateOperators = new HashMap<>();
    intermediateOperators.put("count", CountOperator::new);
    intermediateOperators.put("sum_x1", () -> new IntegralPoweredSumOperator(1));
    final Map<String, AggregatedResultOperator> aggregatedOperators =
        Collections.singletonMap("avg", new AverageOperator());
    final TimeSeriesWindow window = new TimeSeriesWindow(windowingProcessor, null);
    window.initWindow(intermediateOperators, aggregatedOperators, Collections.emptyMap());
    final ByteBuffer legacyState = serializeLegacyWindow(0, Integer.MAX_VALUE, Integer.MAX_VALUE);
    final ByteBuffer legacyStateReader = legacyState.duplicate();
    Assert.assertEquals(0L, ReadWriteIOUtils.readLong(legacyStateReader));
    Assert.assertEquals(2, ReadWriteIOUtils.readInt(legacyStateReader));
    Assert.assertEquals("count", ReadWriteIOUtils.readString(legacyStateReader));
    Assert.assertEquals(TSDataType.INT32, TSDataType.deserializeFrom(legacyStateReader));
    final CountOperator countOperator = new CountOperator();
    countOperator.deserialize(
        legacyStateReader, IntermediateResultOperator.LEGACY_STATE_FORMAT_VERSION);
    Assert.assertEquals(
        Integer.MAX_VALUE, ((Number) countOperator.getResult().getRight()).longValue());
    Assert.assertEquals("sum_x1", ReadWriteIOUtils.readString(legacyStateReader));
    Assert.assertEquals(TSDataType.INT32, TSDataType.deserializeFrom(legacyStateReader));
    final IntegralPoweredSumOperator sumOperator = new IntegralPoweredSumOperator(1);
    sumOperator.deserialize(legacyStateReader);
    Assert.assertEquals(
        (double) Integer.MAX_VALUE, (double) sumOperator.getResult().getRight(), 0.0);
    window.deserialize(legacyState);

    window.updateIntermediateResult(1, 1);
    Assert.assertEquals(
        1.0, (double) window.forceOutput().getAggregatedResults().get("avg").getRight(), 0.0);

    final ByteBuffer currentState;
    try (final PublicBAOS output = new PublicBAOS();
        final DataOutputStream outputStream = new DataOutputStream(output)) {
      window.serialize(outputStream);
      currentState = ByteBuffer.wrap(output.getBuf(), 0, output.size());
    }
    final TimeSeriesWindow restoredWindow = new TimeSeriesWindow(windowingProcessor, null);
    restoredWindow.initWindow(intermediateOperators, aggregatedOperators, Collections.emptyMap());
    restoredWindow.deserialize(currentState);
    Assert.assertEquals(
        1.0,
        (double) restoredWindow.forceOutput().getAggregatedResults().get("avg").getRight(),
        0.0);
  }

  @Test
  public void testMagnitudeFactorsHandleNegativeValues() throws Exception {
    final TumblingWindowingProcessor windowingProcessor = createWindowingProcessor(0, 1);
    final TimeSeriesWindow window = new TimeSeriesWindow(windowingProcessor, null);
    window.initWindow(
        createFactorIntermediateOperators(),
        createFactorAggregatedOperators(),
        Collections.emptyMap());

    window.updateIntermediateResult(0, -2);
    window.updateIntermediateResult(1, 4);

    final Map<String, Pair<TSDataType, Object>> results =
        window.forceOutput().getAggregatedResults();
    Assert.assertEquals(Math.sqrt(10) / 3, (double) results.get("ff").getRight(), 1e-12);
    Assert.assertEquals(4.0 / 3, (double) results.get("pf").getRight(), 1e-12);
    Assert.assertEquals(
        4.0 / Math.pow((Math.sqrt(2) + 2) / 2, 2), (double) results.get("ce").getRight(), 1e-12);
  }

  @Test
  public void testLegacyFactorWindowKeepsFormulaAcrossRestore() throws Exception {
    final TumblingWindowingProcessor windowingProcessor = createWindowingProcessor(0, 1);
    final Map<String, Supplier<IntermediateResultOperator>> intermediateOperators =
        createFactorIntermediateOperators();
    final Map<String, AggregatedResultOperator> aggregatedOperators =
        createFactorAggregatedOperators();
    final List<String> requiredIntermediateResultNames =
        aggregatedOperators.values().stream()
            .flatMap(operator -> operator.getDeclaredIntermediateValueNames().stream())
            .collect(Collectors.toList());
    intermediateOperators.keySet().retainAll(requiredIntermediateResultNames);
    final TimeSeriesWindow window = new TimeSeriesWindow(windowingProcessor, null);
    window.initWindow(intermediateOperators, aggregatedOperators, Collections.emptyMap());
    window.deserialize(serializeLegacyFactorWindow());
    Assert.assertTrue(
        "Expected the legacy clearance factor to be NaN immediately after restore, got "
            + window.forceOutput().getAggregatedResults().get("ce").getRight(),
        Double.isNaN((double) window.forceOutput().getAggregatedResults().get("ce").getRight()));

    window.updateIntermediateResult(2, 5);
    final Map<String, Pair<TSDataType, Object>> results =
        window.forceOutput().getAggregatedResults();
    Assert.assertEquals(Math.sqrt(15) / (7.0 / 3), (double) results.get("ff").getRight(), 1e-12);
    Assert.assertEquals(15.0 / 7, (double) results.get("pf").getRight(), 1e-12);
    Assert.assertTrue(
        "Expected the legacy clearance factor to remain NaN, got " + results.get("ce").getRight(),
        Double.isNaN((double) results.get("ce").getRight()));

    final ByteBuffer currentState;
    try (final PublicBAOS output = new PublicBAOS();
        final DataOutputStream outputStream = new DataOutputStream(output)) {
      window.serialize(outputStream);
      currentState = ByteBuffer.wrap(output.getBuf(), 0, output.size());
    }
    final TimeSeriesWindow restoredWindow = new TimeSeriesWindow(windowingProcessor, null);
    restoredWindow.initWindow(intermediateOperators, aggregatedOperators, Collections.emptyMap());
    restoredWindow.deserialize(currentState);
    Assert.assertEquals(
        15.0 / 7,
        (double) restoredWindow.forceOutput().getAggregatedResults().get("pf").getRight(),
        1e-12);
  }

  @Test
  public void testLegacyFactorWindowSkipsAggregatorsWithoutRestoredInputs() throws Exception {
    final TumblingWindowingProcessor windowingProcessor = createWindowingProcessor(0, 1);
    final TimeSeriesWindow window = new TimeSeriesWindow(windowingProcessor, null);
    window.initWindow(
        createFactorIntermediateOperators(),
        createFactorAggregatedOperators(),
        Collections.emptyMap());
    window.deserialize(serializeLegacyCountWindow());

    Assert.assertTrue(window.forceOutput().getAggregatedResults().isEmpty());
  }

  @Test(expected = IOException.class)
  public void testLegacyWindowStateRejectsRemovedIntermediateResult() throws Exception {
    final TumblingWindowingProcessor windowingProcessor = createWindowingProcessor(0, 1);
    final TimeSeriesWindow window = new TimeSeriesWindow(windowingProcessor, null);
    window.initWindow(
        Collections.singletonMap("count", CountOperator::new),
        Collections.emptyMap(),
        Collections.emptyMap());

    window.deserialize(serializeLegacyWindow(0, 1, 1));
  }

  @Test
  public void testIntegralPoweredSumAvoidsIntegerOverflow() {
    final IntegralPoweredSumOperator sumOfSquares = new IntegralPoweredSumOperator(2);
    Assert.assertTrue(sumOfSquares.initAndGetIsSupport(50_000, 0));
    sumOfSquares.updateValue(50_000, 1);

    Assert.assertEquals(5_000_000_000.0, (double) sumOfSquares.getResult().getRight(), 0.0);
  }

  @Test
  public void testAbsoluteMaxHandlesMinimumIntegerAndLegacyState() throws IOException {
    final AbsoluteMaxOperator absoluteMaxOperator = new AbsoluteMaxOperator();
    Assert.assertTrue(absoluteMaxOperator.initAndGetIsSupport(Integer.MIN_VALUE, 0));
    Assert.assertEquals(TSDataType.DOUBLE, absoluteMaxOperator.getResult().getLeft());
    Assert.assertEquals(2_147_483_648.0, (double) absoluteMaxOperator.getResult().getRight(), 0.0);

    final ByteBuffer legacyState;
    try (final PublicBAOS output = new PublicBAOS();
        final DataOutputStream outputStream = new DataOutputStream(output)) {
      TSDataType.INT32.serializeTo(outputStream);
      ReadWriteIOUtils.write(Integer.MIN_VALUE, outputStream);
      legacyState = ByteBuffer.wrap(output.getBuf(), 0, output.size());
    }
    absoluteMaxOperator.deserialize(legacyState);
    Assert.assertEquals(2_147_483_648.0, (double) absoluteMaxOperator.getResult().getRight(), 0.0);
  }

  @Test
  public void testPeakPreservesInputTypeAndIntegerPrecision() throws IOException {
    final AbsoluteMaxOperator intAbsoluteMax = new AbsoluteMaxOperator();
    Assert.assertTrue(intAbsoluteMax.initAndGetIsSupport(-17, 0));
    Assert.assertEquals(17, intAbsoluteMax.getResult().getRight());
    final Pair<TSDataType, Object> intPeak = terminatePeak(TSDataType.INT32, intAbsoluteMax);
    Assert.assertEquals(TSDataType.INT32, intPeak.getLeft());
    Assert.assertEquals(17, intPeak.getRight());

    final AbsoluteMaxOperator minimumIntAbsoluteMax = new AbsoluteMaxOperator();
    Assert.assertTrue(minimumIntAbsoluteMax.initAndGetIsSupport(Integer.MIN_VALUE, 0));
    final Pair<TSDataType, Object> minimumIntPeak =
        terminatePeak(TSDataType.INT32, minimumIntAbsoluteMax);
    Assert.assertEquals(TSDataType.INT32, minimumIntPeak.getLeft());
    Assert.assertEquals(Integer.MAX_VALUE, minimumIntPeak.getRight());

    final long largeLongValue = Long.MAX_VALUE - 1024;
    final AbsoluteMaxOperator longAbsoluteMax = new AbsoluteMaxOperator();
    Assert.assertTrue(longAbsoluteMax.initAndGetIsSupport(-largeLongValue, 0));
    Assert.assertEquals(TSDataType.INT64, longAbsoluteMax.getResult().getLeft());

    final ByteBuffer serializedState;
    try (final PublicBAOS output = new PublicBAOS();
        final DataOutputStream outputStream = new DataOutputStream(output)) {
      longAbsoluteMax.serialize(outputStream);
      serializedState = ByteBuffer.wrap(output.getBuf(), 0, output.size());
    }
    final AbsoluteMaxOperator restoredLongAbsoluteMax = new AbsoluteMaxOperator();
    restoredLongAbsoluteMax.deserialize(serializedState);
    final Pair<TSDataType, Object> longPeak =
        terminatePeak(TSDataType.INT64, restoredLongAbsoluteMax);
    Assert.assertEquals(TSDataType.INT64, longPeak.getLeft());
    Assert.assertEquals(largeLongValue, longPeak.getRight());
  }

  @Test
  public void testPeakSaturatesLongMinimumAfterCheckpointRestore() throws IOException {
    final AbsoluteMaxOperator absoluteMaxOperator = new AbsoluteMaxOperator();
    Assert.assertTrue(absoluteMaxOperator.initAndGetIsSupport(Long.MIN_VALUE, 0));
    Assert.assertEquals(
        9_223_372_036_854_775_808.0, (double) absoluteMaxOperator.getResult().getRight(), 0.0);

    final ByteBuffer serializedState;
    try (final PublicBAOS output = new PublicBAOS();
        final DataOutputStream outputStream = new DataOutputStream(output)) {
      absoluteMaxOperator.serialize(outputStream);
      serializedState = ByteBuffer.wrap(output.getBuf(), 0, output.size());
    }
    final AbsoluteMaxOperator restoredAbsoluteMaxOperator = new AbsoluteMaxOperator();
    restoredAbsoluteMaxOperator.deserialize(serializedState);
    Assert.assertEquals(TSDataType.DOUBLE, restoredAbsoluteMaxOperator.getResult().getLeft());
    Assert.assertEquals(
        9_223_372_036_854_775_808.0,
        (double) restoredAbsoluteMaxOperator.getResult().getRight(),
        0.0);
    restoredAbsoluteMaxOperator.updateValue(Long.MAX_VALUE, 1);

    final Pair<TSDataType, Object> peak =
        terminatePeak(TSDataType.INT64, restoredAbsoluteMaxOperator);
    Assert.assertEquals(TSDataType.INT64, peak.getLeft());
    Assert.assertEquals(Long.MAX_VALUE, peak.getRight());
  }

  @Test
  public void testNewerRestoredStateReplacesOlderWindows() throws IOException {
    final TumblingWindowingProcessor windowingProcessor = new TumblingWindowingProcessor();
    final TimeSeriesRuntimeState state =
        new TimeSeriesRuntimeState(
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap(),
            windowingProcessor);

    state.restoreTimestampAndWindows(new Pair<>(10L, serializeWindow(windowingProcessor, 100L)));
    state.restoreTimestampAndWindows(new Pair<>(20L, serializeWindow(windowingProcessor, 200L)));

    final List<WindowOutput> outputs = state.forceOutput();
    Assert.assertEquals(1, outputs.size());
    Assert.assertEquals(200L, outputs.get(0).getTimestamp());
  }

  @Test
  public void testNewerRestoredStateWithoutBufferClearsOlderWindows() throws IOException {
    final TumblingWindowingProcessor windowingProcessor = new TumblingWindowingProcessor();
    final TimeSeriesRuntimeState state =
        new TimeSeriesRuntimeState(
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap(),
            windowingProcessor);

    state.restoreTimestampAndWindows(new Pair<>(10L, serializeWindow(windowingProcessor, 100L)));
    state.restoreTimestampAndWindows(new Pair<>(20L, null));

    Assert.assertTrue(state.forceOutput().isEmpty());
  }

  @Test
  public void testReportedWindowProgressIndexRestoresOpenWindowAfterSerialization()
      throws Exception {
    final long interval = TimestampPrecisionUtils.convertToCurrPrecision(1, TimeUnit.SECONDS);
    final TumblingWindowingProcessor windowingProcessor = createWindowingProcessor(0, 1);
    final Map<String, Supplier<IntermediateResultOperator>> intermediateOperators = new HashMap<>();
    intermediateOperators.put("count", CountOperator::new);
    intermediateOperators.put("sum_x1", () -> new IntegralPoweredSumOperator(1));
    final Map<String, AggregatedResultOperator> aggregatedOperators =
        Collections.singletonMap("avg", new AverageOperator());
    final TimeSeriesRuntimeState runtimeState =
        new TimeSeriesRuntimeState(
            aggregatedOperators, intermediateOperators, Collections.emptyMap(), windowingProcessor);

    Assert.assertNull(runtimeState.updateWindows(0, 10, 0));
    final Pair<List<WindowOutput>, Pair<Long, ByteBuffer>> reportedState =
        runtimeState.updateWindows(interval, 20, 0);
    Assert.assertNotNull(reportedState);
    Assert.assertEquals(
        10.0,
        (double) reportedState.getLeft().get(0).getAggregatedResults().get("avg").getRight(),
        0.0);
    Assert.assertNotNull(reportedState.getRight());
    Assert.assertEquals(interval, (long) reportedState.getRight().getLeft());

    final TimeWindowStateProgressIndex reportedProgressIndex =
        new TimeWindowStateProgressIndex(
            Collections.singletonMap("root.sg.d.s", reportedState.getRight()));
    final ProgressIndex restoredProgressIndex = roundTripProgressIndex(reportedProgressIndex);
    final TimeWindowStateProgressIndex restoredWindowProgressIndex =
        restoredProgressIndex
            .getProgressIndexByType(TimeWindowStateProgressIndex.class)
            .orElse(null);
    Assert.assertNotNull(restoredWindowProgressIndex);

    final Pair<Long, ByteBuffer> restoredState =
        restoredWindowProgressIndex.getTimeSeries2TimestampWindowBufferPairMap().get("root.sg.d.s");
    Assert.assertNotNull(restoredState);

    for (int restoration = 0; restoration < 2; ++restoration) {
      final TimeSeriesRuntimeState restoredRuntimeState =
          new TimeSeriesRuntimeState(
              aggregatedOperators,
              intermediateOperators,
              Collections.emptyMap(),
              windowingProcessor);
      restoredRuntimeState.restoreTimestampAndWindows(restoredState);
      Assert.assertNull(restoredRuntimeState.updateWindows(interval, 100, 0));
      Assert.assertNull(restoredRuntimeState.updateWindows(interval + 1, 30, 0));

      final Pair<List<WindowOutput>, Pair<Long, ByteBuffer>> resumedOutput =
          restoredRuntimeState.updateWindows(interval * 2, 40, 0);
      Assert.assertNotNull(resumedOutput);
      Assert.assertEquals(1, resumedOutput.getLeft().size());
      Assert.assertEquals(
          25.0,
          (double) resumedOutput.getLeft().get(0).getAggregatedResults().get("avg").getRight(),
          0.0);
      Assert.assertNotNull(resumedOutput.getRight());
      Assert.assertEquals(interval * 2, (long) resumedOutput.getRight().getLeft());
    }
  }

  private static ByteBuffer serializeWindow(
      final TumblingWindowingProcessor windowingProcessor, final long timestamp)
      throws IOException {
    try (final PublicBAOS output = new PublicBAOS();
        final DataOutputStream outputStream = new DataOutputStream(output)) {
      ReadWriteIOUtils.write(1, outputStream);
      final TimeSeriesWindow window = new TimeSeriesWindow(windowingProcessor, null);
      window.setTimestamp(timestamp);
      window.serialize(outputStream);
      return ByteBuffer.wrap(output.getBuf(), 0, output.size());
    }
  }

  private static ProgressIndex roundTripProgressIndex(final ProgressIndex progressIndex)
      throws IOException {
    try (final PublicBAOS output = new PublicBAOS();
        final DataOutputStream outputStream = new DataOutputStream(output)) {
      progressIndex.serialize(outputStream);
      return ProgressIndexType.deserializeFrom(ByteBuffer.wrap(output.getBuf(), 0, output.size()));
    }
  }

  private static ByteBuffer serializeLegacyWindow(
      final long timestamp, final int count, final int sum) throws IOException {
    try (final PublicBAOS output = new PublicBAOS();
        final DataOutputStream outputStream = new DataOutputStream(output)) {
      ReadWriteIOUtils.write(timestamp, outputStream);
      ReadWriteIOUtils.write(2, outputStream);
      ReadWriteIOUtils.write("count", outputStream);
      TSDataType.INT32.serializeTo(outputStream);
      ReadWriteIOUtils.write(count, outputStream);
      ReadWriteIOUtils.write("sum_x1", outputStream);
      TSDataType.INT32.serializeTo(outputStream);
      TSDataType.INT32.serializeTo(outputStream);
      ReadWriteIOUtils.write(sum, outputStream);
      return ByteBuffer.wrap(output.getBuf(), 0, output.size());
    }
  }

  private static ByteBuffer serializeLegacyFactorWindow() throws IOException {
    try (final PublicBAOS output = new PublicBAOS();
        final DataOutputStream outputStream = new DataOutputStream(output)) {
      ReadWriteIOUtils.write(0L, outputStream);
      ReadWriteIOUtils.write(5, outputStream);
      writeLegacyIntermediate(outputStream, "abs_max", TSDataType.INT32, 4, true);
      writeLegacyIntermediate(outputStream, "count", TSDataType.INT32, 2, false);
      writeLegacyIntermediate(outputStream, "sum_x1", TSDataType.INT32, 2, true);
      writeLegacyIntermediate(outputStream, "sum_x2", TSDataType.INT32, 20, true);
      ReadWriteIOUtils.write("sum_x0.5", outputStream);
      TSDataType.INT32.serializeTo(outputStream);
      ReadWriteIOUtils.write(Double.NaN, outputStream);
      return ByteBuffer.wrap(output.getBuf(), 0, output.size());
    }
  }

  private static ByteBuffer serializeLegacyCountWindow() throws IOException {
    try (final PublicBAOS output = new PublicBAOS();
        final DataOutputStream outputStream = new DataOutputStream(output)) {
      ReadWriteIOUtils.write(0L, outputStream);
      ReadWriteIOUtils.write(1, outputStream);
      ReadWriteIOUtils.write("count", outputStream);
      TSDataType.INT32.serializeTo(outputStream);
      ReadWriteIOUtils.write(2, outputStream);
      return ByteBuffer.wrap(output.getBuf(), 0, output.size());
    }
  }

  private static void writeLegacyIntermediate(
      final DataOutputStream outputStream,
      final String name,
      final TSDataType measurementDataType,
      final int value,
      final boolean operatorStateHasDataType)
      throws IOException {
    ReadWriteIOUtils.write(name, outputStream);
    measurementDataType.serializeTo(outputStream);
    if (operatorStateHasDataType) {
      measurementDataType.serializeTo(outputStream);
    }
    ReadWriteIOUtils.write(value, outputStream);
  }

  private static Map<String, Supplier<IntermediateResultOperator>>
      createFactorIntermediateOperators() {
    final Map<String, Supplier<IntermediateResultOperator>> intermediateOperators = new HashMap<>();
    intermediateOperators.put("abs_max", AbsoluteMaxOperator::new);
    intermediateOperators.put("count", CountOperator::new);
    intermediateOperators.put("sum_abs_x0.5", () -> new FractionPoweredSumOperator(0.5, true));
    intermediateOperators.put("sum_abs_x1", () -> new FractionPoweredSumOperator(1, true));
    intermediateOperators.put("sum_x0.5", () -> new FractionPoweredSumOperator(0.5));
    intermediateOperators.put("sum_x1", () -> new IntegralPoweredSumOperator(1));
    intermediateOperators.put("sum_x2", () -> new IntegralPoweredSumOperator(2));
    return intermediateOperators;
  }

  private static Map<String, AggregatedResultOperator> createFactorAggregatedOperators() {
    final Map<String, AggregatedResultOperator> aggregatedOperators = new HashMap<>();
    aggregatedOperators.put("ff", new FormFactorOperator());
    aggregatedOperators.put("pf", new PulseFactorOperator());
    aggregatedOperators.put("ce", new ClearanceFactorOperator());
    return aggregatedOperators;
  }

  private static Pair<TSDataType, Object> terminatePeak(
      final TSDataType measurementDataType, final AbsoluteMaxOperator absoluteMaxOperator) {
    return new PeakOperator()
        .terminateWindow(
            measurementDataType,
            new CustomizedReadableIntermediateResults(
                Collections.singletonMap("abs_max", absoluteMaxOperator.getResult()), false));
  }

  private static TumblingWindowingProcessor createWindowingProcessor(
      final long boundaryTime, final long slidingSeconds) throws Exception {
    final Map<String, String> attributes = new HashMap<>();
    attributes.put(PROCESSOR_SLIDING_BOUNDARY_TIME_KEY, Long.toString(boundaryTime));
    attributes.put(PROCESSOR_SLIDING_SECONDS_KEY, Long.toString(slidingSeconds));
    final PipeParameters parameters = new PipeParameters(attributes);
    final TumblingWindowingProcessor processor = new TumblingWindowingProcessor();
    processor.validate(new PipeParameterValidator(parameters));
    processor.customize(parameters, () -> null);
    return processor;
  }

  private static void validate(final String operators, final String outputMeasurements)
      throws Exception {
    final Map<String, String> attributes = new HashMap<>();
    attributes.put(PROCESSOR_OPERATORS_KEY, operators);
    attributes.put(PROCESSOR_OUTPUT_MEASUREMENTS_KEY, outputMeasurements);
    new AggregateProcessor().validate(new PipeParameterValidator(new PipeParameters(attributes)));
  }
}
