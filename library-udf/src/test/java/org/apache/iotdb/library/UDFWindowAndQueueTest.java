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

import org.apache.iotdb.library.anomaly.UDTFIQR;
import org.apache.iotdb.library.anomaly.UDTFKSigma;
import org.apache.iotdb.library.anomaly.UDTFLOF;
import org.apache.iotdb.library.anomaly.UDTFRange;
import org.apache.iotdb.library.dlearn.UDTFAR;
import org.apache.iotdb.library.dlearn.UDTFCluster;
import org.apache.iotdb.library.dmatch.UDAFDtw;
import org.apache.iotdb.library.dprofile.UDAFMad;
import org.apache.iotdb.library.dprofile.UDAFMedian;
import org.apache.iotdb.library.dprofile.UDAFPercentile;
import org.apache.iotdb.library.dprofile.UDAFPeriod;
import org.apache.iotdb.library.dprofile.UDAFQuantile;
import org.apache.iotdb.library.dprofile.UDAFSkew;
import org.apache.iotdb.library.dprofile.UDTFHistogram;
import org.apache.iotdb.library.dprofile.UDTFMinMax;
import org.apache.iotdb.library.dprofile.UDTFMvAvg;
import org.apache.iotdb.library.dprofile.UDTFQLB;
import org.apache.iotdb.library.dprofile.UDTFResample;
import org.apache.iotdb.library.dprofile.UDTFSample;
import org.apache.iotdb.library.dprofile.UDTFSegment;
import org.apache.iotdb.library.dprofile.UDTFSpline;
import org.apache.iotdb.library.dprofile.UDTFZScore;
import org.apache.iotdb.library.frequency.UDFEnvelopeAnalysis;
import org.apache.iotdb.library.frequency.UDTFConv;
import org.apache.iotdb.library.frequency.UDTFDWT;
import org.apache.iotdb.library.frequency.UDTFDeconv;
import org.apache.iotdb.library.frequency.UDTFFFT;
import org.apache.iotdb.library.frequency.UDTFHighPass;
import org.apache.iotdb.library.frequency.UDTFIDWT;
import org.apache.iotdb.library.frequency.UDTFIFFT;
import org.apache.iotdb.library.frequency.UDTFLowPass;
import org.apache.iotdb.library.series.UDTFConsecutiveSequences;
import org.apache.iotdb.library.series.UDTFConsecutiveWindows;
import org.apache.iotdb.library.string.UDTFRegexMatch;
import org.apache.iotdb.library.string.UDTFRegexReplace;
import org.apache.iotdb.library.string.UDTFRegexSplit;
import org.apache.iotdb.library.string.UDTFStrReplace;
import org.apache.iotdb.library.util.BooleanCircularQueue;
import org.apache.iotdb.library.util.CircularQueue;
import org.apache.iotdb.library.util.DoubleCircularQueue;
import org.apache.iotdb.library.util.LongCircularQueue;
import org.apache.iotdb.udf.api.access.Row;
import org.apache.iotdb.udf.api.access.RowIterator;
import org.apache.iotdb.udf.api.access.RowWindow;
import org.apache.iotdb.udf.api.collector.PointCollector;
import org.apache.iotdb.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.udf.api.exception.UDFAttributeNotProvidedException;
import org.apache.iotdb.udf.api.exception.UDFInputSeriesDataTypeNotValidException;
import org.apache.iotdb.udf.api.exception.UDFParameterNotValidException;
import org.apache.iotdb.udf.api.type.Binary;
import org.apache.iotdb.udf.api.type.Type;

import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class UDFWindowAndQueueTest {

  @Test
  public void testKSigmaDefaultWindowIsConsistent() throws Exception {
    UDTFKSigma kSigma = new UDTFKSigma();
    UDFParameters parameters = createSingleDoubleSeriesParameters(Collections.emptyMap());

    kSigma.validate(new UDFParameterValidator(parameters));
    kSigma.beforeStart(parameters, new UDTFConfigurations(ZoneId.systemDefault()));

    Assert.assertEquals(10, getWindowSize(kSigma));
  }

  @Test
  public void testKSigmaExplicitWindowOverridesDefault() throws Exception {
    Map<String, String> attributes = new HashMap<>();
    attributes.put("window", "3");

    UDTFKSigma kSigma = new UDTFKSigma();
    UDFParameters parameters = createSingleDoubleSeriesParameters(attributes);

    kSigma.validate(new UDFParameterValidator(parameters));
    kSigma.beforeStart(parameters, new UDTFConfigurations(ZoneId.systemDefault()));

    Assert.assertEquals(3, getWindowSize(kSigma));
  }

  @Test
  public void testMvAvgUsesRunningWindowSum() throws Exception {
    Map<String, String> attributes = new HashMap<>();
    attributes.put("window", "3");

    UDTFMvAvg mvAvg = new UDTFMvAvg();
    UDFParameters parameters = createSingleDoubleSeriesParameters(attributes);
    RecordingPointCollector collector = new RecordingPointCollector();

    mvAvg.validate(new UDFParameterValidator(parameters));
    mvAvg.beforeStart(parameters, new UDTFConfigurations(ZoneId.systemDefault()));
    mvAvg.transform(new DoubleRow(1, 1.0), collector);
    mvAvg.transform(new DoubleRow(2, 2.0), collector);
    mvAvg.transform(new DoubleRow(3, 3.0), collector);
    mvAvg.transform(new DoubleRow(4, 4.0), collector);

    Assert.assertEquals(Arrays.asList(3L, 4L), collector.timestamps);
    Assert.assertEquals(2.0, collector.values.get(0), 0.0);
    Assert.assertEquals(3.0, collector.values.get(1), 0.0);
  }

  @Test
  public void testLOFSkipsNullRowsWithoutReadingCompressedIndex() throws Exception {
    Map<String, String> attributes = new HashMap<>();
    attributes.put("k", "1");

    UDTFLOF lof = new UDTFLOF();
    UDFParameters parameters =
        new UDFParameters(
            Arrays.asList("s1", "s2"), Arrays.asList(Type.DOUBLE, Type.DOUBLE), attributes);
    RecordingPointCollector collector = new RecordingPointCollector();

    lof.validate(new UDFParameterValidator(parameters));
    lof.beforeStart(parameters, new UDTFConfigurations(ZoneId.systemDefault()));
    lof.transform(
        new SimpleRowWindow(
            new DoubleRow(1, new double[] {0.0, 0.0}, new boolean[] {false, false}),
            new DoubleRow(2, new double[] {0.0, 0.0}, new boolean[] {true, true}),
            new DoubleRow(3, new double[] {10.0, 10.0}, new boolean[] {false, false}),
            new DoubleRow(4, new double[] {20.0, 20.0}, new boolean[] {false, false})),
        collector);

    Assert.assertEquals(Arrays.asList(1L, 3L, 4L), collector.timestamps);
    Assert.assertEquals(3, collector.values.size());
  }

  @Test
  public void testLOFValidatesAllInputSeriesTypes() {
    UDFParameters parameters =
        new UDFParameters(
            Arrays.asList("s1", "s2"),
            Arrays.asList(Type.DOUBLE, Type.TEXT),
            Collections.emptyMap());

    Assert.assertThrows(
        UDFInputSeriesDataTypeNotValidException.class,
        () -> new UDTFLOF().validate(new UDFParameterValidator(parameters)));
  }

  @Test
  public void testDeconvAcceptsQuotientResult() throws Exception {
    Map<String, String> attributes = new HashMap<>();
    attributes.put("result", "quotient");

    UDTFDeconv deconv = new UDTFDeconv();
    UDFParameters parameters = createTwoDoubleSeriesParameters(attributes);

    deconv.validate(new UDFParameterValidator(parameters));
    deconv.beforeStart(parameters, new UDTFConfigurations(ZoneId.systemDefault()));
  }

  @Test
  public void testConvIgnoresEmptyEffectiveInput() throws Exception {
    UDTFConv conv = new UDTFConv();
    UDFParameters parameters = createTwoDoubleSeriesParameters(Collections.emptyMap());
    RecordingPointCollector collector = new RecordingPointCollector();

    conv.validate(new UDFParameterValidator(parameters));
    conv.beforeStart(parameters, new UDTFConfigurations(ZoneId.systemDefault()));
    conv.transform(
        new DoubleRow(1, new double[] {0.0, 0.0}, new boolean[] {true, true}), collector);
    conv.terminate(collector);

    Assert.assertTrue(collector.timestamps.isEmpty());
    Assert.assertTrue(collector.values.isEmpty());
  }

  @Test
  public void testConsecutiveSequencesIgnoresShortAutoGapWindow() throws Exception {
    UDFParameters parameters = createSingleDoubleSeriesParameters(Collections.emptyMap());
    RecordingPointCollector collector = new RecordingPointCollector();

    UDTFConsecutiveSequences sequences = new UDTFConsecutiveSequences();
    sequences.validate(new UDFParameterValidator(parameters));
    sequences.beforeStart(parameters, new UDTFConfigurations(ZoneId.systemDefault()));
    sequences.transform(new DoubleRow(1, 1.0), collector);
    sequences.terminate(collector);

    Assert.assertTrue(collector.timestamps.isEmpty());
    Assert.assertTrue(collector.values.isEmpty());
  }

  @Test
  public void testRangeRequiresBoundsAndValidatesOrder() {
    UDTFRange range = new UDTFRange();

    Assert.assertThrows(
        UDFAttributeNotProvidedException.class,
        () ->
            range.validate(
                new UDFParameterValidator(
                    createSingleDoubleSeriesParameters(Collections.emptyMap()))));

    Map<String, String> attributes = new HashMap<>();
    attributes.put("lower_bound", "2");
    attributes.put("upper_bound", "1");
    Assert.assertThrows(
        UDFParameterNotValidException.class,
        () ->
            range.validate(
                new UDFParameterValidator(createSingleDoubleSeriesParameters(attributes))));
  }

  @Test
  public void testStreamModeRequiresExplicitParameters() {
    Map<String, String> attributes = new HashMap<>();
    attributes.put("compute", "stream");

    Assert.assertThrows(
        UDFAttributeNotProvidedException.class,
        () ->
            new UDTFMinMax()
                .validate(
                    new UDFParameterValidator(createSingleDoubleSeriesParameters(attributes))));
    Assert.assertThrows(
        UDFAttributeNotProvidedException.class,
        () ->
            new UDTFZScore()
                .validate(
                    new UDFParameterValidator(createSingleDoubleSeriesParameters(attributes))));
    Assert.assertThrows(
        UDFAttributeNotProvidedException.class,
        () ->
            new UDTFIQR()
                .validate(
                    new UDFParameterValidator(createSingleDoubleSeriesParameters(attributes))));
  }

  @Test
  public void testMinMaxBatchKeepsTimestampsAlignedWhenSkippingInvalidValues() throws Exception {
    UDTFMinMax minMax = new UDTFMinMax();
    UDFParameters parameters = createSingleDoubleSeriesParameters(Collections.emptyMap());
    RecordingPointCollector collector = new RecordingPointCollector();

    minMax.validate(new UDFParameterValidator(parameters));
    minMax.beforeStart(parameters, new UDTFConfigurations(ZoneId.systemDefault()));
    minMax.transform(new DoubleRow(1, Double.NaN), collector);
    minMax.transform(new DoubleRow(2, 10.0), collector);
    minMax.transform(new DoubleRow(3, 20.0), collector);
    minMax.terminate(collector);

    Assert.assertEquals(Arrays.asList(2L, 3L), collector.timestamps);
    Assert.assertEquals(0.0, collector.values.get(0), 0.0);
    Assert.assertEquals(1.0, collector.values.get(1), 0.0);
  }

  @Test
  public void testIQRIgnoresEmptyBatchInput() throws Exception {
    UDTFIQR iqr = new UDTFIQR();
    UDFParameters parameters = createSingleDoubleSeriesParameters(Collections.emptyMap());
    RecordingPointCollector collector = new RecordingPointCollector();

    iqr.validate(new UDFParameterValidator(parameters));
    iqr.beforeStart(parameters, new UDTFConfigurations(ZoneId.systemDefault()));
    iqr.terminate(collector);

    Assert.assertTrue(collector.timestamps.isEmpty());
    Assert.assertTrue(collector.values.isEmpty());
  }

  @Test
  public void testStringReplaceRequiresParameters() {
    Assert.assertThrows(
        UDFAttributeNotProvidedException.class,
        () ->
            new UDTFStrReplace()
                .validate(
                    new UDFParameterValidator(
                        createSingleTextSeriesParameters(Collections.emptyMap()))));

    Map<String, String> attributes = new HashMap<>();
    attributes.put("target", "a");
    Assert.assertThrows(
        UDFAttributeNotProvidedException.class,
        () ->
            new UDTFStrReplace()
                .validate(new UDFParameterValidator(createSingleTextSeriesParameters(attributes))));

    Assert.assertThrows(
        UDFAttributeNotProvidedException.class,
        () ->
            new UDTFRegexReplace()
                .validate(
                    new UDFParameterValidator(
                        createSingleTextSeriesParameters(Collections.emptyMap()))));

    attributes.clear();
    attributes.put("regex", "a+");
    Assert.assertThrows(
        UDFAttributeNotProvidedException.class,
        () ->
            new UDTFRegexReplace()
                .validate(new UDFParameterValidator(createSingleTextSeriesParameters(attributes))));

    Assert.assertThrows(
        UDFAttributeNotProvidedException.class,
        () ->
            new UDTFRegexSplit()
                .validate(
                    new UDFParameterValidator(
                        createSingleTextSeriesParameters(Collections.emptyMap()))));
    Assert.assertThrows(
        UDFAttributeNotProvidedException.class,
        () ->
            new UDTFRegexMatch()
                .validate(
                    new UDFParameterValidator(
                        createSingleTextSeriesParameters(Collections.emptyMap()))));

    attributes.clear();
    attributes.put("regex", "[");
    attributes.put("replace", "x");
    Assert.assertThrows(
        UDFParameterNotValidException.class,
        () ->
            new UDTFRegexReplace()
                .validate(new UDFParameterValidator(createSingleTextSeriesParameters(attributes))));

    attributes.remove("replace");
    Assert.assertThrows(
        UDFParameterNotValidException.class,
        () ->
            new UDTFRegexSplit()
                .validate(new UDFParameterValidator(createSingleTextSeriesParameters(attributes))));
    Assert.assertThrows(
        UDFParameterNotValidException.class,
        () ->
            new UDTFRegexMatch()
                .validate(new UDFParameterValidator(createSingleTextSeriesParameters(attributes))));
  }

  @Test
  public void testAggregateFunctionsIgnoreEmptyInput() throws Exception {
    UDFParameters parameters = createSingleDoubleSeriesParameters(Collections.emptyMap());
    RecordingPointCollector collector = new RecordingPointCollector();

    UDAFMedian median = new UDAFMedian();
    median.validate(new UDFParameterValidator(parameters));
    median.beforeStart(parameters, new UDTFConfigurations(ZoneId.systemDefault()));
    median.terminate(collector);

    UDAFMad mad = new UDAFMad();
    mad.validate(new UDFParameterValidator(parameters));
    mad.beforeStart(parameters, new UDTFConfigurations(ZoneId.systemDefault()));
    mad.terminate(collector);

    UDAFPercentile percentile = new UDAFPercentile();
    percentile.validate(new UDFParameterValidator(parameters));
    percentile.beforeStart(parameters, new UDTFConfigurations(ZoneId.systemDefault()));
    percentile.terminate(collector);

    UDAFQuantile quantile = new UDAFQuantile();
    quantile.validate(new UDFParameterValidator(parameters));
    quantile.beforeStart(parameters, new UDTFConfigurations(ZoneId.systemDefault()));
    quantile.terminate(collector);

    Assert.assertTrue(collector.timestamps.isEmpty());
    Assert.assertTrue(collector.values.isEmpty());
  }

  @Test
  public void testExactMadUsesAbsoluteDeviations() throws Exception {
    UDFParameters parameters = createSingleDoubleSeriesParameters(Collections.emptyMap());
    UDAFMad mad = new UDAFMad();
    RecordingPointCollector collector = new RecordingPointCollector();

    mad.validate(new UDFParameterValidator(parameters));
    mad.beforeStart(parameters, new UDTFConfigurations(ZoneId.systemDefault()));
    mad.transform(new DoubleRow(1, 1.0), collector);
    mad.transform(new DoubleRow(2, 2.0), collector);
    mad.transform(new DoubleRow(3, 4.0), collector);
    mad.terminate(collector);

    Assert.assertEquals(Collections.singletonList(0L), collector.timestamps);
    Assert.assertEquals(1.0, collector.values.get(0), 0.0);
  }

  @Test
  public void testPercentileInstancesKeepIndependentTimestamps() throws Exception {
    UDFParameters parameters = createSingleDoubleSeriesParameters(Collections.emptyMap());
    UDAFPercentile first = new UDAFPercentile();
    UDAFPercentile second = new UDAFPercentile();
    RecordingPointCollector firstCollector = new RecordingPointCollector();

    first.validate(new UDFParameterValidator(parameters));
    second.validate(new UDFParameterValidator(parameters));
    first.beforeStart(parameters, new UDTFConfigurations(ZoneId.systemDefault()));
    first.transform(new DoubleRow(1, 5.0), firstCollector);

    second.beforeStart(parameters, new UDTFConfigurations(ZoneId.systemDefault()));
    second.transform(new DoubleRow(2, 9.0), new RecordingPointCollector());
    first.terminate(firstCollector);

    Assert.assertEquals(Collections.singletonList(1L), firstCollector.timestamps);
    Assert.assertEquals(5.0, firstCollector.values.get(0), 0.0);
  }

  @Test
  public void testPeriodSkipsLeadingInvalidValue() throws Exception {
    UDAFPeriod period = new UDAFPeriod();
    UDFParameters parameters = createSingleDoubleSeriesParameters(Collections.emptyMap());
    RecordingPointCollector collector = new RecordingPointCollector();

    period.validate(new UDFParameterValidator(parameters));
    period.beforeStart(parameters, new UDTFConfigurations(ZoneId.systemDefault()));
    period.transform(
        new SimpleRowWindow(new DoubleRow(1, Double.NaN), new DoubleRow(2, 1.0)), collector);

    Assert.assertEquals(Collections.singletonList(0L), collector.timestamps);
    Assert.assertEquals(0.0, collector.values.get(0), 0.0);
  }

  @Test
  public void testQLBResetsAccumulatedStatisticBetweenRuns() throws Exception {
    UDFParameters parameters = createSingleDoubleSeriesParameters(Collections.emptyMap());
    UDTFQLB reused = new UDTFQLB();
    UDTFQLB fresh = new UDTFQLB();
    RecordingPointCollector firstRun = new RecordingPointCollector();
    RecordingPointCollector reusedSecondRun = new RecordingPointCollector();
    RecordingPointCollector freshRun = new RecordingPointCollector();

    reused.validate(new UDFParameterValidator(parameters));
    fresh.validate(new UDFParameterValidator(parameters));
    runQLB(reused, parameters, firstRun);
    runQLB(reused, parameters, reusedSecondRun);
    runQLB(fresh, parameters, freshRun);

    Assert.assertEquals(freshRun.timestamps, reusedSecondRun.timestamps);
    Assert.assertArrayEquals(
        freshRun.values.stream().mapToDouble(Double::doubleValue).toArray(),
        reusedSecondRun.values.stream().mapToDouble(Double::doubleValue).toArray(),
        1e-12);
  }

  @Test
  public void testSkewIgnoresEmptyInputAndResetsBetweenRuns() throws Exception {
    UDFParameters parameters = createSingleDoubleSeriesParameters(Collections.emptyMap());
    UDAFSkew skew = new UDAFSkew();
    RecordingPointCollector emptyCollector = new RecordingPointCollector();
    RecordingPointCollector firstRun = new RecordingPointCollector();
    RecordingPointCollector secondRun = new RecordingPointCollector();

    skew.validate(new UDFParameterValidator(parameters));
    skew.beforeStart(parameters, new UDTFConfigurations(ZoneId.systemDefault()));
    skew.terminate(emptyCollector);
    Assert.assertTrue(emptyCollector.timestamps.isEmpty());

    skew.beforeStart(parameters, new UDTFConfigurations(ZoneId.systemDefault()));
    skew.transform(new DoubleRow(1, 1.0), firstRun);
    skew.transform(new DoubleRow(2, 2.0), firstRun);
    skew.transform(new DoubleRow(3, 3.0), firstRun);
    skew.terminate(firstRun);

    skew.beforeStart(parameters, new UDTFConfigurations(ZoneId.systemDefault()));
    skew.transform(new DoubleRow(4, 1.0), secondRun);
    skew.transform(new DoubleRow(5, 2.0), secondRun);
    skew.transform(new DoubleRow(6, 3.0), secondRun);
    skew.terminate(secondRun);

    Assert.assertEquals(firstRun.values.get(0), secondRun.values.get(0), 0.0);
  }

  @Test
  public void testHistogramRejectsZeroWidthRange() {
    Map<String, String> attributes = new HashMap<>();
    attributes.put("min", "1");
    attributes.put("max", "1");

    Assert.assertThrows(
        UDFParameterNotValidException.class,
        () ->
            new UDTFHistogram()
                .validate(
                    new UDFParameterValidator(createSingleDoubleSeriesParameters(attributes))));
  }

  @Test
  public void testReservoirSampleResetsCountBetweenRuns() throws Exception {
    Map<String, String> attributes = new HashMap<>();
    attributes.put("k", "2");
    UDFParameters parameters = createSingleDoubleSeriesParameters(attributes);
    UDTFSample sample = new UDTFSample();
    RecordingPointCollector firstRun = new RecordingPointCollector();
    RecordingPointCollector secondRun = new RecordingPointCollector();

    sample.validate(new UDFParameterValidator(parameters));
    sample.beforeStart(parameters, new UDTFConfigurations(ZoneId.systemDefault()));
    sample.transform(new DoubleRow(1, 1.0), firstRun);
    sample.terminate(firstRun);

    sample.beforeStart(parameters, new UDTFConfigurations(ZoneId.systemDefault()));
    sample.transform(new DoubleRow(2, 2.0), secondRun);
    sample.terminate(secondRun);

    Assert.assertEquals(Collections.singletonList(2L), secondRun.timestamps);
    Assert.assertEquals(2.0, secondRun.values.get(0), 0.0);
  }

  @Test
  public void testSampleClearsReservoirStateWhenSwitchingMethod() throws Exception {
    Map<String, String> reservoirAttributes = new HashMap<>();
    reservoirAttributes.put("k", "2");
    UDFParameters reservoirParameters = createSingleDoubleSeriesParameters(reservoirAttributes);

    Map<String, String> isometricAttributes = new HashMap<>();
    isometricAttributes.put("k", "2");
    isometricAttributes.put("method", "isometric");
    UDFParameters isometricParameters = createSingleDoubleSeriesParameters(isometricAttributes);

    UDTFSample sample = new UDTFSample();
    RecordingPointCollector collector = new RecordingPointCollector();

    sample.validate(new UDFParameterValidator(reservoirParameters));
    sample.beforeStart(reservoirParameters, new UDTFConfigurations(ZoneId.systemDefault()));
    sample.transform(new DoubleRow(1, 1.0), collector);

    sample.validate(new UDFParameterValidator(isometricParameters));
    sample.beforeStart(isometricParameters, new UDTFConfigurations(ZoneId.systemDefault()));
    sample.terminate(collector);

    Assert.assertTrue(collector.timestamps.isEmpty());
  }

  @Test
  public void testDtwEmptyInputProducesNoOutput() throws Exception {
    UDAFDtw dtw = new UDAFDtw();
    UDFParameters parameters = createTwoDoubleSeriesParameters(Collections.emptyMap());
    RecordingPointCollector collector = new RecordingPointCollector();

    dtw.validate(new UDFParameterValidator(parameters));
    dtw.beforeStart(parameters, new UDTFConfigurations(ZoneId.systemDefault()));
    dtw.terminate(collector);

    Assert.assertTrue(collector.timestamps.isEmpty());
  }

  @Test
  public void testDtwClearsPreviousResultForEmptyWindow() throws Exception {
    UDAFDtw dtw = new UDAFDtw();
    UDFParameters parameters = createTwoDoubleSeriesParameters(Collections.emptyMap());
    RecordingPointCollector collector = new RecordingPointCollector();

    dtw.validate(new UDFParameterValidator(parameters));
    dtw.beforeStart(parameters, new UDTFConfigurations(ZoneId.systemDefault()));
    dtw.transform(
        new SimpleRowWindow(
            new DoubleRow(1, new double[] {1.0, 1.0}, new boolean[] {false, false})),
        collector);
    dtw.terminate(collector);
    Assert.assertFalse(collector.timestamps.isEmpty());

    collector.timestamps.clear();
    collector.values.clear();
    dtw.transform(
        new SimpleRowWindow(new DoubleRow(2, new double[] {0.0, 0.0}, new boolean[] {true, true})),
        collector);
    dtw.terminate(collector);

    Assert.assertTrue(collector.timestamps.isEmpty());
  }

  @Test
  public void testFrequencyFunctionsIgnoreEmptyInput() throws Exception {
    RecordingPointCollector collector = new RecordingPointCollector();
    UDFParameters singleSeries = createSingleDoubleSeriesParameters(Collections.emptyMap());

    UDTFFFT fft = new UDTFFFT();
    fft.validate(new UDFParameterValidator(singleSeries));
    fft.beforeStart(singleSeries, new UDTFConfigurations(ZoneId.systemDefault()));
    fft.terminate(collector);

    Map<String, String> wpassAttributes = new HashMap<>();
    wpassAttributes.put("wpass", "0.5");
    UDFParameters wpassParameters = createSingleDoubleSeriesParameters(wpassAttributes);
    UDTFLowPass lowPass = new UDTFLowPass();
    lowPass.validate(new UDFParameterValidator(wpassParameters));
    lowPass.beforeStart(wpassParameters, new UDTFConfigurations(ZoneId.systemDefault()));
    lowPass.terminate(collector);

    UDTFHighPass highPass = new UDTFHighPass();
    highPass.validate(new UDFParameterValidator(wpassParameters));
    highPass.beforeStart(wpassParameters, new UDTFConfigurations(ZoneId.systemDefault()));
    highPass.terminate(collector);

    Map<String, String> waveletAttributes = new HashMap<>();
    waveletAttributes.put("method", "Haar");
    UDFParameters waveletParameters = createSingleDoubleSeriesParameters(waveletAttributes);
    UDTFDWT dwt = new UDTFDWT();
    dwt.validate(new UDFParameterValidator(waveletParameters));
    dwt.beforeStart(waveletParameters, new UDTFConfigurations(ZoneId.systemDefault()));
    dwt.terminate(collector);

    UDTFIDWT idwt = new UDTFIDWT();
    idwt.validate(new UDFParameterValidator(waveletParameters));
    idwt.beforeStart(waveletParameters, new UDTFConfigurations(ZoneId.systemDefault()));
    idwt.terminate(collector);

    UDFEnvelopeAnalysis envelopeAnalysis = new UDFEnvelopeAnalysis();
    envelopeAnalysis.validate(new UDFParameterValidator(singleSeries));
    envelopeAnalysis.beforeStart(singleSeries, new UDTFConfigurations(ZoneId.systemDefault()));
    envelopeAnalysis.terminate(collector);

    UDFParameters twoSeries = createTwoDoubleSeriesParameters(Collections.emptyMap());
    UDTFIFFT ifft = new UDTFIFFT();
    ifft.validate(new UDFParameterValidator(twoSeries));
    ifft.beforeStart(twoSeries, new UDTFConfigurations(ZoneId.systemDefault()));
    ifft.terminate(collector);

    Assert.assertTrue(collector.timestamps.isEmpty());
    Assert.assertTrue(collector.values.isEmpty());
  }

  @Test
  public void testMultiLayerDWTRoundTrip() throws Exception {
    Map<String, String> attributes = new HashMap<>();
    attributes.put("method", "Haar");
    attributes.put("layer", "2");
    UDFParameters parameters = createSingleDoubleSeriesParameters(attributes);
    RecordingPointCollector dwtCollector = new RecordingPointCollector();

    UDTFDWT dwt = new UDTFDWT();
    dwt.validate(new UDFParameterValidator(parameters));
    dwt.beforeStart(parameters, new UDTFConfigurations(ZoneId.systemDefault()));
    dwt.transform(new DoubleRow(1, 1.0), dwtCollector);
    dwt.transform(new DoubleRow(2, 2.0), dwtCollector);
    dwt.transform(new DoubleRow(3, 3.0), dwtCollector);
    dwt.transform(new DoubleRow(4, 4.0), dwtCollector);
    dwt.terminate(dwtCollector);

    UDTFIDWT idwt = new UDTFIDWT();
    RecordingPointCollector idwtCollector = new RecordingPointCollector();
    idwt.validate(new UDFParameterValidator(parameters));
    idwt.beforeStart(parameters, new UDTFConfigurations(ZoneId.systemDefault()));
    for (int i = 0; i < dwtCollector.values.size(); i++) {
      idwt.transform(
          new DoubleRow(dwtCollector.timestamps.get(i), dwtCollector.values.get(i)), idwtCollector);
    }
    idwt.terminate(idwtCollector);

    Assert.assertEquals(Arrays.asList(1L, 2L, 3L, 4L), idwtCollector.timestamps);
    Assert.assertArrayEquals(
        new double[] {1.0, 2.0, 3.0, 4.0},
        idwtCollector.values.stream().mapToDouble(Double::doubleValue).toArray(),
        1e-9);
  }

  @Test
  public void testIFFTValidatesImaginaryInputType() {
    UDFParameters parameters =
        new UDFParameters(
            Arrays.asList("s1", "s2"),
            Arrays.asList(Type.DOUBLE, Type.TEXT),
            Collections.emptyMap());

    Assert.assertThrows(
        UDFInputSeriesDataTypeNotValidException.class,
        () -> new UDTFIFFT().validate(new UDFParameterValidator(parameters)));
  }

  @Test
  public void testARValidatesPositiveOrder() {
    Map<String, String> attributes = new HashMap<>();
    attributes.put("p", "0");

    Assert.assertThrows(
        UDFParameterNotValidException.class,
        () ->
            new UDTFAR()
                .validate(
                    new UDFParameterValidator(createSingleDoubleSeriesParameters(attributes))));
  }

  @Test
  public void testRequiredParametersAreValidatedBeforeStart() {
    UDFParameterValidator numericValidator =
        new UDFParameterValidator(createSingleDoubleSeriesParameters(Collections.emptyMap()));

    Assert.assertThrows(
        UDFAttributeNotProvidedException.class, () -> new UDTFCluster().validate(numericValidator));
    Assert.assertThrows(
        UDFAttributeNotProvidedException.class,
        () -> new UDTFResample().validate(numericValidator));
    Assert.assertThrows(
        UDFAttributeNotProvidedException.class, () -> new UDTFSpline().validate(numericValidator));
    Assert.assertThrows(
        UDFAttributeNotProvidedException.class,
        () -> new UDTFConsecutiveWindows().validate(numericValidator));
  }

  @Test
  public void testSplineRequiresAtLeastTwoPoints() {
    Map<String, String> attributes = new HashMap<>();
    attributes.put("points", "1");

    Assert.assertThrows(
        UDFParameterNotValidException.class,
        () ->
            new UDTFSpline()
                .validate(
                    new UDFParameterValidator(createSingleDoubleSeriesParameters(attributes))));
  }

  @Test
  public void testSplineSkipsTooFewPointsAndHandlesNegativeTimestamps() throws Exception {
    Map<String, String> attributes = new HashMap<>();
    attributes.put("points", "3");
    UDFParameters parameters = createSingleDoubleSeriesParameters(attributes);

    UDTFSpline spline = new UDTFSpline();
    RecordingPointCollector collector = new RecordingPointCollector();

    spline.validate(new UDFParameterValidator(parameters));
    spline.beforeStart(parameters, new UDTFConfigurations(ZoneId.systemDefault()));
    spline.transform(new DoubleRow(1, 1.0), collector);
    spline.transform(new DoubleRow(2, 2.0), collector);
    spline.transform(new DoubleRow(3, 3.0), collector);
    spline.transform(new DoubleRow(4, 4.0), collector);
    spline.terminate(collector);

    Assert.assertTrue(collector.timestamps.isEmpty());

    spline.beforeStart(parameters, new UDTFConfigurations(ZoneId.systemDefault()));
    spline.transform(new DoubleRow(-5, 1.0), collector);
    spline.transform(new DoubleRow(-4, 2.0), collector);
    spline.transform(new DoubleRow(-3, 3.0), collector);
    spline.transform(new DoubleRow(-2, 4.0), collector);
    spline.transform(new DoubleRow(-1, 5.0), collector);
    spline.terminate(collector);

    Assert.assertEquals(Arrays.asList(-5L, -3L, -1L), collector.timestamps);
  }

  @Test
  public void testSegmentInstancesKeepIndependentBuffers() throws Exception {
    UDFParameters parameters = createSingleDoubleSeriesParameters(Collections.emptyMap());
    UDTFSegment first = new UDTFSegment();
    UDTFSegment second = new UDTFSegment();
    RecordingPointCollector firstCollector = new RecordingPointCollector();

    first.validate(new UDFParameterValidator(parameters));
    second.validate(new UDFParameterValidator(parameters));
    first.beforeStart(parameters, new UDTFConfigurations(ZoneId.systemDefault()));
    first.transform(new DoubleRow(1, 10.0), firstCollector);

    second.beforeStart(parameters, new UDTFConfigurations(ZoneId.systemDefault()));
    second.transform(new DoubleRow(2, 20.0), new RecordingPointCollector());
    first.terminate(firstCollector);

    Assert.assertEquals(Collections.singletonList(1L), firstCollector.timestamps);
    Assert.assertEquals(10.0, firstCollector.values.get(0), 0.0);
  }

  @Test
  public void testCircularQueueRejectsNonPositiveCapacity() {
    Assert.assertThrows(IllegalArgumentException.class, () -> new CircularQueue<>(0));
    Assert.assertThrows(IllegalArgumentException.class, () -> new CircularQueue<>(-1));
    Assert.assertThrows(IllegalArgumentException.class, () -> new DoubleCircularQueue(0));
    Assert.assertThrows(IllegalArgumentException.class, () -> new DoubleCircularQueue(-1));
    Assert.assertThrows(IllegalArgumentException.class, () -> new LongCircularQueue(0));
    Assert.assertThrows(IllegalArgumentException.class, () -> new LongCircularQueue(-1));
    Assert.assertThrows(IllegalArgumentException.class, () -> new BooleanCircularQueue(0));
    Assert.assertThrows(IllegalArgumentException.class, () -> new BooleanCircularQueue(-1));
  }

  @Test
  public void testObjectCircularQueueMaintainsOrderAfterWrapAndResize() {
    CircularQueue<String> queue = new CircularQueue<>(2);

    queue.push("a");
    queue.push("b");
    Assert.assertTrue(queue.isFull());
    Assert.assertEquals("a", queue.pop());

    queue.push("c");
    queue.push("d");

    Assert.assertEquals(3, queue.getSize());
    Assert.assertEquals("b", queue.get(0));
    Assert.assertEquals("c", queue.get(1));
    Assert.assertEquals("d", queue.get(2));
    Assert.assertEquals("b", queue.pop());
    Assert.assertEquals("c", queue.pop());
    Assert.assertEquals("d", queue.pop());
    Assert.assertTrue(queue.isEmpty());
  }

  @Test
  public void testPrimitiveCircularQueuesMaintainOrderAfterWrapAndResize() {
    DoubleCircularQueue doubleQueue = new DoubleCircularQueue(2);
    doubleQueue.push(1.5);
    doubleQueue.push(2.5);
    Assert.assertTrue(doubleQueue.isFull());
    Assert.assertEquals(1.5, doubleQueue.pop(), 0.0);
    doubleQueue.push(3.5);
    doubleQueue.push(4.5);
    Assert.assertEquals(2.5, doubleQueue.get(0), 0.0);
    Assert.assertEquals(3.5, doubleQueue.get(1), 0.0);
    Assert.assertEquals(4.5, doubleQueue.get(2), 0.0);

    LongCircularQueue longQueue = new LongCircularQueue(2);
    longQueue.push(1);
    longQueue.push(2);
    Assert.assertEquals(1, longQueue.pop());
    longQueue.push(3);
    longQueue.push(4);
    Assert.assertEquals(2, longQueue.get(0));
    Assert.assertEquals(3, longQueue.get(1));
    Assert.assertEquals(4, longQueue.get(2));

    BooleanCircularQueue booleanQueue = new BooleanCircularQueue(2);
    booleanQueue.push(true);
    booleanQueue.push(false);
    Assert.assertTrue(booleanQueue.pop());
    booleanQueue.push(true);
    booleanQueue.push(false);
    Assert.assertFalse(booleanQueue.get(0));
    Assert.assertTrue(booleanQueue.get(1));
    Assert.assertFalse(booleanQueue.get(2));
  }

  @Test
  public void testCircularQueuesRejectEmptyPopAndHead() {
    CircularQueue<String> objectQueue = new CircularQueue<>();
    Assert.assertThrows(IllegalArgumentException.class, () -> objectQueue.pop());
    Assert.assertThrows(IllegalArgumentException.class, () -> objectQueue.getHead());

    DoubleCircularQueue doubleQueue = new DoubleCircularQueue();
    Assert.assertThrows(IllegalArgumentException.class, () -> doubleQueue.pop());
    Assert.assertThrows(IllegalArgumentException.class, () -> doubleQueue.getHead());

    LongCircularQueue longQueue = new LongCircularQueue();
    Assert.assertThrows(IllegalArgumentException.class, () -> longQueue.pop());
    Assert.assertThrows(IllegalArgumentException.class, () -> longQueue.getHead());

    BooleanCircularQueue booleanQueue = new BooleanCircularQueue();
    Assert.assertThrows(IllegalArgumentException.class, () -> booleanQueue.pop());
    Assert.assertThrows(IllegalArgumentException.class, () -> booleanQueue.getHead());
  }

  private static UDFParameters createSingleDoubleSeriesParameters(Map<String, String> attributes) {
    return new UDFParameters(
        Collections.singletonList("s1"), Collections.singletonList(Type.DOUBLE), attributes);
  }

  private static UDFParameters createSingleTextSeriesParameters(Map<String, String> attributes) {
    return new UDFParameters(
        Collections.singletonList("s1"), Collections.singletonList(Type.TEXT), attributes);
  }

  private static UDFParameters createTwoDoubleSeriesParameters(Map<String, String> attributes) {
    return new UDFParameters(
        Arrays.asList("s1", "s2"), Arrays.asList(Type.DOUBLE, Type.DOUBLE), attributes);
  }

  private static int getWindowSize(UDTFKSigma kSigma) throws Exception {
    Field windowSize = UDTFKSigma.class.getDeclaredField("windowSize");
    windowSize.setAccessible(true);
    return (int) windowSize.get(kSigma);
  }

  private static void runQLB(
      UDTFQLB qlb, UDFParameters parameters, RecordingPointCollector collector) throws Exception {
    qlb.beforeStart(parameters, new UDTFConfigurations(ZoneId.systemDefault()));
    qlb.transform(new DoubleRow(1, 0.1), collector);
    qlb.transform(new DoubleRow(2, 0.2), collector);
    qlb.transform(new DoubleRow(3, 0.3), collector);
    qlb.terminate(collector);
  }

  private static class DoubleRow implements Row {

    private final long time;
    private final double[] values;
    private final boolean[] nulls;

    private DoubleRow(long time, double value) {
      this(time, new double[] {value}, new boolean[] {false});
    }

    private DoubleRow(long time, double[] values, boolean[] nulls) {
      this.time = time;
      this.values = values;
      this.nulls = nulls;
    }

    @Override
    public long getTime() {
      return time;
    }

    @Override
    public int getInt(int columnIndex) {
      throw new UnsupportedOperationException();
    }

    @Override
    public long getLong(int columnIndex) {
      throw new UnsupportedOperationException();
    }

    @Override
    public float getFloat(int columnIndex) {
      throw new UnsupportedOperationException();
    }

    @Override
    public double getDouble(int columnIndex) {
      if (nulls[columnIndex]) {
        throw new IllegalStateException("Null value should not be read");
      }
      return values[columnIndex];
    }

    @Override
    public boolean getBoolean(int columnIndex) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Binary getBinary(int columnIndex) {
      throw new UnsupportedOperationException();
    }

    @Override
    public String getString(int columnIndex) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Type getDataType(int columnIndex) {
      return Type.DOUBLE;
    }

    @Override
    public boolean isNull(int columnIndex) {
      return nulls[columnIndex];
    }

    @Override
    public int size() {
      return values.length;
    }
  }

  private static class SimpleRowWindow implements RowWindow {

    private final Row[] rows;

    private SimpleRowWindow(Row... rows) {
      this.rows = rows;
    }

    @Override
    public int windowSize() {
      return rows.length;
    }

    @Override
    public Row getRow(int rowIndex) {
      return rows[rowIndex];
    }

    @Override
    public Type getDataType(int columnIndex) {
      return Type.DOUBLE;
    }

    @Override
    public RowIterator getRowIterator() {
      return new SimpleRowIterator(rows);
    }

    @Override
    public long windowStartTime() {
      throw new UnsupportedOperationException();
    }

    @Override
    public long windowEndTime() {
      throw new UnsupportedOperationException();
    }
  }

  private static class SimpleRowIterator implements RowIterator {

    private final Row[] rows;
    private int index;

    private SimpleRowIterator(Row[] rows) {
      this.rows = rows;
      this.index = 0;
    }

    @Override
    public boolean hasNextRow() {
      return index < rows.length;
    }

    @Override
    public Row next() {
      return rows[index++];
    }

    @Override
    public void reset() {
      index = 0;
    }
  }

  private static class RecordingPointCollector implements PointCollector {

    private final List<Long> timestamps = new ArrayList<>();
    private final List<Double> values = new ArrayList<>();

    @Override
    public void putInt(long timestamp, int value) {
      timestamps.add(timestamp);
      values.add((double) value);
    }

    @Override
    public void putLong(long timestamp, long value) {
      timestamps.add(timestamp);
      values.add((double) value);
    }

    @Override
    public void putFloat(long timestamp, float value) {
      timestamps.add(timestamp);
      values.add((double) value);
    }

    @Override
    public void putDouble(long timestamp, double value) {
      timestamps.add(timestamp);
      values.add(value);
    }

    @Override
    public void putBoolean(long timestamp, boolean value) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void putBinary(long timestamp, Binary value) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void putString(long timestamp, String value) {
      throw new UnsupportedOperationException();
    }
  }
}
