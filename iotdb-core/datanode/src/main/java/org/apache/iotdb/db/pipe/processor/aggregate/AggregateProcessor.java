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

package org.apache.iotdb.db.pipe.processor.aggregate;

import org.apache.iotdb.db.pipe.agent.PipeAgent;
import org.apache.iotdb.db.pipe.agent.plugin.dataregion.PipeDataRegionPluginAgent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeInsertNodeTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.db.pipe.processor.aggregate.datastructure.aggregator.AggregatorRuntimeAttributes;
import org.apache.iotdb.db.pipe.processor.aggregate.datastructure.aggregator.AggregatorStaticAttributes;
import org.apache.iotdb.db.pipe.processor.aggregate.datastructure.intermediate.IntermediateResultAttributes;
import org.apache.iotdb.db.pipe.processor.aggregate.datastructure.timeseries.TimeSeriesAttributes;
import org.apache.iotdb.db.utils.DateTimeUtils;
import org.apache.iotdb.db.utils.TimestampPrecisionUtils;
import org.apache.iotdb.pipe.api.PipeProcessor;
import org.apache.iotdb.pipe.api.access.Row;
import org.apache.iotdb.pipe.api.collector.EventCollector;
import org.apache.iotdb.pipe.api.collector.RowCollector;
import org.apache.iotdb.pipe.api.customizer.configuration.PipeProcessorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.event.dml.insertion.TsFileInsertionEvent;
import org.apache.iotdb.pipe.api.exception.PipeException;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.apache.iotdb.commons.pipe.config.constant.PipeProcessorConstant.PROCESSOR_AGGREGATORS_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeProcessorConstant.PROCESSOR_AGGREGATORS_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeProcessorConstant.PROCESSOR_OUTPUT_DATABASE_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeProcessorConstant.PROCESSOR_OUTPUT_DATABASE_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeProcessorConstant.PROCESSOR_OUTPUT_MAX_DELAY_SECONDS_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeProcessorConstant.PROCESSOR_OUTPUT_MAX_DELAY_SECONDS_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeProcessorConstant.PROCESSOR_OUTPUT_MEASUREMENTS_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeProcessorConstant.PROCESSOR_OUTPUT_MEASUREMENTS_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeProcessorConstant.PROCESSOR_SLIDING_BOUNDARY_TIME_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeProcessorConstant.PROCESSOR_SLIDING_BOUNDARY_TIME_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeProcessorConstant.PROCESSOR_SLIDING_SECONDS_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeProcessorConstant.PROCESSOR_SLIDING_SECONDS_KEY;

/** {@link AggregateProcessor} is the parent class of all the aggregate functions' implementors. */
public class AggregateProcessor implements PipeProcessor {

  private String pipeName;

  private long outputMaxDelayMilliseconds;

  private String outputDatabase;

  private long slidingBoundaryTime;

  private long slidingInterval;

  private final Map<String, AggregatorRuntimeAttributes> aggregator2RuntimeAttributesMap =
      new HashMap<>();
  private final Map<String, IntermediateResultAttributes> intermediateResult2AttributesMap =
      new HashMap<>();

  private static final Map<String, Integer> pipeName2referenceCountMap = new ConcurrentHashMap<>();
  private static final Map<String, Map<String, TimeSeriesAttributes>>
      pipeName2timeSeries2TimeSeriesAttributesMap = new ConcurrentHashMap<>();

  private final List<PipeProcessor> childProcessors = new ArrayList<>();

  private static final AtomicLong lastValueReceiveTime = new AtomicLong(0);

  @Override
  public void validate(PipeParameterValidator validator) throws Exception {
    // Do nothing if is called by child classes
    if (this.getClass() != AggregateProcessor.class) {
      return;
    }
    final PipeParameters parameters = validator.getParameters();
    validator
        .validate(
            args -> (long) args == -1 || (long) args > 0,
            String.format(
                "The parameter %s must be equals to -1 or greater than 0",
                PROCESSOR_OUTPUT_MAX_DELAY_SECONDS_KEY),
            parameters.getLongOrDefault(
                PROCESSOR_OUTPUT_MAX_DELAY_SECONDS_KEY,
                PROCESSOR_OUTPUT_MAX_DELAY_SECONDS_DEFAULT_VALUE))
        .validate(
            args -> (long) args > 0,
            String.format("The parameter %s must be greater than 0", PROCESSOR_SLIDING_SECONDS_KEY),
            parameters.getLongOrDefault(
                PROCESSOR_SLIDING_SECONDS_KEY, PROCESSOR_SLIDING_SECONDS_DEFAULT_VALUE))
        .validate(
            args -> !((String) args).isEmpty(),
            String.format("The parameter %s must not be empty", PROCESSOR_AGGREGATORS_KEY),
            parameters.getStringOrDefault(
                PROCESSOR_AGGREGATORS_KEY, PROCESSOR_AGGREGATORS_DEFAULT_VALUE));
  }

  @Override
  public void customize(PipeParameters parameters, PipeProcessorRuntimeConfiguration configuration)
      throws Exception {
    // Do nothing if is called by child classes
    if (this.getClass() != AggregateProcessor.class) {
      return;
    }
    pipeName = configuration.getRuntimeEnvironment().getPipeName();
    pipeName2referenceCountMap.compute(
        pipeName, (name, count) -> Objects.nonNull(count) ? count + 1 : 1);

    long outputMaxDelaySeconds =
        parameters.getLongOrDefault(
            PROCESSOR_OUTPUT_MAX_DELAY_SECONDS_KEY,
            PROCESSOR_OUTPUT_MAX_DELAY_SECONDS_DEFAULT_VALUE);
    outputMaxDelayMilliseconds =
        outputMaxDelaySeconds == -1 ? Long.MAX_VALUE : outputMaxDelaySeconds * 1000;
    outputDatabase =
        parameters.getStringOrDefault(
            PROCESSOR_OUTPUT_DATABASE_KEY, PROCESSOR_OUTPUT_DATABASE_DEFAULT_VALUE);
    slidingBoundaryTime =
        parameters.hasAnyAttributes(PROCESSOR_SLIDING_BOUNDARY_TIME_KEY)
            ? DateTimeUtils.convertTimestampOrDatetimeStrToLongWithDefaultZone(
                parameters.getString(PROCESSOR_SLIDING_BOUNDARY_TIME_KEY))
            : PROCESSOR_SLIDING_BOUNDARY_TIME_DEFAULT_VALUE;
    slidingInterval =
        TimestampPrecisionUtils.convertToCurrPrecision(
            parameters.getLongOrDefault(
                PROCESSOR_SLIDING_SECONDS_KEY, PROCESSOR_SLIDING_SECONDS_DEFAULT_VALUE),
            TimeUnit.SECONDS);

    List<String> aggregatorNameList =
        Arrays.stream(
                parameters
                    .getStringOrDefault(
                        PROCESSOR_AGGREGATORS_KEY, PROCESSOR_AGGREGATORS_DEFAULT_VALUE)
                    .replace(" ", "")
                    .split(","))
            .collect(Collectors.toList());
    List<String> outputMeasurementNameList =
        Arrays.stream(
                parameters
                    .getStringOrDefault(
                        PROCESSOR_OUTPUT_MEASUREMENTS_KEY,
                        PROCESSOR_OUTPUT_MEASUREMENTS_DEFAULT_VALUE)
                    .replace(" ", "")
                    .split(","))
            .collect(Collectors.toList());
    Map<String, String> aggregator2outputMeasurementNameMap = new HashMap<>();
    for (int i = 0; i < aggregatorNameList.size(); ++i) {
      if (i < outputMeasurementNameList.size()) {
        aggregator2outputMeasurementNameMap.put(
            aggregatorNameList.get(i).toLowerCase(), outputMeasurementNameList.get(i));
      } else {
        aggregator2outputMeasurementNameMap.put(
            aggregatorNameList.get(i).toLowerCase(), aggregatorNameList.get(i));
      }
    }

    // Load the useful aggregators' and their corresponding intermediate results' computational
    // logic.
    Set<String> aggregatorNameSet =
        aggregatorNameList.stream().map(String::toLowerCase).collect(Collectors.toSet());
    Set<String> declaredIntermediateResultSet = new HashSet<>();
    PipeDataRegionPluginAgent agent = PipeAgent.plugin().dataRegion();
    for (String pipePluginName :
        agent.getSubPluginNamesWithSpecifiedParent(AggregateProcessor.class)) {
      // Children are allowed to validate and configure the computational logic
      // from the same parameters other than processor name
      PipeProcessor childProcessor =
          agent.getConfiguredProcessor(pipePluginName, parameters, configuration);
      Map<String, AggregatorStaticAttributes> aggregatorName2StaticAttributesMap =
          ((AggregateProcessor) childProcessor).getAggregatorName2StaticAttributesMap();
      for (Map.Entry<String, AggregatorStaticAttributes> entry :
          aggregatorName2StaticAttributesMap.entrySet()) {
        String lowerCaseAggregatorName = entry.getKey().toLowerCase();
        AggregatorStaticAttributes aggregatorStaticAttributes = entry.getValue();
        if (!aggregatorNameSet.contains(lowerCaseAggregatorName)) {
          continue;
        }
        aggregatorNameSet.remove(lowerCaseAggregatorName);
        aggregator2RuntimeAttributesMap.put(
            lowerCaseAggregatorName,
            new AggregatorRuntimeAttributes(
                aggregator2outputMeasurementNameMap.get(lowerCaseAggregatorName),
                aggregatorStaticAttributes.getTerminateWindowFunction(),
                aggregatorStaticAttributes.getOutputDataType()));
        declaredIntermediateResultSet.addAll(
            aggregatorStaticAttributes.getDeclaredIntermediateValueNames());
      }
      intermediateResult2AttributesMap.putAll(
          ((AggregateProcessor) childProcessor).getIntermediateResultName2AttributesMap());
      childProcessors.add(childProcessor);
    }
    if (!aggregatorNameSet.isEmpty()) {
      throw new PipeException(
          String.format("The attribute keys %s are invalid.", aggregatorNameSet));
    }
    intermediateResult2AttributesMap
        .keySet()
        .removeIf(key -> !declaredIntermediateResultSet.contains(key));
  }

  @Override
  public final void process(
      TabletInsertionEvent tabletInsertionEvent, EventCollector eventCollector) throws Exception {
    if (!(tabletInsertionEvent instanceof PipeInsertNodeTabletInsertionEvent)
        && !(tabletInsertionEvent instanceof PipeRawTabletInsertionEvent)) {
      eventCollector.collect(tabletInsertionEvent);
      return;
    }

    lastValueReceiveTime.set(System.currentTimeMillis());
    final AtomicReference<Exception> exception = new AtomicReference<>();

    tabletInsertionEvent
        .processRowByRow((row, rowCollector) -> processRow(row, rowCollector, exception))
        .forEach(
            event -> {
              try {
                eventCollector.collect(event);
              } catch (Exception e) {
                exception.set(e);
              }
            });

    if (exception.get() != null) {
      throw exception.get();
    }
  }

  private void processRow(
      Row row, RowCollector rowCollector, AtomicReference<Exception> exception) {
    for (int index = 0, size = row.size(); index < size; ++index) {
      // Do not calculate null values
      if (row.isNull(index)) {
        continue;
      }

      final String timeSeries =
          row.getDeviceId() + TsFileConstant.PATH_SEPARATOR + row.getColumnName(index);
      long timeStamp = row.getTime();
      final TimeSeriesAttributes timeSeriesAttributes =
          pipeName2timeSeries2TimeSeriesAttributesMap.get(pipeName).get(timeSeries);

      // Initiate the slidingBoundaryTime on the first incoming value
      if (timeSeriesAttributes.getCurrentBoundaryTime() == Long.MIN_VALUE) {
        if (timeStamp <= slidingBoundaryTime) {
          timeSeriesAttributes.setCurrentBoundaryTime(slidingBoundaryTime);
        } else {
          timeSeriesAttributes.setCurrentBoundaryTime(
              ((timeStamp - slidingBoundaryTime - 1) / slidingInterval + 1) * slidingInterval
                  + slidingBoundaryTime);
        }
      }

      long boundaryTime = timeSeriesAttributes.getCurrentBoundaryTime();
      // Ignore the value if the timestamp is sooner than the window left bound
      if (timeStamp < boundaryTime) {
        return;
      }
      // Compute the value if the timestamp is in the current window
      else if (timeStamp < boundaryTime + slidingInterval) {

      }
      // Terminate the window and emit the output if the timestamp is later than the window right
      // bound
      else {

      }
    }
  }

  /**
   * This equals to the default implementation of the interface. Here we add "final" to prevent
   * child classes from rewriting it.
   *
   * @param tsFileInsertionEvent {@link TsFileInsertionEvent} to be processed * @param
   *     eventCollector used to collect result events after processing * @throws Exception the user
   *     can throw errors if necessary
   */
  @Override
  public final void process(
      TsFileInsertionEvent tsFileInsertionEvent, EventCollector eventCollector) throws Exception {
    try {
      for (final TabletInsertionEvent tabletInsertionEvent :
          tsFileInsertionEvent.toTabletInsertionEvents()) {
        process(tabletInsertionEvent, eventCollector);
      }
    } finally {
      tsFileInsertionEvent.close();
    }
  }

  @Override
  public final void process(Event event, EventCollector eventCollector) throws Exception {
    eventCollector.collect(event);
  }

  @Override
  public void close() throws Exception {
    if (pipeName2referenceCountMap.compute(
            pipeName, (name, count) -> Objects.nonNull(count) ? count - 1 : 0)
        == 0) {
      pipeName2timeSeries2TimeSeriesAttributesMap.get(pipeName).clear();
      pipeName2timeSeries2TimeSeriesAttributesMap.remove(pipeName);
    }
    for (PipeProcessor childProcessor : childProcessors) {
      childProcessor.close();
    }
  }

  /////////////////////////////// Child classes logic ///////////////////////////////

  // Child classes must override these logics to be functional.
  /**
   * Get the supported aggregators and its corresponding {@link AggregatorStaticAttributes}.
   *
   * @return Map {@literal <}AggregatorName, {@link AggregatorStaticAttributes}{@literal >}
   */
  protected Map<String, AggregatorStaticAttributes> getAggregatorName2StaticAttributesMap() {
    throw new UnsupportedOperationException(
        "The aggregate processor does not support getAggregatorName2StaticAttributesMap");
  }

  /**
   * Get the supported intermediate results and its corresponding {@link
   * IntermediateResultAttributes}.
   *
   * @return Map {@literal <}AggregatorName, {@link IntermediateResultAttributes}{@literal >}
   */
  protected Map<String, IntermediateResultAttributes> getIntermediateResultName2AttributesMap() {
    throw new UnsupportedOperationException(
        "The aggregate processor does not support getIntermediateResultName2StaticAttributesMap");
  }
}
