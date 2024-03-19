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
import org.apache.iotdb.db.pipe.processor.aggregate.datastructure.window.TimeSeriesWindowState;
import org.apache.iotdb.db.pipe.processor.aggregate.operator.AbstractOperatorProcessor;
import org.apache.iotdb.db.pipe.processor.aggregate.operator.aggregatedresult.AggregatedResultOperator;
import org.apache.iotdb.db.pipe.processor.aggregate.operator.aggregatedresult.AggregatorRuntimeAttributes;
import org.apache.iotdb.db.pipe.processor.aggregate.operator.intermediateresult.IntermediateResultOperator;
import org.apache.iotdb.db.pipe.processor.aggregate.windowing.AbstractWindowingProcessor;
import org.apache.iotdb.pipe.api.PipeProcessor;
import org.apache.iotdb.pipe.api.access.Row;
import org.apache.iotdb.pipe.api.collector.EventCollector;
import org.apache.iotdb.pipe.api.collector.RowCollector;
import org.apache.iotdb.pipe.api.customizer.configuration.PipeProcessorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.exception.PipeException;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.utils.Pair;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.apache.iotdb.commons.pipe.config.constant.PipeProcessorConstant.PROCESSOR_OPERATORS_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeProcessorConstant.PROCESSOR_OPERATORS_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeProcessorConstant.PROCESSOR_OUTPUT_DATABASE_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeProcessorConstant.PROCESSOR_OUTPUT_DATABASE_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeProcessorConstant.PROCESSOR_OUTPUT_MAX_DELAY_SECONDS_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeProcessorConstant.PROCESSOR_OUTPUT_MAX_DELAY_SECONDS_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeProcessorConstant.PROCESSOR_OUTPUT_MEASUREMENTS_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeProcessorConstant.PROCESSOR_OUTPUT_MEASUREMENTS_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeProcessorConstant.PROCESSOR_WINDOWING_STRATEGY_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeProcessorConstant.PROCESSOR_WINDOWING_STRATEGY_KEY;

/**
 * {@link AggregateProcessor} is a {@link PipeProcessor} that can adopt different implementations of
 * {@link AbstractWindowingProcessor} as windowing strategy and use calculation methods from all the
 * {@link AbstractOperatorProcessor}s to calculate the given operators. Both the {@link
 * AbstractWindowingProcessor} and {@link AbstractOperatorProcessor} can be implemented by user and
 * loaded as a normal {@link PipeProcessor}
 */
public class AggregateProcessor implements PipeProcessor {
  String pipeName;
  private long outputMaxDelayMilliseconds;

  private String outputDatabase;

  private final Map<String, AggregatedResultOperator> operator2RuntimeAttributesMap =
      new HashMap<>();
  private final Map<String, Supplier<IntermediateResultOperator>> intermediateResult2AttributesMap =
      new HashMap<>();

  private static final Map<String, Integer> pipeName2referenceCountMap = new ConcurrentHashMap<>();
  private static final Map<
          String, Map<String, AtomicReference<Pair<Long, List<TimeSeriesWindowState>>>>>
      pipeName2timeSeries2LastReportTimeAndTimeSeriesWindowStateMap = new ConcurrentHashMap<>();

  private AbstractWindowingProcessor windowingProcessor;
  private final List<AbstractOperatorProcessor> operatorProcessors = new ArrayList<>();

  private static final AtomicLong lastValueReceiveTime = new AtomicLong(0);

  @Override
  public void validate(PipeParameterValidator validator) throws Exception {
    final PipeParameters parameters = validator.getParameters();
    validator
        .validate(
            args -> !((String) args).isEmpty(),
            String.format("The parameter %s must not be empty", PROCESSOR_OPERATORS_KEY),
            parameters.getStringOrDefault(
                PROCESSOR_OPERATORS_KEY, PROCESSOR_OPERATORS_DEFAULT_VALUE))
        .validate(
            args -> !((String) args).isEmpty(),
            String.format("The parameter %s must not be empty", PROCESSOR_WINDOWING_STRATEGY_KEY),
            parameters.getStringOrDefault(
                PROCESSOR_WINDOWING_STRATEGY_KEY, PROCESSOR_WINDOWING_STRATEGY_DEFAULT_VALUE));
  }

  @Override
  public void customize(PipeParameters parameters, PipeProcessorRuntimeConfiguration configuration)
      throws Exception {
    pipeName = configuration.getRuntimeEnvironment().getPipeName();
    pipeName2referenceCountMap.compute(
        pipeName, (name, count) -> Objects.nonNull(count) ? count + 1 : 1);

    long outputMaxDelaySeconds =
        parameters.getLongOrDefault(
            PROCESSOR_OUTPUT_MAX_DELAY_SECONDS_KEY,
            PROCESSOR_OUTPUT_MAX_DELAY_SECONDS_DEFAULT_VALUE);
    outputMaxDelayMilliseconds =
        outputMaxDelaySeconds < 0 ? Long.MAX_VALUE : outputMaxDelaySeconds * 1000;
    outputDatabase =
        parameters.getStringOrDefault(
            PROCESSOR_OUTPUT_DATABASE_KEY, PROCESSOR_OUTPUT_DATABASE_DEFAULT_VALUE);

    List<String> operatorNameList =
        Arrays.stream(
                parameters
                    .getStringOrDefault(PROCESSOR_OPERATORS_KEY, PROCESSOR_OPERATORS_DEFAULT_VALUE)
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
    Map<String, String> operator2outputMeasurementNameMap = new HashMap<>();
    for (int i = 0; i < operatorNameList.size(); ++i) {
      if (i < outputMeasurementNameList.size()) {
        operator2outputMeasurementNameMap.put(
            operatorNameList.get(i).toLowerCase(), outputMeasurementNameList.get(i));
      } else {
        operator2outputMeasurementNameMap.put(
            operatorNameList.get(i).toLowerCase(), operatorNameList.get(i));
      }
    }

    // Load the useful aggregators' and their corresponding intermediate results' computational
    // logic.
    Set<String> operatorNameSet =
        operatorNameList.stream().map(String::toLowerCase).collect(Collectors.toSet());
    Set<String> declaredIntermediateResultSet = new HashSet<>();
    PipeDataRegionPluginAgent agent = PipeAgent.plugin().dataRegion();
    for (String pipePluginName :
        agent.getSubPluginNamesWithSpecifiedParent(AbstractOperatorProcessor.class)) {
      // Children are allowed to validate and configure the computational logic
      // from the same parameters other than processor name
      AbstractOperatorProcessor operatorProcessor =
          (AbstractOperatorProcessor)
              agent.getConfiguredProcessor(pipePluginName, parameters, configuration);
      Map<String, AggregatedResultOperator> operatorName2StaticAttributesMap =
          operatorProcessor.getAggregatorName2StaticAttributesMap();
      for (Map.Entry<String, AggregatedResultOperator> entry :
          operatorName2StaticAttributesMap.entrySet()) {
        String lowerCaseAggregatorName = entry.getKey().toLowerCase();
        AggregatedResultOperator aggregatedResultOperator = entry.getValue();
        if (!operatorNameSet.contains(lowerCaseAggregatorName)) {
          continue;
        }
        operatorNameSet.remove(lowerCaseAggregatorName);
        operator2RuntimeAttributesMap.put(
            lowerCaseAggregatorName,
            new AggregatorRuntimeAttributes(
                operator2outputMeasurementNameMap.get(lowerCaseAggregatorName),
                aggregatedResultOperator.getTerminateWindowFunction(),
                aggregatedResultOperator.getOutputDataType()));
        declaredIntermediateResultSet.addAll(
            aggregatedResultOperator.getDeclaredIntermediateValueNames());
      }
      intermediateResult2AttributesMap.putAll(
          operatorProcessor.getIntermediateResultName2AttributesMap());
      operatorProcessors.add(operatorProcessor);
    }
    if (!operatorNameSet.isEmpty()) {
      throw new PipeException(String.format("The attribute keys %s are invalid.", operatorNameSet));
    }
    intermediateResult2AttributesMap
        .keySet()
        .removeIf(key -> !declaredIntermediateResultSet.contains(key));
    declaredIntermediateResultSet.removeAll(intermediateResult2AttributesMap.keySet());
    if (!declaredIntermediateResultSet.isEmpty()) {
      throw new PipeException(
          String.format(
              "The needed intermediate values %s are not defined.", declaredIntermediateResultSet));
    }
  }

  @Override
  public void process(TabletInsertionEvent tabletInsertionEvent, EventCollector eventCollector)
      throws Exception {
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
      final List<TimeSeriesWindowState> timeSeriesWindowState =
          pipeName2timeSeries2LastReportTimeAndTimeSeriesWindowStateMap
              .get(pipeName)
              .get(timeSeries)
              .get();

      // Initiate the slidingBoundaryTime on the first incoming value
      timeSeriesWindowState.tryInitBoundaryTime(timeStamp, slidingBoundaryTime, slidingInterval);

      long boundaryTime = timeSeriesWindowState.getCurrentBoundaryTime();
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

  @Override
  public void process(Event event, EventCollector eventCollector) throws Exception {
    if (System.currentTimeMillis() - lastValueReceiveTime.get() > outputMaxDelayMilliseconds) {}

    eventCollector.collect(event);
  }

  @Override
  public void close() throws Exception {
    if (pipeName2referenceCountMap.compute(
            pipeName, (name, count) -> Objects.nonNull(count) ? count - 1 : 0)
        == 0) {
      pipeName2timeSeries2LastReportTimeAndTimeSeriesWindowStateMap.get(pipeName).clear();
      pipeName2timeSeries2LastReportTimeAndTimeSeriesWindowStateMap.remove(pipeName);
    }
    if (Objects.nonNull(windowingProcessor)) {
      windowingProcessor.close();
    }
    for (PipeProcessor operatorProcessor : operatorProcessors) {
      operatorProcessor.close();
    }
  }
}
