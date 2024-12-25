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

import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.consensus.index.ProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.MinimumProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.TimeWindowStateProgressIndex;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeTaskMeta;
import org.apache.iotdb.commons.pipe.config.plugin.env.PipeTaskProcessorRuntimeEnvironment;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.commons.utils.PathUtils;
import org.apache.iotdb.db.pipe.agent.PipeDataNodeAgent;
import org.apache.iotdb.db.pipe.agent.plugin.dataregion.PipeDataRegionPluginAgent;
import org.apache.iotdb.db.pipe.event.common.row.PipeResetTabletRow;
import org.apache.iotdb.db.pipe.event.common.row.PipeRow;
import org.apache.iotdb.db.pipe.event.common.row.PipeRowCollector;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeInsertNodeTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tsfile.PipeTsFileInsertionEvent;
import org.apache.iotdb.db.pipe.processor.aggregate.operator.aggregatedresult.AggregatedResultOperator;
import org.apache.iotdb.db.pipe.processor.aggregate.operator.intermediateresult.IntermediateResultOperator;
import org.apache.iotdb.db.pipe.processor.aggregate.operator.processor.AbstractOperatorProcessor;
import org.apache.iotdb.db.pipe.processor.aggregate.window.datastructure.WindowOutput;
import org.apache.iotdb.db.pipe.processor.aggregate.window.processor.AbstractWindowingProcessor;
import org.apache.iotdb.db.queryengine.transformation.dag.udf.UDFParametersFactory;
import org.apache.iotdb.db.storageengine.StorageEngine;
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

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.common.constant.TsFileConstant;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.write.schema.MeasurementSchema;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
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
import static org.apache.iotdb.commons.pipe.config.constant.PipeProcessorConstant.PROCESSOR_OUTPUT_MIN_REPORT_INTERVAL_SECONDS_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeProcessorConstant.PROCESSOR_OUTPUT_MIN_REPORT_INTERVAL_SECONDS_KEY;
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
  private static final String WINDOWING_PROCESSOR_SUFFIX = "-windowing-processor";

  private String pipeName;
  private String databaseWithPathSeparator;
  private PipeTaskMeta pipeTaskMeta;
  private long outputMaxDelayMilliseconds;
  private long outputMinReportIntervalMilliseconds;
  private String outputDatabaseWithPathSeparator;

  private final Map<String, AggregatedResultOperator> outputName2OperatorMap = new HashMap<>();
  private final Map<String, Supplier<IntermediateResultOperator>>
      intermediateResultName2OperatorSupplierMap = new HashMap<>();
  private final Map<String, String> systemParameters = new HashMap<>();

  private static final Map<String, Integer> pipeName2referenceCountMap = new ConcurrentHashMap<>();
  private static final Map<String, AtomicLong> pipeName2LastValueReceiveTimeMap =
      new ConcurrentHashMap<>();
  private static final ConcurrentMap<
          String, ConcurrentMap<String, AtomicReference<TimeSeriesRuntimeState>>>
      pipeName2timeSeries2TimeSeriesRuntimeStateMap = new ConcurrentHashMap<>();

  private AbstractWindowingProcessor windowingProcessor;
  private final List<AbstractOperatorProcessor> operatorProcessors = new ArrayList<>();

  // Static values, calculated on initialization
  private String[] columnNameStringList;

  @Override
  public void validate(final PipeParameterValidator validator) throws Exception {
    final PipeParameters parameters = validator.getParameters();
    validator
        .validate(
            arg -> !((String) arg).isEmpty(),
            String.format("The parameter %s must not be empty.", PROCESSOR_OPERATORS_KEY),
            parameters.getStringOrDefault(
                PROCESSOR_OPERATORS_KEY, PROCESSOR_OPERATORS_DEFAULT_VALUE))
        .validate(
            arg -> !((String) arg).isEmpty(),
            String.format("The parameter %s must not be empty.", PROCESSOR_WINDOWING_STRATEGY_KEY),
            parameters.getStringOrDefault(
                PROCESSOR_WINDOWING_STRATEGY_KEY, PROCESSOR_WINDOWING_STRATEGY_DEFAULT_VALUE))
        .validate(
            arg -> ((String) arg).isEmpty() || ((String) arg).startsWith("root."),
            String.format(
                "The output database %s shall start with root.",
                parameters.getStringOrDefault(
                    PROCESSOR_OUTPUT_DATABASE_KEY, PROCESSOR_OUTPUT_DATABASE_DEFAULT_VALUE)),
            parameters.getStringOrDefault(
                PROCESSOR_OUTPUT_DATABASE_KEY, PROCESSOR_OUTPUT_DATABASE_DEFAULT_VALUE))
        .validate(
            arg ->
                Arrays.stream(((String) arg).replace(" ", "").split(","))
                    .allMatch(this::isLegalMeasurement),
            String.format(
                "The output measurements %s contains illegal measurements, the measurements must be the last level of a legal path",
                parameters.getStringOrDefault(
                    PROCESSOR_OUTPUT_MEASUREMENTS_KEY,
                    PROCESSOR_OUTPUT_MEASUREMENTS_DEFAULT_VALUE)),
            parameters.getStringOrDefault(
                PROCESSOR_OUTPUT_MEASUREMENTS_KEY, PROCESSOR_OUTPUT_MEASUREMENTS_DEFAULT_VALUE));
  }

  private boolean isLegalMeasurement(final String measurement) {
    try {
      PathUtils.isLegalPath("root." + measurement);
    } catch (final IllegalPathException e) {
      return false;
    }
    return measurement.startsWith("`") && measurement.endsWith("`") || !measurement.contains(".");
  }

  @Override
  public void customize(
      final PipeParameters parameters, final PipeProcessorRuntimeConfiguration configuration)
      throws Exception {
    pipeName = configuration.getRuntimeEnvironment().getPipeName();
    pipeName2referenceCountMap.compute(
        pipeName, (name, count) -> Objects.nonNull(count) ? count + 1 : 1);
    pipeName2timeSeries2TimeSeriesRuntimeStateMap.putIfAbsent(pipeName, new ConcurrentHashMap<>());

    databaseWithPathSeparator =
        StorageEngine.getInstance()
                .getDataRegion(
                    new DataRegionId(configuration.getRuntimeEnvironment().getRegionId()))
                .getDatabaseName()
            + TsFileConstant.PATH_SEPARATOR;

    pipeTaskMeta =
        ((PipeTaskProcessorRuntimeEnvironment) configuration.getRuntimeEnvironment())
            .getPipeTaskMeta();
    // Load parameters
    final long outputMaxDelaySeconds =
        parameters.getLongOrDefault(
            PROCESSOR_OUTPUT_MAX_DELAY_SECONDS_KEY,
            PROCESSOR_OUTPUT_MAX_DELAY_SECONDS_DEFAULT_VALUE);
    // The output max delay milliseconds must be set to at least 1
    // to guarantee the correctness of the CAS in last receive time
    outputMaxDelayMilliseconds =
        outputMaxDelaySeconds < 0 ? Long.MAX_VALUE : Math.max(outputMaxDelaySeconds * 1000, 1);
    outputMinReportIntervalMilliseconds =
        parameters.getLongOrDefault(
                PROCESSOR_OUTPUT_MIN_REPORT_INTERVAL_SECONDS_KEY,
                PROCESSOR_OUTPUT_MIN_REPORT_INTERVAL_SECONDS_DEFAULT_VALUE)
            * 1000;
    final String outputDatabase =
        parameters.getStringOrDefault(
            PROCESSOR_OUTPUT_DATABASE_KEY, PROCESSOR_OUTPUT_DATABASE_DEFAULT_VALUE);
    outputDatabaseWithPathSeparator =
        outputDatabase.isEmpty() ? outputDatabase : outputDatabase + TsFileConstant.PATH_SEPARATOR;

    // Set output name
    final List<String> operatorNameList =
        Arrays.stream(
                parameters
                    .getStringOrDefault(PROCESSOR_OPERATORS_KEY, PROCESSOR_OPERATORS_DEFAULT_VALUE)
                    .replace(" ", "")
                    .split(","))
            .collect(Collectors.toList());

    final String outputMeasurementString =
        parameters.getStringOrDefault(
            PROCESSOR_OUTPUT_MEASUREMENTS_KEY, PROCESSOR_OUTPUT_MEASUREMENTS_DEFAULT_VALUE);
    final List<String> outputMeasurementNameList =
        outputMeasurementString.isEmpty()
            ? Collections.emptyList()
            : Arrays.stream(outputMeasurementString.replace(" ", "").split(","))
                .collect(Collectors.toList());

    final Map<String, String> aggregatorName2OutputNameMap = new HashMap<>();
    for (int i = 0; i < operatorNameList.size(); ++i) {
      if (i < outputMeasurementNameList.size()) {
        aggregatorName2OutputNameMap.put(
            operatorNameList.get(i).toLowerCase(), outputMeasurementNameList.get(i));
      } else {
        aggregatorName2OutputNameMap.put(
            operatorNameList.get(i).toLowerCase(), operatorNameList.get(i));
      }
    }

    // Load the useful aggregators' and their corresponding intermediate results' computational
    // logic.
    final Set<String> declaredIntermediateResultSet = new HashSet<>();
    final PipeDataRegionPluginAgent agent = PipeDataNodeAgent.plugin().dataRegion();
    for (final String pipePluginName :
        agent.getSubProcessorNamesWithSpecifiedParent(AbstractOperatorProcessor.class)) {
      // Children are allowed to validate and configure the computational logic
      // from the same parameters other than processor name
      final AbstractOperatorProcessor operatorProcessor =
          (AbstractOperatorProcessor)
              agent.getConfiguredProcessor(pipePluginName, parameters, configuration);
      operatorProcessor.getAggregatorOperatorSet().stream()
          .filter(
              operator ->
                  aggregatorName2OutputNameMap.containsKey(operator.getName().toLowerCase()))
          .forEach(
              operator -> {
                outputName2OperatorMap.put(
                    aggregatorName2OutputNameMap.get(operator.getName().toLowerCase()), operator);
                declaredIntermediateResultSet.addAll(operator.getDeclaredIntermediateValueNames());
              });

      operatorProcessor
          .getIntermediateResultOperatorSupplierSet()
          .forEach(
              supplier ->
                  intermediateResultName2OperatorSupplierMap.put(
                      supplier.get().getName(), supplier));
      operatorProcessors.add(operatorProcessor);
    }

    aggregatorName2OutputNameMap
        .entrySet()
        .removeIf(entry -> outputName2OperatorMap.containsKey(entry.getValue()));
    if (!aggregatorName2OutputNameMap.isEmpty()) {
      throw new PipeException(
          String.format(
              "The aggregator and output name %s is invalid.", aggregatorName2OutputNameMap));
    }

    intermediateResultName2OperatorSupplierMap.keySet().retainAll(declaredIntermediateResultSet);
    declaredIntermediateResultSet.removeAll(intermediateResultName2OperatorSupplierMap.keySet());
    if (!declaredIntermediateResultSet.isEmpty()) {
      throw new PipeException(
          String.format(
              "The needed intermediate values %s are not defined.", declaredIntermediateResultSet));
    }

    // Set up column name strings
    columnNameStringList = new String[outputName2OperatorMap.size()];
    final List<String> operatorNames = new ArrayList<>(outputName2OperatorMap.keySet());
    for (int i = 0; i < outputName2OperatorMap.size(); ++i) {
      columnNameStringList[i] = operatorNames.get(i);
    }

    // Get windowing processor
    final String processorName =
        parameters.getStringOrDefault(
                PROCESSOR_WINDOWING_STRATEGY_KEY, PROCESSOR_WINDOWING_STRATEGY_DEFAULT_VALUE)
            + WINDOWING_PROCESSOR_SUFFIX;
    final PipeProcessor windowProcessor =
        agent.getConfiguredProcessor(processorName, parameters, configuration);
    if (!(windowProcessor instanceof AbstractWindowingProcessor)) {
      throw new PipeException(
          String.format("The processor %s is not a windowing processor.", processorName));
    }
    windowingProcessor = (AbstractWindowingProcessor) windowProcessor;

    // Configure system parameters
    systemParameters.put(
        UDFParametersFactory.TIMESTAMP_PRECISION,
        CommonDescriptor.getInstance().getConfig().getTimestampPrecision());

    // The aggregated result operators can be configured here because they are global
    // and stateless, needing only one configuration
    this.outputName2OperatorMap
        .values()
        .forEach(operator -> operator.configureSystemParameters(systemParameters));

    // Restore window state
    final ProgressIndex index = pipeTaskMeta.getProgressIndex();
    if (index == MinimumProgressIndex.INSTANCE) {
      return;
    }
    if (!(index instanceof TimeWindowStateProgressIndex)) {
      throw new PipeException(
          String.format(
              "The aggregate processor does not support progressIndexType %s", index.getType()));
    }

    final TimeWindowStateProgressIndex timeWindowStateProgressIndex =
        (TimeWindowStateProgressIndex) index;
    for (final Map.Entry<String, Pair<Long, ByteBuffer>> entry :
        timeWindowStateProgressIndex.getTimeSeries2TimestampWindowBufferPairMap().entrySet()) {
      final AtomicReference<TimeSeriesRuntimeState> stateReference =
          pipeName2timeSeries2TimeSeriesRuntimeStateMap
              .get(pipeName)
              .computeIfAbsent(
                  entry.getKey(),
                  key ->
                      new AtomicReference<>(
                          new TimeSeriesRuntimeState(
                              outputName2OperatorMap,
                              intermediateResultName2OperatorSupplierMap,
                              systemParameters,
                              windowingProcessor)));
      synchronized (stateReference) {
        try {
          stateReference.get().restoreTimestampAndWindows(entry.getValue());
        } catch (final IOException e) {
          throw new PipeException("Encountered exception when deserializing from PipeTaskMeta", e);
        }
      }
    }
  }

  @Override
  public void process(
      final TabletInsertionEvent tabletInsertionEvent, final EventCollector eventCollector)
      throws Exception {
    if (!(tabletInsertionEvent instanceof PipeInsertNodeTabletInsertionEvent)
        && !(tabletInsertionEvent instanceof PipeRawTabletInsertionEvent)) {
      eventCollector.collect(tabletInsertionEvent);
      return;
    }

    pipeName2LastValueReceiveTimeMap
        .computeIfAbsent(pipeName, key -> new AtomicLong(System.currentTimeMillis()))
        .set(System.currentTimeMillis());

    final AtomicReference<Exception> exception = new AtomicReference<>();
    final TimeWindowStateProgressIndex[] progressIndex = {
      new TimeWindowStateProgressIndex(new ConcurrentHashMap<>())
    };

    final Iterable<TabletInsertionEvent> outputEvents =
        tabletInsertionEvent.processRowByRow(
            (row, rowCollector) ->
                progressIndex[0] =
                    (TimeWindowStateProgressIndex)
                        progressIndex[0].updateToMinimumEqualOrIsAfterProgressIndex(
                            new TimeWindowStateProgressIndex(
                                processRow(row, rowCollector, exception))));

    // Must reset progressIndex before collection
    ((EnrichedEvent) tabletInsertionEvent).bindProgressIndex(progressIndex[0]);

    outputEvents.forEach(
        event -> {
          try {
            eventCollector.collect(event);
          } catch (Exception e) {
            exception.set(e);
          }
        });

    if (Objects.nonNull(exception.get())) {
      throw exception.get();
    }
  }

  private Map<String, Pair<Long, ByteBuffer>> processRow(
      final Row row, final RowCollector rowCollector, final AtomicReference<Exception> exception) {
    final Map<String, Pair<Long, ByteBuffer>> resultMap = new HashMap<>();

    final long timestamp = row.getTime();
    for (int index = 0, size = row.size(); index < size; ++index) {
      // Do not calculate null values
      if (row.isNull(index)) {
        continue;
      }

      // All the timeSeries we stored are without database here if the parameters "outputDatabase"
      // is configured, because we do not support the same timeSeries (all the same except database)
      // in that mode, without the database we can save space and prevent string replacing problems.
      final String timeSeries =
          (outputDatabaseWithPathSeparator.isEmpty()
                  ? row.getDeviceId()
                  : row.getDeviceId().replaceFirst(databaseWithPathSeparator, ""))
              + TsFileConstant.PATH_SEPARATOR
              + row.getColumnName(index);

      final AtomicReference<TimeSeriesRuntimeState> stateReference =
          pipeName2timeSeries2TimeSeriesRuntimeStateMap
              .get(pipeName)
              .computeIfAbsent(
                  timeSeries,
                  key ->
                      new AtomicReference<>(
                          new TimeSeriesRuntimeState(
                              outputName2OperatorMap,
                              intermediateResultName2OperatorSupplierMap,
                              systemParameters,
                              windowingProcessor)));

      final Pair<List<WindowOutput>, Pair<Long, ByteBuffer>> result;
      synchronized (stateReference) {
        final TimeSeriesRuntimeState state = stateReference.get();
        try {
          switch (row.getDataType(index)) {
            case BOOLEAN:
              result =
                  state.updateWindows(
                      timestamp, row.getBoolean(index), outputMinReportIntervalMilliseconds);
              break;
            case INT32:
              result =
                  state.updateWindows(
                      timestamp, row.getInt(index), outputMinReportIntervalMilliseconds);
              break;
            case DATE:
              result =
                  state.updateWindows(
                      timestamp, row.getDate(index), outputMinReportIntervalMilliseconds);
              break;
            case INT64:
            case TIMESTAMP:
              result =
                  state.updateWindows(
                      timestamp, row.getLong(index), outputMinReportIntervalMilliseconds);
              break;
            case FLOAT:
              result =
                  state.updateWindows(
                      timestamp, row.getFloat(index), outputMinReportIntervalMilliseconds);
              break;
            case DOUBLE:
              result =
                  state.updateWindows(
                      timestamp, row.getDouble(index), outputMinReportIntervalMilliseconds);
              break;
            case TEXT:
            case STRING:
              result =
                  state.updateWindows(
                      timestamp, row.getString(index), outputMinReportIntervalMilliseconds);
              break;
            case BLOB:
              result =
                  state.updateWindows(
                      timestamp, row.getBinary(index), outputMinReportIntervalMilliseconds);
              break;
            default:
              throw new UnsupportedOperationException(
                  String.format("The type %s is not supported", row.getDataType(index)));
          }
          if (Objects.nonNull(result)) {
            collectWindowOutputs(result.getLeft(), timeSeries, rowCollector);
            if (Objects.nonNull(result.getRight())) {
              resultMap.put(timeSeries, result.getRight());
            }
          }
        } catch (final IOException | UnsupportedOperationException e) {
          exception.set(e);
        }
      }
    }
    return resultMap;
  }

  @Override
  public void process(
      final TsFileInsertionEvent tsFileInsertionEvent, final EventCollector eventCollector)
      throws Exception {
    try {
      for (final TabletInsertionEvent tabletInsertionEvent :
          tsFileInsertionEvent.toTabletInsertionEvents()) {
        process(tabletInsertionEvent, eventCollector);
      }
    } finally {
      tsFileInsertionEvent.close();
    }
    // The timeProgressIndex shall only be reported by the output events
    // whose progressIndex is bounded with tablet events
    if (tsFileInsertionEvent instanceof PipeTsFileInsertionEvent) {
      ((PipeTsFileInsertionEvent) tsFileInsertionEvent).skipReportOnCommit();
    }
  }

  @Override
  public void process(final Event event, final EventCollector eventCollector) throws Exception {
    final AtomicLong lastReceiveTime =
        pipeName2LastValueReceiveTimeMap.computeIfAbsent(
            pipeName, key -> new AtomicLong(System.currentTimeMillis()));

    final long previousTime = lastReceiveTime.get();

    if (System.currentTimeMillis() - previousTime > outputMaxDelayMilliseconds) {
      final AtomicReference<Exception> exception = new AtomicReference<>();

      pipeName2timeSeries2TimeSeriesRuntimeStateMap
          .get(pipeName)
          .keySet()
          .forEach(
              timeSeries -> {
                final AtomicReference<TimeSeriesRuntimeState> stateReference =
                    pipeName2timeSeries2TimeSeriesRuntimeStateMap.get(pipeName).get(timeSeries);
                synchronized (stateReference) {
                  final PipeRowCollector rowCollector = new PipeRowCollector(pipeTaskMeta, null);
                  try {
                    collectWindowOutputs(
                        stateReference.get().forceOutput(), timeSeries, rowCollector);
                  } catch (final IOException e) {
                    exception.set(e);
                  }
                  rowCollector
                      .convertToTabletInsertionEvents(false)
                      .forEach(
                          tabletEvent -> {
                            try {
                              eventCollector.collect(tabletEvent);
                            } catch (Exception e) {
                              exception.set(e);
                            }
                          });
                }
              });
      if (exception.get() != null) {
        // Retry at the fixed interval
        lastReceiveTime.set(System.currentTimeMillis());
        throw exception.get();
      }
      // Forbidding emitting results until next data comes
      // If the last receive time has changed, it means new data has come
      // thus the next output is needed
      lastReceiveTime.compareAndSet(previousTime, Long.MAX_VALUE);
    }

    eventCollector.collect(event);
  }

  /**
   * Collect {@link WindowOutput}s of a single timeSeries in one turn. The {@link TSDataType}s shall
   * be the same because the {@link AggregatedResultOperator}s shall return the same value for the
   * same timeSeries.
   *
   * @param outputs the {@link WindowOutput} output
   * @param timeSeries the timeSeries‘ name
   * @param collector {@link RowCollector}
   */
  public void collectWindowOutputs(
      final List<WindowOutput> outputs, final String timeSeries, final RowCollector collector)
      throws IOException {
    if (Objects.isNull(outputs) || outputs.isEmpty()) {
      return;
    }
    // Sort and same timestamps removal
    outputs.sort(Comparator.comparingLong(WindowOutput::getTimestamp));

    final AtomicLong lastValue = new AtomicLong(Long.MIN_VALUE);
    final List<WindowOutput> distinctOutputs = new ArrayList<>();
    outputs.forEach(
        output -> {
          final long timeStamp = output.getTimestamp();
          if (timeStamp != lastValue.get()) {
            lastValue.set(timeStamp);
            distinctOutputs.add(output);
          }
        });

    final MeasurementSchema[] measurementSchemaList =
        new MeasurementSchema[columnNameStringList.length];
    final TSDataType[] valueColumnTypes = new TSDataType[columnNameStringList.length];
    final Object[] valueColumns = new Object[columnNameStringList.length];
    final BitMap[] bitMaps = new BitMap[columnNameStringList.length];

    // Setup timestamps
    final long[] timestampColumn = new long[distinctOutputs.size()];
    for (int i = 0; i < distinctOutputs.size(); ++i) {
      timestampColumn[i] = distinctOutputs.get(i).getTimestamp();
    }

    for (int columnIndex = 0; columnIndex < columnNameStringList.length; ++columnIndex) {
      bitMaps[columnIndex] = new BitMap(distinctOutputs.size());
      for (int rowIndex = 0; rowIndex < distinctOutputs.size(); ++rowIndex) {
        final Map<String, Pair<TSDataType, Object>> aggregatedResults =
            distinctOutputs.get(rowIndex).getAggregatedResults();
        if (aggregatedResults.containsKey(columnNameStringList[columnIndex])) {
          if (Objects.isNull(valueColumnTypes[columnIndex])) {
            // Fill in measurements and init columns when the first non-null value is seen
            valueColumnTypes[columnIndex] =
                aggregatedResults.get(columnNameStringList[columnIndex]).getLeft();
            measurementSchemaList[columnIndex] =
                new MeasurementSchema(
                    columnNameStringList[columnIndex], valueColumnTypes[columnIndex]);
            switch (valueColumnTypes[columnIndex]) {
              case BOOLEAN:
                valueColumns[columnIndex] = new boolean[distinctOutputs.size()];
                break;
              case INT32:
                valueColumns[columnIndex] = new int[distinctOutputs.size()];
                break;
              case DATE:
                valueColumns[columnIndex] = new LocalDate[distinctOutputs.size()];
                break;
              case INT64:
              case TIMESTAMP:
                valueColumns[columnIndex] = new long[distinctOutputs.size()];
                break;
              case FLOAT:
                valueColumns[columnIndex] = new float[distinctOutputs.size()];
                break;
              case DOUBLE:
                valueColumns[columnIndex] = new double[distinctOutputs.size()];
                break;
              case TEXT:
              case BLOB:
              case STRING:
                valueColumns[columnIndex] = new Binary[distinctOutputs.size()];
                break;
              default:
                throw new UnsupportedOperationException(
                    String.format(
                        "The output tablet does not support column type %s",
                        valueColumnTypes[columnIndex]));
            }
          }
          // Fill in values
          switch (valueColumnTypes[columnIndex]) {
            case BOOLEAN:
              ((boolean[]) valueColumns[columnIndex])[rowIndex] =
                  (boolean) aggregatedResults.get(columnNameStringList[columnIndex]).getRight();
              break;
            case INT32:
              ((int[]) valueColumns[columnIndex])[rowIndex] =
                  (int) aggregatedResults.get(columnNameStringList[columnIndex]).getRight();
              break;
            case DATE:
              ((LocalDate[]) valueColumns[columnIndex])[rowIndex] =
                  (LocalDate) aggregatedResults.get(columnNameStringList[columnIndex]).getRight();
              break;
            case INT64:
            case TIMESTAMP:
              ((long[]) valueColumns[columnIndex])[rowIndex] =
                  (long) aggregatedResults.get(columnNameStringList[columnIndex]).getRight();
              break;
            case FLOAT:
              ((float[]) valueColumns[columnIndex])[rowIndex] =
                  (float) aggregatedResults.get(columnNameStringList[columnIndex]).getRight();
              break;
            case DOUBLE:
              ((double[]) valueColumns[columnIndex])[rowIndex] =
                  (double) aggregatedResults.get(columnNameStringList[columnIndex]).getRight();
              break;
            case TEXT:
            case STRING:
              ((Binary[]) valueColumns[columnIndex])[rowIndex] =
                  aggregatedResults.get(columnNameStringList[columnIndex]).getRight()
                          instanceof Binary
                      ? (Binary) aggregatedResults.get(columnNameStringList[columnIndex]).getRight()
                      : new Binary(
                          (String)
                              aggregatedResults.get(columnNameStringList[columnIndex]).getRight(),
                          TSFileConfig.STRING_CHARSET);
              break;
            case BLOB:
              ((Binary[]) valueColumns[columnIndex])[rowIndex] =
                  (Binary) aggregatedResults.get(columnNameStringList[columnIndex]).getRight();
              break;
            default:
              throw new UnsupportedOperationException(
                  String.format(
                      "The output tablet does not support column type %s",
                      valueColumnTypes[rowIndex]));
          }
        } else {
          bitMaps[columnIndex].mark(rowIndex);
        }
      }
    }

    // Filter null outputs
    final Integer[] originColumnIndex2FilteredColumnIndexMapperList =
        new Integer[columnNameStringList.length];
    int filteredCount = 0;
    for (int i = 0; i < columnNameStringList.length; ++i) {
      if (!bitMaps[i].isAllMarked()) {
        originColumnIndex2FilteredColumnIndexMapperList[i] = ++filteredCount;
      }
    }

    final String outputTimeSeries =
        outputDatabaseWithPathSeparator.isEmpty()
            ? timeSeries
            : outputDatabaseWithPathSeparator + timeSeries;

    if (filteredCount == columnNameStringList.length) {
      // No filter, collect rows
      for (int rowIndex = 0; rowIndex < distinctOutputs.size(); ++rowIndex) {
        collector.collectRow(
            rowIndex == 0
                ? new PipeResetTabletRow(
                    rowIndex,
                    outputTimeSeries,
                    false,
                    measurementSchemaList,
                    timestampColumn,
                    valueColumnTypes,
                    valueColumns,
                    bitMaps,
                    columnNameStringList)
                : new PipeRow(
                    rowIndex,
                    outputTimeSeries,
                    false,
                    measurementSchemaList,
                    timestampColumn,
                    valueColumnTypes,
                    valueColumns,
                    bitMaps,
                    columnNameStringList));
      }
    } else {
      // Recompute the column arrays
      final MeasurementSchema[] filteredMeasurementSchemaList =
          new MeasurementSchema[filteredCount];
      final String[] filteredColumnNameStringList = new String[filteredCount];
      final TSDataType[] filteredValueColumnTypes = new TSDataType[filteredCount];
      final Object[] filteredValueColumns = new Object[filteredCount];
      final BitMap[] filteredBitMaps = new BitMap[filteredCount];

      for (int i = 0; i < originColumnIndex2FilteredColumnIndexMapperList.length; i++) {
        if (originColumnIndex2FilteredColumnIndexMapperList[i] != null) {
          final int filteredColumnIndex = originColumnIndex2FilteredColumnIndexMapperList[i];
          filteredMeasurementSchemaList[filteredColumnIndex] = measurementSchemaList[i];
          filteredColumnNameStringList[filteredColumnIndex] = columnNameStringList[i];
          filteredValueColumnTypes[filteredColumnIndex] = valueColumnTypes[i];
          filteredBitMaps[filteredColumnIndex] = bitMaps[i];
          filteredValueColumns[filteredColumnIndex] = valueColumns[i];
        }
      }
      // Collect rows
      for (int rowIndex = 0; rowIndex < distinctOutputs.size(); ++rowIndex) {
        collector.collectRow(
            rowIndex == 0
                ? new PipeResetTabletRow(
                    rowIndex,
                    outputTimeSeries,
                    false,
                    filteredMeasurementSchemaList,
                    timestampColumn,
                    filteredValueColumnTypes,
                    filteredValueColumns,
                    filteredBitMaps,
                    filteredColumnNameStringList)
                : new PipeRow(
                    rowIndex,
                    outputTimeSeries,
                    false,
                    filteredMeasurementSchemaList,
                    timestampColumn,
                    filteredValueColumnTypes,
                    filteredValueColumns,
                    filteredBitMaps,
                    filteredColumnNameStringList));
      }
    }
  }

  @Override
  public void close() throws Exception {
    if (Objects.nonNull(pipeName)
        && pipeName2referenceCountMap.compute(
                pipeName, (name, count) -> Objects.nonNull(count) ? count - 1 : 0)
            == 0) {
      pipeName2timeSeries2TimeSeriesRuntimeStateMap.get(pipeName).clear();
      pipeName2timeSeries2TimeSeriesRuntimeStateMap.remove(pipeName);
      pipeName2LastValueReceiveTimeMap.remove(pipeName);
    }
    if (Objects.nonNull(windowingProcessor)) {
      windowingProcessor.close();
    }
    for (final PipeProcessor operatorProcessor : operatorProcessors) {
      operatorProcessor.close();
    }
  }
}
