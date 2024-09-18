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

package org.apache.iotdb.db.pipe.processor.aggregate.window.datastructure;

import org.apache.iotdb.db.pipe.processor.aggregate.operator.aggregatedresult.AggregatedResultOperator;
import org.apache.iotdb.db.pipe.processor.aggregate.operator.intermediateresult.CustomizedReadableIntermediateResults;
import org.apache.iotdb.db.pipe.processor.aggregate.operator.intermediateresult.IntermediateResultOperator;
import org.apache.iotdb.db.pipe.processor.aggregate.window.processor.AbstractWindowingProcessor;
import org.apache.iotdb.pipe.api.type.Binary;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class TimeSeriesWindow {
  private static final Logger LOGGER = LoggerFactory.getLogger(TimeSeriesWindow.class);

  // A window is typically with a timestamp. We define it here to avoid
  // boxing/unboxing and simplify the logics.
  private long timestamp = 0;
  private Map<String, AggregatedResultOperator> aggregatedOutputName2OperatorMap;
  private final Map<String, Pair<TSDataType, IntermediateResultOperator>>
      intermediateResultName2tsTypeAndOperatorMap = new HashMap<>();

  // WARNING: Using the customized runtime value may cause performance loss
  // due to boxing/unboxing issues.
  private Object customizedRuntimeValue;
  private final AbstractWindowingProcessor processor;

  public TimeSeriesWindow(
      final AbstractWindowingProcessor processor, final Object customizedRuntimeValue) {
    this.processor = processor;
    this.customizedRuntimeValue = customizedRuntimeValue;
  }

  /////////////////////////////// Getter/Setters for WindowProcessor ///////////////////////////////

  public long getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(final long timestamp) {
    this.timestamp = timestamp;
  }

  public Object getCustomizedRuntimeValue() {
    return customizedRuntimeValue;
  }

  public void setCustomizedRuntimeValue(final Object customizedRuntimeValue) {
    this.customizedRuntimeValue = customizedRuntimeValue;
  }

  /////////////////////////////// Calculation ///////////////////////////////

  public void initWindow(
      final Map<String, Supplier<IntermediateResultOperator>>
          intermediateResult2OperatorSupplierMap,
      final Map<String, AggregatedResultOperator> aggregatedResultOperatorMap,
      final Map<String, String> systemParameters) {
    for (final Map.Entry<String, Supplier<IntermediateResultOperator>> entry :
        intermediateResult2OperatorSupplierMap.entrySet()) {
      intermediateResultName2tsTypeAndOperatorMap.put(
          entry.getKey(), new Pair<>(TSDataType.UNKNOWN, entry.getValue().get()));
    }
    // Deep copy because some unsupported aggregated results may be removed
    this.aggregatedOutputName2OperatorMap = new HashMap<>(aggregatedResultOperatorMap);
    // Configure system parameters
    this.intermediateResultName2tsTypeAndOperatorMap.values().stream()
        .map(Pair::getRight)
        .forEach(operator -> operator.configureSystemParameters(systemParameters));
  }

  // Return the output and state of the window.
  // Return null if the state is normal to avoid boxing.
  public Pair<WindowState, WindowOutput> updateIntermediateResult(
      final long timestamp, final boolean value) {
    final Pair<WindowState, WindowOutput> stateOutputPair =
        processor.updateAndMaySetWindowState(this, timestamp, value);
    final WindowState state = stateOutputPair.getLeft();

    if (state.isEmitWithoutCompute()) {
      stateOutputPair.getRight().setAggregatedResults(getAggregatedResults(TSDataType.BOOLEAN));
    }

    if (state.isCalculate()) {
      final Iterator<Map.Entry<String, Pair<TSDataType, IntermediateResultOperator>>> iterator =
          intermediateResultName2tsTypeAndOperatorMap.entrySet().iterator();
      Map.Entry<String, Pair<TSDataType, IntermediateResultOperator>> entry;

      while (iterator.hasNext()) {
        entry = iterator.next();
        final IntermediateResultOperator operator = entry.getValue().getRight();
        if (entry.getValue().getLeft() == TSDataType.UNKNOWN) {
          if (!operator.initAndGetIsSupport(value, timestamp)) {
            // Remove unsupported aggregated results
            aggregatedOutputName2OperatorMap
                .entrySet()
                .removeIf(
                    entry1 ->
                        entry1
                            .getValue()
                            .getDeclaredIntermediateValueNames()
                            .contains(operator.getName()));
            // If no aggregated values can be calculated, purge the window
            if (aggregatedOutputName2OperatorMap.isEmpty()) {
              return new Pair<>(WindowState.PURGE, null);
            }
            // Remove unsupported intermediate values
            iterator.remove();
            continue;
          }
          entry.getValue().setLeft(TSDataType.BOOLEAN);
        } else if (entry.getValue().getLeft() != TSDataType.BOOLEAN) {
          LOGGER.warn(
              "Different data type encountered in one window, will purge. Previous type: {}, now type: {}",
              entry.getValue().getLeft(),
              TSDataType.BOOLEAN);
          return new Pair<>(WindowState.PURGE, null);
        } else {
          operator.updateValue(value, timestamp);
        }
      }
    }
    if (state.isEmitWithCompute()) {
      stateOutputPair.getRight().setAggregatedResults(getAggregatedResults(TSDataType.BOOLEAN));
    }
    return state.isEmit() ? stateOutputPair : null;
  }

  // The same logic is repeated because java does not support basic type template :-)
  public Pair<WindowState, WindowOutput> updateIntermediateResult(
      final long timestamp, final int value) {
    final Pair<WindowState, WindowOutput> stateOutputPair =
        processor.updateAndMaySetWindowState(this, timestamp, value);
    final WindowState state = stateOutputPair.getLeft();

    if (state.isEmitWithoutCompute()) {
      stateOutputPair.getRight().setAggregatedResults(getAggregatedResults(TSDataType.INT32));
    }

    if (state.isCalculate()) {
      final Iterator<Map.Entry<String, Pair<TSDataType, IntermediateResultOperator>>> iterator =
          intermediateResultName2tsTypeAndOperatorMap.entrySet().iterator();
      Map.Entry<String, Pair<TSDataType, IntermediateResultOperator>> entry;

      while (iterator.hasNext()) {
        entry = iterator.next();
        final IntermediateResultOperator operator = entry.getValue().getRight();
        if (entry.getValue().getLeft() == TSDataType.UNKNOWN) {
          if (!operator.initAndGetIsSupport(value, timestamp)) {
            // Remove unsupported aggregated results
            aggregatedOutputName2OperatorMap
                .entrySet()
                .removeIf(
                    entry1 ->
                        entry1
                            .getValue()
                            .getDeclaredIntermediateValueNames()
                            .contains(operator.getName()));
            // If no aggregated values can be calculated, purge the window
            if (aggregatedOutputName2OperatorMap.isEmpty()) {
              return new Pair<>(WindowState.PURGE, null);
            }
            // Remove unsupported intermediate values
            iterator.remove();
            continue;
          }
          entry.getValue().setLeft(TSDataType.INT32);
        } else if (entry.getValue().getLeft() != TSDataType.INT32) {
          LOGGER.warn(
              "Different data type encountered in one window, will purge. Previous type: {}, now type: {}",
              entry.getValue().getLeft(),
              TSDataType.INT32);
          return new Pair<>(WindowState.PURGE, null);
        } else {
          operator.updateValue(value, timestamp);
        }
      }
    }
    if (state.isEmitWithCompute()) {
      stateOutputPair.getRight().setAggregatedResults(getAggregatedResults(TSDataType.INT32));
    }
    return state.isEmit() ? stateOutputPair : null;
  }

  public Pair<WindowState, WindowOutput> updateIntermediateResult(
      final long timestamp, final LocalDate value) {
    final Pair<WindowState, WindowOutput> stateOutputPair =
        processor.updateAndMaySetWindowState(this, timestamp, value);
    final WindowState state = stateOutputPair.getLeft();

    if (state.isEmitWithoutCompute()) {
      stateOutputPair.getRight().setAggregatedResults(getAggregatedResults(TSDataType.DATE));
    }

    if (state.isCalculate()) {
      final Iterator<Map.Entry<String, Pair<TSDataType, IntermediateResultOperator>>> iterator =
          intermediateResultName2tsTypeAndOperatorMap.entrySet().iterator();
      Map.Entry<String, Pair<TSDataType, IntermediateResultOperator>> entry;

      while (iterator.hasNext()) {
        entry = iterator.next();
        final IntermediateResultOperator operator = entry.getValue().getRight();
        if (entry.getValue().getLeft() == TSDataType.UNKNOWN) {
          if (!operator.initAndGetIsSupport(value, timestamp)) {
            // Remove unsupported aggregated results
            aggregatedOutputName2OperatorMap
                .entrySet()
                .removeIf(
                    entry1 ->
                        entry1
                            .getValue()
                            .getDeclaredIntermediateValueNames()
                            .contains(operator.getName()));
            // If no aggregated values can be calculated, purge the window
            if (aggregatedOutputName2OperatorMap.isEmpty()) {
              return new Pair<>(WindowState.PURGE, null);
            }
            // Remove unsupported intermediate values
            iterator.remove();
            continue;
          }
          entry.getValue().setLeft(TSDataType.DATE);
        } else if (entry.getValue().getLeft() != TSDataType.DATE) {
          LOGGER.warn(
              "Different data type encountered in one window, will purge. Previous type: {}, now type: {}",
              entry.getValue().getLeft(),
              TSDataType.DATE);
          return new Pair<>(WindowState.PURGE, null);
        } else {
          operator.updateValue(value, timestamp);
        }
      }
    }
    if (state.isEmitWithCompute()) {
      stateOutputPair.getRight().setAggregatedResults(getAggregatedResults(TSDataType.DATE));
    }
    return state.isEmit() ? stateOutputPair : null;
  }

  public Pair<WindowState, WindowOutput> updateIntermediateResult(
      final long timestamp, final long value) {
    final Pair<WindowState, WindowOutput> stateOutputPair =
        processor.updateAndMaySetWindowState(this, timestamp, value);
    final WindowState state = stateOutputPair.getLeft();

    if (state.isEmitWithoutCompute()) {
      stateOutputPair.getRight().setAggregatedResults(getAggregatedResults(TSDataType.INT64));
    }

    if (state.isCalculate()) {
      final Iterator<Map.Entry<String, Pair<TSDataType, IntermediateResultOperator>>> iterator =
          intermediateResultName2tsTypeAndOperatorMap.entrySet().iterator();
      Map.Entry<String, Pair<TSDataType, IntermediateResultOperator>> entry;

      while (iterator.hasNext()) {
        entry = iterator.next();
        final IntermediateResultOperator operator = entry.getValue().getRight();
        if (entry.getValue().getLeft() == TSDataType.UNKNOWN) {
          if (!operator.initAndGetIsSupport(value, timestamp)) {
            // Remove unsupported aggregated results
            aggregatedOutputName2OperatorMap
                .entrySet()
                .removeIf(
                    entry1 ->
                        entry1
                            .getValue()
                            .getDeclaredIntermediateValueNames()
                            .contains(operator.getName()));
            // If no aggregated values can be calculated, purge the window
            if (aggregatedOutputName2OperatorMap.isEmpty()) {
              return new Pair<>(WindowState.PURGE, null);
            }
            // Remove unsupported intermediate values
            iterator.remove();
            continue;
          }
          entry.getValue().setLeft(TSDataType.INT64);
        } else if (entry.getValue().getLeft() != TSDataType.INT64) {
          LOGGER.warn(
              "Different data type encountered in one window, will purge. Previous type: {}, now type: {}",
              entry.getValue().getLeft(),
              TSDataType.INT64);
          return new Pair<>(WindowState.PURGE, null);
        } else {
          operator.updateValue(value, timestamp);
        }
      }
    }
    if (state.isEmitWithCompute()) {
      stateOutputPair.getRight().setAggregatedResults(getAggregatedResults(TSDataType.INT64));
    }
    return state.isEmit() ? stateOutputPair : null;
  }

  public Pair<WindowState, WindowOutput> updateIntermediateResult(
      final long timestamp, final float value) {
    final Pair<WindowState, WindowOutput> stateOutputPair =
        processor.updateAndMaySetWindowState(this, timestamp, value);
    final WindowState state = stateOutputPair.getLeft();

    if (state.isEmitWithoutCompute()) {
      stateOutputPair.getRight().setAggregatedResults(getAggregatedResults(TSDataType.FLOAT));
    }

    if (state.isCalculate()) {
      final Iterator<Map.Entry<String, Pair<TSDataType, IntermediateResultOperator>>> iterator =
          intermediateResultName2tsTypeAndOperatorMap.entrySet().iterator();
      Map.Entry<String, Pair<TSDataType, IntermediateResultOperator>> entry;

      while (iterator.hasNext()) {
        entry = iterator.next();
        final IntermediateResultOperator operator = entry.getValue().getRight();
        if (entry.getValue().getLeft() == TSDataType.UNKNOWN) {
          if (!operator.initAndGetIsSupport(value, timestamp)) {
            // Remove unsupported aggregated results
            aggregatedOutputName2OperatorMap
                .entrySet()
                .removeIf(
                    entry1 ->
                        entry1
                            .getValue()
                            .getDeclaredIntermediateValueNames()
                            .contains(operator.getName()));
            // If no aggregated values can be calculated, purge the window
            if (aggregatedOutputName2OperatorMap.isEmpty()) {
              return new Pair<>(WindowState.PURGE, null);
            }
            // Remove unsupported intermediate values
            iterator.remove();
            continue;
          }
          entry.getValue().setLeft(TSDataType.FLOAT);
        } else if (entry.getValue().getLeft() != TSDataType.FLOAT) {
          LOGGER.warn(
              "Different data type encountered in one window, will purge. Previous type: {}, now type: {}",
              entry.getValue().getLeft(),
              TSDataType.FLOAT);
          return new Pair<>(WindowState.PURGE, null);
        } else {
          operator.updateValue(value, timestamp);
        }
      }
    }
    if (state.isEmitWithCompute()) {
      stateOutputPair.getRight().setAggregatedResults(getAggregatedResults(TSDataType.FLOAT));
    }
    return state.isEmit() ? stateOutputPair : null;
  }

  public Pair<WindowState, WindowOutput> updateIntermediateResult(
      final long timestamp, final double value) {
    final Pair<WindowState, WindowOutput> stateOutputPair =
        processor.updateAndMaySetWindowState(this, timestamp, value);
    final WindowState state = stateOutputPair.getLeft();

    if (state.isEmitWithoutCompute()) {
      stateOutputPair.getRight().setAggregatedResults(getAggregatedResults(TSDataType.DOUBLE));
    }
    if (state.isCalculate()) {
      final Iterator<Map.Entry<String, Pair<TSDataType, IntermediateResultOperator>>> iterator =
          intermediateResultName2tsTypeAndOperatorMap.entrySet().iterator();
      Map.Entry<String, Pair<TSDataType, IntermediateResultOperator>> entry;

      while (iterator.hasNext()) {
        entry = iterator.next();
        final IntermediateResultOperator operator = entry.getValue().getRight();
        if (entry.getValue().getLeft() == TSDataType.UNKNOWN) {
          if (!operator.initAndGetIsSupport(value, timestamp)) {
            // Remove unsupported aggregated results
            aggregatedOutputName2OperatorMap
                .entrySet()
                .removeIf(
                    entry1 ->
                        entry1
                            .getValue()
                            .getDeclaredIntermediateValueNames()
                            .contains(operator.getName()));
            // If no aggregated values can be calculated, purge the window
            if (aggregatedOutputName2OperatorMap.isEmpty()) {
              return new Pair<>(WindowState.PURGE, null);
            }
            // Remove unsupported intermediate values
            iterator.remove();
            continue;
          }
          entry.getValue().setLeft(TSDataType.DOUBLE);
        } else if (entry.getValue().getLeft() != TSDataType.DOUBLE) {
          LOGGER.warn(
              "Different data type encountered in one window, will purge. Previous type: {}, now type: {}",
              entry.getValue().getLeft(),
              TSDataType.DOUBLE);
          return new Pair<>(WindowState.PURGE, null);
        } else {
          operator.updateValue(value, timestamp);
        }
      }
    }
    if (state.isEmitWithCompute()) {
      stateOutputPair.getRight().setAggregatedResults(getAggregatedResults(TSDataType.DOUBLE));
    }
    return state.isEmit() ? stateOutputPair : null;
  }

  public Pair<WindowState, WindowOutput> updateIntermediateResult(
      final long timestamp, final String value) {
    final Pair<WindowState, WindowOutput> stateOutputPair =
        processor.updateAndMaySetWindowState(this, timestamp, value);
    final WindowState state = stateOutputPair.getLeft();

    if (state.isEmitWithoutCompute()) {
      stateOutputPair.getRight().setAggregatedResults(getAggregatedResults(TSDataType.TEXT));
    }
    if (state.isCalculate()) {
      final Iterator<Map.Entry<String, Pair<TSDataType, IntermediateResultOperator>>> iterator =
          intermediateResultName2tsTypeAndOperatorMap.entrySet().iterator();
      Map.Entry<String, Pair<TSDataType, IntermediateResultOperator>> entry;

      while (iterator.hasNext()) {
        entry = iterator.next();
        final IntermediateResultOperator operator = entry.getValue().getRight();
        if (entry.getValue().getLeft() == TSDataType.UNKNOWN) {
          if (!operator.initAndGetIsSupport(value, timestamp)) {
            // Remove unsupported aggregated results
            aggregatedOutputName2OperatorMap
                .entrySet()
                .removeIf(
                    entry1 ->
                        entry1
                            .getValue()
                            .getDeclaredIntermediateValueNames()
                            .contains(operator.getName()));
            // If no aggregated values can be calculated, purge the window
            if (aggregatedOutputName2OperatorMap.isEmpty()) {
              return new Pair<>(WindowState.PURGE, null);
            }
            // Remove unsupported intermediate values
            iterator.remove();
            continue;
          }
          entry.getValue().setLeft(TSDataType.TEXT);
        } else if (entry.getValue().getLeft() != TSDataType.TEXT) {
          LOGGER.warn(
              "Different data type encountered in one window, will purge. Previous type: {}, now type: {}",
              entry.getValue().getLeft(),
              TSDataType.TEXT);
          return new Pair<>(WindowState.PURGE, null);
        } else {
          operator.updateValue(value, timestamp);
        }
      }
    }
    if (state.isEmitWithCompute()) {
      stateOutputPair.getRight().setAggregatedResults(getAggregatedResults(TSDataType.TEXT));
    }
    return state.isEmit() ? stateOutputPair : null;
  }

  public Pair<WindowState, WindowOutput> updateIntermediateResult(
      final long timestamp, final Binary value) {
    final Pair<WindowState, WindowOutput> stateOutputPair =
        processor.updateAndMaySetWindowState(this, timestamp, value);
    final WindowState state = stateOutputPair.getLeft();

    if (state.isEmitWithoutCompute()) {
      stateOutputPair.getRight().setAggregatedResults(getAggregatedResults(TSDataType.BLOB));
    }
    if (state.isCalculate()) {
      final Iterator<Map.Entry<String, Pair<TSDataType, IntermediateResultOperator>>> iterator =
          intermediateResultName2tsTypeAndOperatorMap.entrySet().iterator();
      Map.Entry<String, Pair<TSDataType, IntermediateResultOperator>> entry;

      while (iterator.hasNext()) {
        entry = iterator.next();
        final IntermediateResultOperator operator = entry.getValue().getRight();
        if (entry.getValue().getLeft() == TSDataType.UNKNOWN) {
          if (!operator.initAndGetIsSupport(value, timestamp)) {
            // Remove unsupported aggregated results
            aggregatedOutputName2OperatorMap
                .entrySet()
                .removeIf(
                    entry1 ->
                        entry1
                            .getValue()
                            .getDeclaredIntermediateValueNames()
                            .contains(operator.getName()));
            // If no aggregated values can be calculated, purge the window
            if (aggregatedOutputName2OperatorMap.isEmpty()) {
              return new Pair<>(WindowState.PURGE, null);
            }
            // Remove unsupported intermediate values
            iterator.remove();
            continue;
          }
          entry.getValue().setLeft(TSDataType.BLOB);
        } else if (entry.getValue().getLeft() != TSDataType.BLOB) {
          LOGGER.warn(
              "Different data type encountered in one window, will purge. Previous type: {}, now type: {}",
              entry.getValue().getLeft(),
              TSDataType.BLOB);
          return new Pair<>(WindowState.PURGE, null);
        } else {
          operator.updateValue(value, timestamp);
        }
      }
    }
    if (state.isEmitWithCompute()) {
      stateOutputPair.getRight().setAggregatedResults(getAggregatedResults(TSDataType.BLOB));
    }
    return state.isEmit() ? stateOutputPair : null;
  }

  public WindowOutput forceOutput() {
    return processor
        .forceOutput(this)
        .setAggregatedResults(
            getAggregatedResults(
                intermediateResultName2tsTypeAndOperatorMap.values().stream()
                    .findFirst()
                    .map(Pair::getLeft)
                    .orElse(TSDataType.UNKNOWN)));
  }

  private Map<String, Pair<TSDataType, Object>> getAggregatedResults(final TSDataType dataType) {
    // The remaining intermediate results' datatype shall all be equal to this
    // If not, return nothing
    if (dataType == TSDataType.UNKNOWN
        || intermediateResultName2tsTypeAndOperatorMap.entrySet().stream()
            .anyMatch(entry -> entry.getValue().getLeft() != dataType)) {
      return Collections.emptyMap();
    }
    final CustomizedReadableIntermediateResults readableIntermediateResults =
        new CustomizedReadableIntermediateResults(
            intermediateResultName2tsTypeAndOperatorMap.entrySet().stream()
                .collect(
                    Collectors.toMap(
                        Map.Entry::getKey, entry -> entry.getValue().getRight().getResult())));
    return aggregatedOutputName2OperatorMap.entrySet().stream()
        .collect(
            Collectors.toMap(
                Map.Entry::getKey,
                entry -> entry.getValue().terminateWindow(dataType, readableIntermediateResults)));
  }

  /////////////////////////////// Ser/De logics ///////////////////////////////

  public void serialize(final DataOutputStream outputStream) throws IOException {
    ReadWriteIOUtils.write(timestamp, outputStream);
    ReadWriteIOUtils.write(intermediateResultName2tsTypeAndOperatorMap.size(), outputStream);
    for (final Map.Entry<String, Pair<TSDataType, IntermediateResultOperator>> entry :
        intermediateResultName2tsTypeAndOperatorMap.entrySet()) {
      ReadWriteIOUtils.write(entry.getKey(), outputStream);
      entry.getValue().getLeft().serializeTo(outputStream);
      entry.getValue().getRight().serialize(outputStream);
    }
    processor.serializeCustomizedAttributes(this, outputStream);
  }

  // Unlike normal deserialization method, the pipe may be altered before deserialization,
  // which means the operators may increase or decrease. Hence, we need to combine the
  // deserialized value and existing entries.
  // WARNING: We do not support removing intermediate values (e.g. intermediate values are
  // less after the aggregators decreased) or altering windowing processor in altering aggregate
  // processor, only adding intermediate values is permitted.
  public void deserialize(final ByteBuffer byteBuffer) throws IOException {
    timestamp = ReadWriteIOUtils.readLong(byteBuffer);
    final int size = ReadWriteIOUtils.readInt(byteBuffer);
    for (int i = 0; i < size; i++) {
      final String intermediateResultName = ReadWriteIOUtils.readString(byteBuffer);
      final Pair<TSDataType, IntermediateResultOperator> initializedAndOperatorPair =
          intermediateResultName2tsTypeAndOperatorMap.get(intermediateResultName);
      if (Objects.nonNull(initializedAndOperatorPair)) {
        initializedAndOperatorPair.setLeft(TSDataType.deserializeFrom(byteBuffer));
        initializedAndOperatorPair.getRight().deserialize(byteBuffer);
      }
    }
    processor.deserializeCustomizedAttributes(this, byteBuffer);
  }
}
