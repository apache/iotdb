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
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class TimeSeriesWindow {

  // A window is typically with a timestamp. We define it here to avoid
  // boxing/unboxing and simplify the logics.
  private long timestamp = 0;
  private Map<String, AggregatedResultOperator> aggregatedResultOperatorMap;
  private final Map<String, Pair<Boolean, IntermediateResultOperator>>
      intermediateResultName2InitializedAndOperatorMap = new HashMap<>();

  // WARNING: Using the customized runtime value may cause performance loss
  // due to boxing/unboxing issues.
  private Object customizedRuntimeValue;
  private final AbstractWindowingProcessor processor;

  public TimeSeriesWindow(AbstractWindowingProcessor processor, Object customizedRuntimeValue) {
    this.processor = processor;
    this.customizedRuntimeValue = customizedRuntimeValue;
  }

  /////////////////////////////// Getter/Setters for WindowProcessor ///////////////////////////////

  public long getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }

  public Object getCustomizedRuntimeValue() {
    return customizedRuntimeValue;
  }

  public void setCustomizedRuntimeValue(Object customizedRuntimeValue) {
    this.customizedRuntimeValue = customizedRuntimeValue;
  }

  /////////////////////////////// Calculation ///////////////////////////////

  public void initWindow(
      Map<String, Supplier<IntermediateResultOperator>> intermediateResult2OperatorSupplierMap,
      Map<String, AggregatedResultOperator> aggregatedResultOperatorMap) {
    for (Map.Entry<String, Supplier<IntermediateResultOperator>> entry :
        intermediateResult2OperatorSupplierMap.entrySet()) {
      intermediateResultName2InitializedAndOperatorMap.put(
          entry.getKey(), new Pair<>(false, entry.getValue().get()));
    }
    // Deep copy because some unsupported aggregated results may be removed
    this.aggregatedResultOperatorMap = new HashMap<>(aggregatedResultOperatorMap);
  }

  // Return the output and state of the window.
  // Return null if the state is normal to avoid boxing.
  public Pair<WindowState, WindowOutput> updateIntermediateResult(long timeStamp, boolean value) {
    Pair<WindowState, WindowOutput> stateOutputPair =
        processor.updateAndMaySetWindowState(this, timeStamp, value);
    WindowState state = stateOutputPair.getLeft();

    if (state == WindowState.IGNORE_DATA) {
      return null;
    }
    if (state.isEmit()) {
      stateOutputPair.getRight().setAggregatedResults(getAggregatedResults(TSDataType.BOOLEAN));
      return stateOutputPair;
    }

    // If not purge, continue to calculate even if the output is emitted.
    if (!state.isPurge()) {
      Iterator<Map.Entry<String, Pair<Boolean, IntermediateResultOperator>>> iterator =
          intermediateResultName2InitializedAndOperatorMap.entrySet().iterator();
      Map.Entry<String, Pair<Boolean, IntermediateResultOperator>> entry;

      while (iterator.hasNext()) {
        entry = iterator.next();
        final IntermediateResultOperator operator = entry.getValue().getRight();
        if (!entry.getValue().getLeft()) {
          if (!operator.initAndGetIsSupport(value, timeStamp)) {
            // Remove unsupported aggregated results
            aggregatedResultOperatorMap
                .entrySet()
                .removeIf(
                    entry1 ->
                        entry1
                            .getValue()
                            .getDeclaredIntermediateValueNames()
                            .contains(operator.getName()));
            // If no aggregated values can be calculated, purge the window
            if (aggregatedResultOperatorMap.isEmpty()) {
              return new Pair<>(WindowState.PURGE, null);
            }
            // Remove unsupported intermediate values
            iterator.remove();
            continue;
          }
          entry.getValue().setLeft(true);
        } else {
          operator.updateValue(value, timeStamp);
        }
      }
    }
    if (state.isEmit()) {
      return stateOutputPair;
    }
    return null;
  }

  // The same logic is repeated because java does not support basic type template :-)
  public Pair<WindowState, WindowOutput> updateIntermediateResult(long timeStamp, int value) {
    Pair<WindowState, WindowOutput> stateOutputPair =
        processor.updateAndMaySetWindowState(this, timeStamp, value);
    WindowState state = stateOutputPair.getLeft();

    if (state == WindowState.IGNORE_DATA) {
      return null;
    }
    if (state.isEmit()) {
      stateOutputPair.getRight().setAggregatedResults(getAggregatedResults(TSDataType.INT32));
      return stateOutputPair;
    }

    // If not purge, continue to calculate even if the output is emitted.
    if (!state.isPurge()) {
      Iterator<Map.Entry<String, Pair<Boolean, IntermediateResultOperator>>> iterator =
          intermediateResultName2InitializedAndOperatorMap.entrySet().iterator();
      Map.Entry<String, Pair<Boolean, IntermediateResultOperator>> entry;

      while (iterator.hasNext()) {
        entry = iterator.next();
        final IntermediateResultOperator operator = entry.getValue().getRight();
        if (!entry.getValue().getLeft()) {
          if (!operator.initAndGetIsSupport(value, timeStamp)) {
            // Remove unsupported aggregated results
            aggregatedResultOperatorMap
                .entrySet()
                .removeIf(
                    entry1 ->
                        entry1
                            .getValue()
                            .getDeclaredIntermediateValueNames()
                            .contains(operator.getName()));
            // If no aggregated values can be calculated, purge the window
            if (aggregatedResultOperatorMap.isEmpty()) {
              return new Pair<>(WindowState.PURGE, null);
            }
            // Remove unsupported intermediate values
            iterator.remove();
            continue;
          }
          entry.getValue().setLeft(true);
        } else {
          operator.updateValue(value, timeStamp);
        }
      }
    }
    if (state.isEmit()) {
      return stateOutputPair;
    }
    return null;
  }

  public Pair<WindowState, WindowOutput> updateIntermediateResult(long timeStamp, long value) {
    Pair<WindowState, WindowOutput> stateOutputPair =
        processor.updateAndMaySetWindowState(this, timeStamp, value);
    WindowState state = stateOutputPair.getLeft();

    if (state == WindowState.IGNORE_DATA) {
      return null;
    }
    if (state.isEmit()) {
      stateOutputPair.getRight().setAggregatedResults(getAggregatedResults(TSDataType.INT64));
      return stateOutputPair;
    }

    // If not purge, continue to calculate even if the output is emitted.
    if (!state.isPurge()) {
      Iterator<Map.Entry<String, Pair<Boolean, IntermediateResultOperator>>> iterator =
          intermediateResultName2InitializedAndOperatorMap.entrySet().iterator();
      Map.Entry<String, Pair<Boolean, IntermediateResultOperator>> entry;

      while (iterator.hasNext()) {
        entry = iterator.next();
        final IntermediateResultOperator operator = entry.getValue().getRight();
        if (!entry.getValue().getLeft()) {
          if (!operator.initAndGetIsSupport(value, timeStamp)) {
            // Remove unsupported aggregated results
            aggregatedResultOperatorMap
                .entrySet()
                .removeIf(
                    entry1 ->
                        entry1
                            .getValue()
                            .getDeclaredIntermediateValueNames()
                            .contains(operator.getName()));
            // If no aggregated values can be calculated, purge the window
            if (aggregatedResultOperatorMap.isEmpty()) {
              return new Pair<>(WindowState.PURGE, null);
            }
            // Remove unsupported intermediate values
            iterator.remove();
            continue;
          }
          entry.getValue().setLeft(true);
        } else {
          operator.updateValue(value, timeStamp);
        }
      }
    }
    if (state.isEmit()) {
      return stateOutputPair;
    }
    return null;
  }

  public Pair<WindowState, WindowOutput> updateIntermediateResult(long timeStamp, float value) {
    Pair<WindowState, WindowOutput> stateOutputPair =
        processor.updateAndMaySetWindowState(this, timeStamp, value);
    WindowState state = stateOutputPair.getLeft();

    if (state == WindowState.IGNORE_DATA) {
      return null;
    }
    if (state.isEmit()) {
      stateOutputPair.getRight().setAggregatedResults(getAggregatedResults(TSDataType.FLOAT));
      return stateOutputPair;
    }

    // If not purge, continue to calculate even if the output is emitted.
    if (!state.isPurge()) {
      Iterator<Map.Entry<String, Pair<Boolean, IntermediateResultOperator>>> iterator =
          intermediateResultName2InitializedAndOperatorMap.entrySet().iterator();
      Map.Entry<String, Pair<Boolean, IntermediateResultOperator>> entry;

      while (iterator.hasNext()) {
        entry = iterator.next();
        final IntermediateResultOperator operator = entry.getValue().getRight();
        if (!entry.getValue().getLeft()) {
          if (!operator.initAndGetIsSupport(value, timeStamp)) {
            // Remove unsupported aggregated results
            aggregatedResultOperatorMap
                .entrySet()
                .removeIf(
                    entry1 ->
                        entry1
                            .getValue()
                            .getDeclaredIntermediateValueNames()
                            .contains(operator.getName()));
            // If no aggregated values can be calculated, purge the window
            if (aggregatedResultOperatorMap.isEmpty()) {
              return new Pair<>(WindowState.PURGE, null);
            }
            // Remove unsupported intermediate values
            iterator.remove();
            continue;
          }
          entry.getValue().setLeft(true);
        } else {
          operator.updateValue(value, timeStamp);
        }
      }
    }
    if (state.isEmit()) {
      return stateOutputPair;
    }
    return null;
  }

  public Pair<WindowState, WindowOutput> updateIntermediateResult(long timeStamp, String value) {
    Pair<WindowState, WindowOutput> stateOutputPair =
        processor.updateAndMaySetWindowState(this, timeStamp, value);
    WindowState state = stateOutputPair.getLeft();

    if (state == WindowState.IGNORE_DATA) {
      return null;
    }
    if (state.isEmit()) {
      stateOutputPair.getRight().setAggregatedResults(getAggregatedResults(TSDataType.TEXT));
      return stateOutputPair;
    }

    // If not purge, continue to calculate even if the output is emitted.
    if (!state.isPurge()) {
      Iterator<Map.Entry<String, Pair<Boolean, IntermediateResultOperator>>> iterator =
          intermediateResultName2InitializedAndOperatorMap.entrySet().iterator();
      Map.Entry<String, Pair<Boolean, IntermediateResultOperator>> entry;

      while (iterator.hasNext()) {
        entry = iterator.next();
        final IntermediateResultOperator operator = entry.getValue().getRight();
        if (!entry.getValue().getLeft()) {
          if (!operator.initAndGetIsSupport(value, timeStamp)) {
            // Remove unsupported aggregated results
            aggregatedResultOperatorMap
                .entrySet()
                .removeIf(
                    entry1 ->
                        entry1
                            .getValue()
                            .getDeclaredIntermediateValueNames()
                            .contains(operator.getName()));
            // If no aggregated values can be calculated, purge the window
            if (aggregatedResultOperatorMap.isEmpty()) {
              return new Pair<>(WindowState.PURGE, null);
            }
            // Remove unsupported intermediate values
            iterator.remove();
            continue;
          }
          entry.getValue().setLeft(true);
        } else {
          operator.updateValue(value, timeStamp);
        }
      }
    }
    if (state.isEmit()) {
      return stateOutputPair;
    }
    return null;
  }

  public Pair<WindowState, WindowOutput> updateIntermediateResult(long timeStamp, double value) {
    Pair<WindowState, WindowOutput> stateOutputPair =
        processor.updateAndMaySetWindowState(this, timeStamp, value);
    WindowState state = stateOutputPair.getLeft();

    if (state == WindowState.IGNORE_DATA) {
      return null;
    }
    if (state.isEmit()) {
      stateOutputPair.getRight().setAggregatedResults(getAggregatedResults(TSDataType.DOUBLE));
      return stateOutputPair;
    }

    // If not purge, continue to calculate even if the output is emitted.
    if (!state.isPurge()) {
      Iterator<Map.Entry<String, Pair<Boolean, IntermediateResultOperator>>> iterator =
          intermediateResultName2InitializedAndOperatorMap.entrySet().iterator();
      Map.Entry<String, Pair<Boolean, IntermediateResultOperator>> entry;

      while (iterator.hasNext()) {
        entry = iterator.next();
        final IntermediateResultOperator operator = entry.getValue().getRight();
        if (!entry.getValue().getLeft()) {
          if (!operator.initAndGetIsSupport(value, timeStamp)) {
            // Remove unsupported aggregated results
            aggregatedResultOperatorMap
                .entrySet()
                .removeIf(
                    entry1 ->
                        entry1
                            .getValue()
                            .getDeclaredIntermediateValueNames()
                            .contains(operator.getName()));
            // If no aggregated values can be calculated, purge the window
            if (aggregatedResultOperatorMap.isEmpty()) {
              return new Pair<>(WindowState.PURGE, null);
            }
            // Remove unsupported intermediate values
            iterator.remove();
            continue;
          }
          entry.getValue().setLeft(true);
        } else {
          operator.updateValue(value, timeStamp);
        }
      }
    }
    if (state.isEmit()) {
      return stateOutputPair;
    }
    return null;
  }

  private Map<String, Pair<TSDataType, Object>> getAggregatedResults(TSDataType dataType) {
    CustomizedReadableIntermediateResults readableIntermediateResults =
        new CustomizedReadableIntermediateResults(
            intermediateResultName2InitializedAndOperatorMap.entrySet().stream()
                .collect(
                    Collectors.toMap(
                        Map.Entry::getKey, entry -> entry.getValue().getRight().getResult())));
    return aggregatedResultOperatorMap.entrySet().stream()
        .collect(
            Collectors.toMap(
                Map.Entry::getKey,
                entry -> entry.getValue().terminateWindow(dataType, readableIntermediateResults)));
  }

  /////////////////////////////// Ser/De logics ///////////////////////////////

  public void serialize(DataOutputStream outputStream) throws IOException {
    ReadWriteIOUtils.write(timestamp, outputStream);
    ReadWriteIOUtils.write(intermediateResultName2InitializedAndOperatorMap.size(), outputStream);
    for (Map.Entry<String, Pair<Boolean, IntermediateResultOperator>> entry :
        intermediateResultName2InitializedAndOperatorMap.entrySet()) {
      ReadWriteIOUtils.write(entry.getKey(), outputStream);
      entry.getValue().getRight().serialize(outputStream);
    }
    processor.serializeCustomizedAttributes(this, outputStream);
  }

  // Unlike normal deserialization method, the pipe may be altered before deserialization,
  // which means the operators may increase or decrease. Hence, we need to combine the
  // deserialized value and existing entries.
  // WARNING: We do not support removing intermediate values(e.g. intermediate values are
  // less after the aggregators decreased) in altering aggregate processor, only adding values
  // is permitted.
  public void deserialize(ByteBuffer byteBuffer) throws IOException {
    timestamp = ReadWriteIOUtils.readLong(byteBuffer);
    final int size = ReadWriteIOUtils.readInt(byteBuffer);
    for (int i = 0; i < size; i++) {
      final String intermediateResultName = ReadWriteIOUtils.readString(byteBuffer);
      Pair<Boolean, IntermediateResultOperator> initializedAndOperatorPair =
          intermediateResultName2InitializedAndOperatorMap.get(intermediateResultName);
      if (Objects.nonNull(initializedAndOperatorPair)) {
        initializedAndOperatorPair.setLeft(true);
        initializedAndOperatorPair.getRight().deserialize(byteBuffer);
      }
    }
    processor.deserializeCustomizedAttributes(this, byteBuffer);
  }
}
