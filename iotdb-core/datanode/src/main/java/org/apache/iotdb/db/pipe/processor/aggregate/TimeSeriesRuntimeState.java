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

import org.apache.iotdb.db.pipe.processor.aggregate.operator.aggregatedresult.AggregatedResultOperator;
import org.apache.iotdb.db.pipe.processor.aggregate.operator.intermediateresult.IntermediateResultOperator;
import org.apache.iotdb.db.pipe.processor.aggregate.window.datastructure.TimeSeriesWindow;
import org.apache.iotdb.db.pipe.processor.aggregate.window.datastructure.WindowOutput;
import org.apache.iotdb.db.pipe.processor.aggregate.window.datastructure.WindowState;
import org.apache.iotdb.db.pipe.processor.aggregate.window.processor.AbstractWindowingProcessor;
import org.apache.iotdb.pipe.api.type.Binary;

import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.PublicBAOS;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class TimeSeriesRuntimeState {
  // References
  private final Map<String, AggregatedResultOperator> aggregatorOutputName2OperatorMap;
  private final Map<String, Supplier<IntermediateResultOperator>>
      intermediateResultName2OperatorSupplierMap;
  private final Map<String, String> systemParameters;
  private final AbstractWindowingProcessor windowingProcessor;

  // Inner set values
  private long lastStateReportPhysicalTime;

  // Outer set values
  private long lastReportTimeStamp = Long.MIN_VALUE;
  private final List<TimeSeriesWindow> currentOpeningWindows = new ArrayList<>();

  // Variables to avoid "new" operation
  private final List<WindowOutput> outputList = new ArrayList<>();

  public TimeSeriesRuntimeState(
      final Map<String, AggregatedResultOperator> aggregatorOutputName2OperatorMap,
      final Map<String, Supplier<IntermediateResultOperator>>
          intermediateResultName2OperatorSupplierMap,
      final Map<String, String> systemParameters,
      final AbstractWindowingProcessor windowingProcessor) {
    this.aggregatorOutputName2OperatorMap = aggregatorOutputName2OperatorMap;
    this.intermediateResultName2OperatorSupplierMap = intermediateResultName2OperatorSupplierMap;
    this.systemParameters = systemParameters;
    this.windowingProcessor = windowingProcessor;
  }

  // The following "updateWindows" are the same except for the input value types
  public Pair<List<WindowOutput>, Pair<Long, ByteBuffer>> updateWindows(
      final long timestamp, final boolean value, final long outputMinReportIntervalMilliseconds)
      throws IOException {
    Pair<List<WindowOutput>, Pair<Long, ByteBuffer>> output = null;

    if (timestamp <= lastReportTimeStamp) {
      return null;
    }
    final Set<TimeSeriesWindow> addedWindows =
        windowingProcessor.mayAddWindow(currentOpeningWindows, timestamp, value);
    if (Objects.nonNull(addedWindows)) {
      addedWindows.forEach(
          window ->
              window.initWindow(
                  intermediateResultName2OperatorSupplierMap,
                  aggregatorOutputName2OperatorMap,
                  systemParameters));
    }
    final Iterator<TimeSeriesWindow> windowIterator = currentOpeningWindows.iterator();
    while (windowIterator.hasNext()) {
      final TimeSeriesWindow window = windowIterator.next();
      final Pair<WindowState, WindowOutput> stateWindowOutputPair =
          window.updateIntermediateResult(timestamp, value);
      if (Objects.isNull(stateWindowOutputPair)) {
        continue;
      }
      if (stateWindowOutputPair.getLeft().isEmit()
          && Objects.nonNull(stateWindowOutputPair.getRight())) {
        outputList.add(stateWindowOutputPair.getRight());
        lastReportTimeStamp =
            Math.max(lastReportTimeStamp, stateWindowOutputPair.getRight().getProgressTime());
      }
      if (stateWindowOutputPair.getLeft().isPurge()) {
        windowIterator.remove();
      }
    }
    if (!outputList.isEmpty()) {
      output =
          new Pair<>(
              new ArrayList<>(outputList),
              getTimestampWindowBufferPair(outputMinReportIntervalMilliseconds));
      outputList.clear();
    }
    return output;
  }

  public Pair<List<WindowOutput>, Pair<Long, ByteBuffer>> updateWindows(
      final long timestamp, final int value, final long outputMinReportIntervalMilliseconds)
      throws IOException {
    Pair<List<WindowOutput>, Pair<Long, ByteBuffer>> output = null;

    if (timestamp <= lastReportTimeStamp) {
      return null;
    }
    final Set<TimeSeriesWindow> addedWindows =
        windowingProcessor.mayAddWindow(currentOpeningWindows, timestamp, value);
    if (Objects.nonNull(addedWindows)) {
      addedWindows.forEach(
          window ->
              window.initWindow(
                  intermediateResultName2OperatorSupplierMap,
                  aggregatorOutputName2OperatorMap,
                  systemParameters));
    }
    final Iterator<TimeSeriesWindow> windowIterator = currentOpeningWindows.iterator();
    while (windowIterator.hasNext()) {
      final TimeSeriesWindow window = windowIterator.next();
      final Pair<WindowState, WindowOutput> stateWindowOutputPair =
          window.updateIntermediateResult(timestamp, value);
      if (Objects.isNull(stateWindowOutputPair)) {
        continue;
      }
      if (stateWindowOutputPair.getLeft().isEmit()
          && Objects.nonNull(stateWindowOutputPair.getRight())) {
        outputList.add(stateWindowOutputPair.getRight());
        lastReportTimeStamp =
            Math.max(lastReportTimeStamp, stateWindowOutputPair.getRight().getProgressTime());
      }
      if (stateWindowOutputPair.getLeft().isPurge()) {
        windowIterator.remove();
      }
    }
    if (!outputList.isEmpty()) {
      output =
          new Pair<>(
              new ArrayList<>(outputList),
              getTimestampWindowBufferPair(outputMinReportIntervalMilliseconds));
      outputList.clear();
    }
    return output;
  }

  public Pair<List<WindowOutput>, Pair<Long, ByteBuffer>> updateWindows(
      final long timestamp, final LocalDate value, final long outputMinReportIntervalMilliseconds)
      throws IOException {
    Pair<List<WindowOutput>, Pair<Long, ByteBuffer>> output = null;

    if (timestamp <= lastReportTimeStamp) {
      return null;
    }
    final Set<TimeSeriesWindow> addedWindows =
        windowingProcessor.mayAddWindow(currentOpeningWindows, timestamp, value);
    if (Objects.nonNull(addedWindows)) {
      addedWindows.forEach(
          window ->
              window.initWindow(
                  intermediateResultName2OperatorSupplierMap,
                  aggregatorOutputName2OperatorMap,
                  systemParameters));
    }
    final Iterator<TimeSeriesWindow> windowIterator = currentOpeningWindows.iterator();
    while (windowIterator.hasNext()) {
      final TimeSeriesWindow window = windowIterator.next();
      final Pair<WindowState, WindowOutput> stateWindowOutputPair =
          window.updateIntermediateResult(timestamp, value);
      if (Objects.isNull(stateWindowOutputPair)) {
        continue;
      }
      if (stateWindowOutputPair.getLeft().isEmit()
          && Objects.nonNull(stateWindowOutputPair.getRight())) {
        outputList.add(stateWindowOutputPair.getRight());
        lastReportTimeStamp =
            Math.max(lastReportTimeStamp, stateWindowOutputPair.getRight().getProgressTime());
      }
      if (stateWindowOutputPair.getLeft().isPurge()) {
        windowIterator.remove();
      }
    }
    if (!outputList.isEmpty()) {
      output =
          new Pair<>(
              new ArrayList<>(outputList),
              getTimestampWindowBufferPair(outputMinReportIntervalMilliseconds));
      outputList.clear();
    }
    return output;
  }

  public Pair<List<WindowOutput>, Pair<Long, ByteBuffer>> updateWindows(
      final long timestamp, final long value, final long outputMinReportIntervalMilliseconds)
      throws IOException {
    Pair<List<WindowOutput>, Pair<Long, ByteBuffer>> output = null;

    if (timestamp <= lastReportTimeStamp) {
      return null;
    }
    final Set<TimeSeriesWindow> addedWindows =
        windowingProcessor.mayAddWindow(currentOpeningWindows, timestamp, value);
    if (Objects.nonNull(addedWindows)) {
      addedWindows.forEach(
          window ->
              window.initWindow(
                  intermediateResultName2OperatorSupplierMap,
                  aggregatorOutputName2OperatorMap,
                  systemParameters));
    }
    final Iterator<TimeSeriesWindow> windowIterator = currentOpeningWindows.iterator();
    while (windowIterator.hasNext()) {
      final TimeSeriesWindow window = windowIterator.next();
      final Pair<WindowState, WindowOutput> stateWindowOutputPair =
          window.updateIntermediateResult(timestamp, value);
      if (Objects.isNull(stateWindowOutputPair)) {
        continue;
      }
      if (stateWindowOutputPair.getLeft().isEmit()
          && Objects.nonNull(stateWindowOutputPair.getRight())) {
        outputList.add(stateWindowOutputPair.getRight());
        lastReportTimeStamp =
            Math.max(lastReportTimeStamp, stateWindowOutputPair.getRight().getProgressTime());
      }
      if (stateWindowOutputPair.getLeft().isPurge()) {
        windowIterator.remove();
      }
    }
    if (!outputList.isEmpty()) {
      output =
          new Pair<>(
              new ArrayList<>(outputList),
              getTimestampWindowBufferPair(outputMinReportIntervalMilliseconds));
      outputList.clear();
    }
    return output;
  }

  public Pair<List<WindowOutput>, Pair<Long, ByteBuffer>> updateWindows(
      final long timestamp, final float value, final long outputMinReportIntervalMilliseconds)
      throws IOException {
    Pair<List<WindowOutput>, Pair<Long, ByteBuffer>> output = null;

    if (timestamp <= lastReportTimeStamp) {
      return null;
    }
    final Set<TimeSeriesWindow> addedWindows =
        windowingProcessor.mayAddWindow(currentOpeningWindows, timestamp, value);
    if (Objects.nonNull(addedWindows)) {
      addedWindows.forEach(
          window ->
              window.initWindow(
                  intermediateResultName2OperatorSupplierMap,
                  aggregatorOutputName2OperatorMap,
                  systemParameters));
    }
    final Iterator<TimeSeriesWindow> windowIterator = currentOpeningWindows.iterator();
    while (windowIterator.hasNext()) {
      final TimeSeriesWindow window = windowIterator.next();
      final Pair<WindowState, WindowOutput> stateWindowOutputPair =
          window.updateIntermediateResult(timestamp, value);
      if (Objects.isNull(stateWindowOutputPair)) {
        continue;
      }
      if (stateWindowOutputPair.getLeft().isEmit()
          && Objects.nonNull(stateWindowOutputPair.getRight())) {
        outputList.add(stateWindowOutputPair.getRight());
        lastReportTimeStamp =
            Math.max(lastReportTimeStamp, stateWindowOutputPair.getRight().getProgressTime());
      }
      if (stateWindowOutputPair.getLeft().isPurge()) {
        windowIterator.remove();
      }
    }
    if (!outputList.isEmpty()) {
      output =
          new Pair<>(
              new ArrayList<>(outputList),
              getTimestampWindowBufferPair(outputMinReportIntervalMilliseconds));
      outputList.clear();
    }
    return output;
  }

  public Pair<List<WindowOutput>, Pair<Long, ByteBuffer>> updateWindows(
      final long timestamp, final double value, final long outputMinReportIntervalMilliseconds)
      throws IOException {
    Pair<List<WindowOutput>, Pair<Long, ByteBuffer>> output = null;

    if (timestamp <= lastReportTimeStamp) {
      return null;
    }
    final Set<TimeSeriesWindow> addedWindows =
        windowingProcessor.mayAddWindow(currentOpeningWindows, timestamp, value);
    if (Objects.nonNull(addedWindows)) {
      addedWindows.forEach(
          window ->
              window.initWindow(
                  intermediateResultName2OperatorSupplierMap,
                  aggregatorOutputName2OperatorMap,
                  systemParameters));
    }
    final Iterator<TimeSeriesWindow> windowIterator = currentOpeningWindows.iterator();
    while (windowIterator.hasNext()) {
      final TimeSeriesWindow window = windowIterator.next();
      final Pair<WindowState, WindowOutput> stateWindowOutputPair =
          window.updateIntermediateResult(timestamp, value);
      if (Objects.isNull(stateWindowOutputPair)) {
        continue;
      }
      if (stateWindowOutputPair.getLeft().isEmit()
          && Objects.nonNull(stateWindowOutputPair.getRight())) {
        outputList.add(stateWindowOutputPair.getRight());
        lastReportTimeStamp =
            Math.max(lastReportTimeStamp, stateWindowOutputPair.getRight().getProgressTime());
      }
      if (stateWindowOutputPair.getLeft().isPurge()) {
        windowIterator.remove();
      }
    }
    if (!outputList.isEmpty()) {
      output =
          new Pair<>(
              new ArrayList<>(outputList),
              getTimestampWindowBufferPair(outputMinReportIntervalMilliseconds));
      outputList.clear();
    }
    return output;
  }

  public Pair<List<WindowOutput>, Pair<Long, ByteBuffer>> updateWindows(
      final long timestamp, final String value, final long outputMinReportIntervalMilliseconds)
      throws IOException {
    Pair<List<WindowOutput>, Pair<Long, ByteBuffer>> output = null;

    if (timestamp <= lastReportTimeStamp) {
      return null;
    }
    final Set<TimeSeriesWindow> addedWindows =
        windowingProcessor.mayAddWindow(currentOpeningWindows, timestamp, value);
    if (Objects.nonNull(addedWindows)) {
      addedWindows.forEach(
          window ->
              window.initWindow(
                  intermediateResultName2OperatorSupplierMap,
                  aggregatorOutputName2OperatorMap,
                  systemParameters));
    }
    final Iterator<TimeSeriesWindow> windowIterator = currentOpeningWindows.iterator();
    while (windowIterator.hasNext()) {
      final TimeSeriesWindow window = windowIterator.next();
      final Pair<WindowState, WindowOutput> stateWindowOutputPair =
          window.updateIntermediateResult(timestamp, value);
      if (Objects.isNull(stateWindowOutputPair)) {
        continue;
      }
      if (stateWindowOutputPair.getLeft().isEmit()
          && Objects.nonNull(stateWindowOutputPair.getRight())) {
        outputList.add(stateWindowOutputPair.getRight());
        lastReportTimeStamp =
            Math.max(lastReportTimeStamp, stateWindowOutputPair.getRight().getProgressTime());
      }
      if (stateWindowOutputPair.getLeft().isPurge()) {
        windowIterator.remove();
      }
    }
    if (!outputList.isEmpty()) {
      output =
          new Pair<>(
              new ArrayList<>(outputList),
              getTimestampWindowBufferPair(outputMinReportIntervalMilliseconds));
      outputList.clear();
    }
    return output;
  }

  public Pair<List<WindowOutput>, Pair<Long, ByteBuffer>> updateWindows(
      final long timestamp, final Binary value, final long outputMinReportIntervalMilliseconds)
      throws IOException {
    Pair<List<WindowOutput>, Pair<Long, ByteBuffer>> output = null;

    if (timestamp <= lastReportTimeStamp) {
      return null;
    }
    final Set<TimeSeriesWindow> addedWindows =
        windowingProcessor.mayAddWindow(currentOpeningWindows, timestamp, value);
    if (Objects.nonNull(addedWindows)) {
      addedWindows.forEach(
          window ->
              window.initWindow(
                  intermediateResultName2OperatorSupplierMap,
                  aggregatorOutputName2OperatorMap,
                  systemParameters));
    }
    final Iterator<TimeSeriesWindow> windowIterator = currentOpeningWindows.iterator();
    while (windowIterator.hasNext()) {
      final TimeSeriesWindow window = windowIterator.next();
      final Pair<WindowState, WindowOutput> stateWindowOutputPair =
          window.updateIntermediateResult(timestamp, value);
      if (Objects.isNull(stateWindowOutputPair)) {
        continue;
      }
      if (stateWindowOutputPair.getLeft().isEmit()
          && Objects.nonNull(stateWindowOutputPair.getRight())) {
        outputList.add(stateWindowOutputPair.getRight());
        lastReportTimeStamp =
            Math.max(lastReportTimeStamp, stateWindowOutputPair.getRight().getProgressTime());
      }
      if (stateWindowOutputPair.getLeft().isPurge()) {
        windowIterator.remove();
      }
    }
    if (!outputList.isEmpty()) {
      output =
          new Pair<>(
              new ArrayList<>(outputList),
              getTimestampWindowBufferPair(outputMinReportIntervalMilliseconds));
      outputList.clear();
    }
    return output;
  }

  public List<WindowOutput> forceOutput() {
    return currentOpeningWindows.stream()
        .map(TimeSeriesWindow::forceOutput)
        .collect(Collectors.toList());
  }

  // A runtime state with window buffer shall be reported after at least output min report
  // interval time to avoid frequent serialization
  // Return null if this should not report
  private Pair<Long, ByteBuffer> getTimestampWindowBufferPair(
      final long outputMinReportIntervalMilliseconds) throws IOException {
    if (currentOpeningWindows.isEmpty()) {
      return new Pair<>(lastReportTimeStamp, null);
    }
    if (System.currentTimeMillis() - lastStateReportPhysicalTime
        < outputMinReportIntervalMilliseconds) {
      return null;
    }
    try (final PublicBAOS byteArrayOutputStream = new PublicBAOS();
        final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      ReadWriteIOUtils.write(currentOpeningWindows.size(), outputStream);
      for (TimeSeriesWindow window : currentOpeningWindows) {
        window.serialize(outputStream);
      }
      lastStateReportPhysicalTime = System.currentTimeMillis();
      return new Pair<>(
          lastReportTimeStamp,
          ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size()));
    }
  }

  public void restoreTimestampAndWindows(final Pair<Long, ByteBuffer> timestampWindowBufferPair)
      throws IOException {
    if (timestampWindowBufferPair.getLeft() <= lastReportTimeStamp) {
      // The runtime state may be initialized by different processorSubtasks on one DataNode
      // Only the subtask with the largest timestamp wins
      return;
    }
    this.lastReportTimeStamp = timestampWindowBufferPair.getLeft();
    final ByteBuffer buffer = timestampWindowBufferPair.getRight();
    final int size = ReadWriteIOUtils.readInt(buffer);
    for (int i = 0; i < size; ++i) {
      // The runtime value will be deserialized if exists
      final TimeSeriesWindow currentWindow = new TimeSeriesWindow(windowingProcessor, null);
      currentWindow.initWindow(
          intermediateResultName2OperatorSupplierMap,
          aggregatorOutputName2OperatorMap,
          systemParameters);
      currentWindow.deserialize(buffer);
      currentOpeningWindows.add(currentWindow);
    }
  }
}
