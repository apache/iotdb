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

package org.apache.iotdb.db.storageengine.load.converter;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.pipe.PipeRuntimeOutOfMemoryCriticalException;
import org.apache.iotdb.commons.pipe.datastructure.pattern.IoTDBTreePattern;
import org.apache.iotdb.commons.pipe.datastructure.pattern.TreePattern;
import org.apache.iotdb.db.exception.load.LoadAnalyzeInvalidPathException;
import org.apache.iotdb.db.exception.load.LoadRuntimeOutOfMemoryException;
import org.apache.iotdb.db.i18n.StorageEngineMessages;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tsfile.parser.query.TsFileInsertionEventQueryParser;
import org.apache.iotdb.db.pipe.event.common.tsfile.parser.scan.TsFileInsertionEventScanParser;
import org.apache.iotdb.db.storageengine.load.memory.LoadTsFileParserMemoryManager;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;

import org.apache.tsfile.exception.PathParseException;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.TsFileSequenceReaderTimeseriesMetadataIterator;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.apache.iotdb.db.storageengine.load.LoadTsFilePathUtils.getValidatedDevicePath;

/**
 * Load uses scan parsing first for throughput. If scan parsing hits corruption, fall back to query
 * parsing for the remaining measurements and devices so later data can still be loaded. Report any
 * unrecoverable query failure after exhausting the readable data so a partial load cannot succeed.
 */
class LoadTreeTsFileTabletIterator
    implements Iterable<Pair<Tablet, Boolean>>, Iterator<Pair<Tablet, Boolean>>, AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(LoadTreeTsFileTabletIterator.class);

  private static final TreePattern LOAD_TREE_PATTERN =
      new IoTDBTreePattern(null) {
        @Override
        public boolean mayOverlapWithDevice(final IDeviceID device) {
          try {
            getValidatedDevicePath(device);
          } catch (final LoadAnalyzeInvalidPathException e) {
            throw new IllegalArgumentException(e.getMessage(), e);
          }
          return super.mayOverlapWithDevice(device);
        }
      };

  private final File file;
  private final boolean isWithMod;
  private final ArrayDeque<QueryTask> pendingQueryTasks = new ArrayDeque<>();

  private TsFileInsertionEventScanParser scanParser;
  private QueryTask activeQueryTask;
  private TsFileInsertionEventQueryParser activeQueryParser;
  private Iterator<Pair<Tablet, Boolean>> activeIterator;
  private boolean scanInitialized;
  private boolean fallbackTriggered;
  private RuntimeException unrecoverableQueryFailure;

  private final Map<IDeviceID, Map<String, Long>> lastEmittedTimestampsByDevice =
      new LinkedHashMap<>();

  LoadTreeTsFileTabletIterator(final File file, final boolean isWithMod) {
    this.file = file;
    this.isWithMod = isWithMod;
  }

  @Override
  public Iterator<Pair<Tablet, Boolean>> iterator() {
    return this;
  }

  @Override
  public boolean hasNext() {
    while (true) {
      try {
        ensureActiveIterator();
        if (Objects.isNull(activeIterator)) {
          return finishIteration();
        }

        if (activeIterator.hasNext()) {
          return true;
        }

        if (!switchToNextIterator()) {
          return finishIteration();
        }
      } catch (final Exception e) {
        if (recoverFromIteratorFailure(e)) {
          continue;
        }
        if (!shouldRethrow(e)) {
          close();
        }
        throw toRuntimeException(e);
      }
    }
  }

  @Override
  public Pair<Tablet, Boolean> next() {
    while (true) {
      if (!hasNext()) {
        close();
        throw new NoSuchElementException();
      }

      try {
        final Pair<Tablet, Boolean> next = activeIterator.next();
        recordProgress(next);
        return next;
      } catch (final Exception e) {
        if (recoverFromIteratorFailure(e)) {
          continue;
        }
        if (!shouldRethrow(e)) {
          close();
        }
        throw toRuntimeException(e);
      }
    }
  }

  private void ensureActiveIterator() throws Exception {
    if (Objects.nonNull(activeIterator)) {
      return;
    }

    if (!scanInitialized && !fallbackTriggered) {
      scanInitialized = true;
      try {
        scanParser =
            new TsFileInsertionEventScanParser(
                file,
                LOAD_TREE_PATTERN,
                Long.MIN_VALUE,
                Long.MAX_VALUE,
                null,
                null,
                isWithMod,
                LoadTsFileParserMemoryManager.getInstance());
        activeIterator = scanParser.toTabletWithIsAligneds().iterator();
        return;
      } catch (final Exception e) {
        if (shouldRethrow(e)) {
          scanInitialized = false;
          throw toRuntimeException(e);
        }
        if (!switchFromScanToQuery(e)) {
          throw toRuntimeException(e);
        }
      }
    }

    activateNextQueryParser();
  }

  private boolean switchToNextIterator() {
    if (Objects.nonNull(activeQueryParser)) {
      closeActiveQueryParser();
      return activateNextQueryParser();
    }

    closeScanParser();
    return activateNextQueryParser();
  }

  private boolean recoverFromIteratorFailure(final Exception e) {
    if (shouldRethrow(e)) {
      return false;
    }

    if (Objects.nonNull(activeQueryTask)) {
      LOGGER.warn(
          StorageEngineMessages
              .MESSAGE_LOAD_QUERY_FALLBACK_FAILED_FOR_DEVICE_ARG_MEASUREMENTS_ARG_IN_TSFILE_ARG_SPLIT_OR_SKIP_THIS_QUERY_TASK_AND_CONTINUE_3A4EA407,
          activeQueryTask.device,
          activeQueryTask.measurements,
          file.getAbsolutePath(),
          e);
      splitOrSkipActiveQueryTask(e);
      return true;
    }

    return switchFromScanToQuery(e);
  }

  private boolean switchFromScanToQuery(final Exception e) {
    if (fallbackTriggered) {
      return false;
    }

    fallbackTriggered = true;
    final IDeviceID currentDevice =
        Objects.nonNull(scanParser) ? scanParser.getCurrentDevice() : null;
    closeScanParser();

    try {
      pendingQueryTasks.addAll(buildQueryTasks());
    } catch (final Exception queryInitException) {
      LOGGER.warn(
          StorageEngineMessages
              .MESSAGE_LOAD_FAILED_TO_INITIALIZE_QUERY_FALLBACK_FOR_TSFILE_ARG_AFTER_SCAN_PARSER_FAILURE_143B6037,
          file.getAbsolutePath(),
          queryInitException);
      return false;
    }

    LOGGER.warn(
        StorageEngineMessages
            .MESSAGE_LOAD_SCAN_PARSER_DETECTED_A_CORRUPTED_SECTION_IN_TSFILE_ARG_AT_DEVICE_ARG_SWITCH_TO_QUERY_PARSING_FOR_REMAINING_DEVICES_EFA07985,
        file.getAbsolutePath(),
        currentDevice,
        e);
    return true;
  }

  private ArrayDeque<QueryTask> buildQueryTasks() throws IOException {
    final LinkedHashMap<IDeviceID, List<String>> deviceMeasurementsMap =
        readDeviceMeasurementsInOrder();
    if (deviceMeasurementsMap.isEmpty()) {
      return new ArrayDeque<>();
    }

    final ArrayDeque<QueryTask> tasks = new ArrayDeque<>();
    // Devices and measurements can recur in later chunk groups. The metadata index order does
    // not describe the scan position, so resume every series from its own emitted prefix.
    for (final Map.Entry<IDeviceID, List<String>> entry : deviceMeasurementsMap.entrySet()) {
      addQueryTasks(tasks, entry.getKey(), entry.getValue(), Long.MIN_VALUE, Long.MAX_VALUE);
    }

    return tasks;
  }

  private LinkedHashMap<IDeviceID, List<String>> readDeviceMeasurementsInOrder()
      throws IOException {
    final LinkedHashMap<IDeviceID, List<String>> deviceMeasurementsMap = new LinkedHashMap<>();
    try (final TsFileSequenceReader reader = new TsFileSequenceReader(file.getAbsolutePath())) {
      // iterAllTimeseriesMetadata treats an IOException as the end of iteration. Use the
      // throwing batch iterator so an incomplete metadata index cannot turn a partial load into a
      // success without rereading every device's metadata separately.
      final TsFileSequenceReaderTimeseriesMetadataIterator metadataIterator =
          new TsFileSequenceReaderTimeseriesMetadataIterator(reader, false);
      while (metadataIterator.hasNext()) {
        for (final Map.Entry<IDeviceID, List<TimeseriesMetadata>> deviceMetadata :
            metadataIterator.next().entrySet()) {
          final Map<String, Long> emittedTimestamps =
              lastEmittedTimestampsByDevice.getOrDefault(
                  deviceMetadata.getKey(), Collections.emptyMap());
          deviceMeasurementsMap
              .computeIfAbsent(deviceMetadata.getKey(), key -> new ArrayList<>())
              .addAll(
                  deviceMetadata.getValue().stream()
                      .filter(
                          metadata -> {
                            final Long emittedTimestamp =
                                emittedTimestamps.get(metadata.getMeasurementId());
                            return !metadata.getMeasurementId().isEmpty()
                                && (emittedTimestamp == null
                                    || emittedTimestamp < metadata.getStatistics().getEndTime());
                          })
                      .map(TimeseriesMetadata::getMeasurementId)
                      .collect(Collectors.toList()));
        }
      }
    }
    return deviceMeasurementsMap;
  }

  private void addQueryTasks(
      final ArrayDeque<QueryTask> tasks,
      final IDeviceID device,
      final List<String> measurements,
      final long startTime,
      final long endTime) {
    final Map<String, Long> emittedTimestamps =
        lastEmittedTimestampsByDevice.getOrDefault(device, Collections.emptyMap());
    final Map<Long, List<String>> measurementsByStartTime = new LinkedHashMap<>();
    for (final String measurement : measurements) {
      final Long emittedTimestamp = emittedTimestamps.get(measurement);
      if (emittedTimestamp != null && emittedTimestamp >= endTime) {
        continue;
      }
      // A missing checkpoint differs from a checkpoint at Long.MIN_VALUE. A start time of
      // Long.MAX_VALUE is also valid when the last unconsumed point has that timestamp.
      final long resumeStartTime =
          emittedTimestamp == null ? startTime : Math.max(startTime, emittedTimestamp + 1);
      if (resumeStartTime <= endTime) {
        measurementsByStartTime
            .computeIfAbsent(resumeStartTime, key -> new ArrayList<>())
            .add(measurement);
      }
    }

    measurementsByStartTime.forEach(
        (resumeStartTime, taskMeasurements) ->
            tasks.addLast(new QueryTask(device, taskMeasurements, resumeStartTime, endTime)));
  }

  private boolean activateNextQueryParser() {
    closeActiveQueryParser();

    while (!pendingQueryTasks.isEmpty()) {
      activeQueryTask = pendingQueryTasks.removeFirst();
      try {
        getValidatedDevicePath(activeQueryTask.device);
        activeQueryParser =
            new TsFileInsertionEventQueryParser(
                file,
                LOAD_TREE_PATTERN,
                activeQueryTask.startTime,
                activeQueryTask.endTime,
                activeQueryTask.toDeviceMeasurementsMap(),
                isWithMod,
                LoadTsFileParserMemoryManager.getInstance());
        final Iterator<TabletInsertionEvent> tabletIterator =
            activeQueryParser.toTabletInsertionEvents().iterator();
        activeIterator =
            new Iterator<Pair<Tablet, Boolean>>() {
              @Override
              public boolean hasNext() {
                return tabletIterator.hasNext();
              }

              @Override
              public Pair<Tablet, Boolean> next() {
                final TabletInsertionEvent event = tabletIterator.next();
                if (!(event instanceof PipeRawTabletInsertionEvent)) {
                  throw new IllegalStateException(
                      StorageEngineMessages
                              .EXCEPTION_EXPECTED_PIPERAWTABLETINSERTIONEVENT_BUT_GOT_D1D1DD05
                          + event.getClass().getName());
                }

                final PipeRawTabletInsertionEvent rawTabletInsertionEvent =
                    (PipeRawTabletInsertionEvent) event;
                return new Pair<>(
                    rawTabletInsertionEvent.convertToTablet(), rawTabletInsertionEvent.isAligned());
              }
            };
        return true;
      } catch (final Exception e) {
        if (shouldRethrow(e)) {
          pendingQueryTasks.addFirst(activeQueryTask);
          activeQueryTask = null;
          throw toRuntimeException(e);
        }
        LOGGER.warn(
            StorageEngineMessages
                .MESSAGE_LOAD_FAILED_TO_INITIALIZE_QUERY_FALLBACK_FOR_DEVICE_ARG_MEASUREMENTS_ARG_IN_TSFILE_ARG_SPLIT_OR_SKIP_THIS_QUERY_TASK_AND_CONTINUE_C6F69685,
            activeQueryTask.device,
            activeQueryTask.measurements,
            file.getAbsolutePath(),
            e);
        splitOrSkipActiveQueryTask(e);
      }
    }

    activeIterator = null;
    return false;
  }

  private void recordProgress(final Pair<Tablet, Boolean> tabletWithIsAligned) {
    final Tablet tablet = tabletWithIsAligned.getLeft();
    if (Objects.isNull(tablet) || tablet.getRowSize() == 0) {
      return;
    }

    final IDeviceID device = IDeviceID.Factory.DEFAULT_FACTORY.create(tablet.getDeviceId());
    final Map<String, Long> emittedTimestamps =
        lastEmittedTimestampsByDevice.computeIfAbsent(device, key -> new LinkedHashMap<>());
    final long timestamp = tablet.getTimestamp(tablet.getRowSize() - 1);
    for (final String measurement : extractMeasurementNames(tablet)) {
      emittedTimestamps.merge(measurement, timestamp, Math::max);
    }
  }

  private boolean shouldRethrow(final Exception e) {
    if (LoadTsFileDataTypeConverter.isMemoryPressureException(e)) {
      return true;
    }
    Throwable current = e;
    while (Objects.nonNull(current)) {
      if (current instanceof InterruptedException
          // Invalid paths cannot be recovered by query parsing or splitting measurements.
          || current instanceof PathParseException
          || current instanceof IllegalPathException
          || current instanceof LoadAnalyzeInvalidPathException
          || current instanceof PipeRuntimeOutOfMemoryCriticalException
          || current instanceof LoadRuntimeOutOfMemoryException) {
        return true;
      }
      current = current.getCause();
    }
    return false;
  }

  private RuntimeException toRuntimeException(final Exception e) {
    if (e instanceof LoadAnalyzeInvalidPathException) {
      return new IllegalArgumentException(e.getMessage(), e);
    }
    return e instanceof RuntimeException
        ? (RuntimeException) e
        : new IllegalStateException(
            StorageEngineMessages.EXCEPTION_FAILED_TO_ITERATE_TABLETS_WHILE_LOADING_TSFILE_4E6289F3,
            e);
  }

  private void closeScanParser() {
    activeIterator = null;
    if (Objects.nonNull(scanParser)) {
      scanParser.close();
      scanParser = null;
    }
  }

  private void closeActiveQueryParser() {
    activeIterator = null;
    activeQueryTask = null;
    if (Objects.isNull(activeQueryParser)) {
      return;
    }

    activeQueryParser.close();
    activeQueryParser = null;
  }

  @Override
  public void close() {
    activeIterator = null;
    closeScanParser();
    closeActiveQueryParser();
    pendingQueryTasks.clear();
  }

  private boolean finishIteration() {
    close();
    if (Objects.nonNull(unrecoverableQueryFailure)) {
      throw unrecoverableQueryFailure;
    }
    return false;
  }

  private void splitOrSkipActiveQueryTask(final Exception failure) {
    final QueryTask failedTask = activeQueryTask;
    closeActiveQueryParser();
    if (Objects.isNull(failedTask)) {
      return;
    }

    if (failedTask.measurements.size() <= 1) {
      // Keep salvaging later tasks, but retain the first failure so the caller preserves the
      // source.
      if (Objects.isNull(unrecoverableQueryFailure)) {
        unrecoverableQueryFailure = toRuntimeException(failure);
      }
      return;
    }

    final ArrayDeque<QueryTask> splitTasks = new ArrayDeque<>();
    for (final QueryTask task : failedTask.split(failedTask.startTime)) {
      addQueryTasks(splitTasks, task.device, task.measurements, task.startTime, task.endTime);
    }
    while (!splitTasks.isEmpty()) {
      pendingQueryTasks.addFirst(splitTasks.removeLast());
    }
  }

  private List<String> extractMeasurementNames(final Tablet tablet) {
    return IMeasurementSchema.getMeasurementNameList(tablet.getSchemas());
  }

  private static class QueryTask {
    private final IDeviceID device;
    private final List<String> measurements;
    private final long startTime;
    private final long endTime;

    private QueryTask(
        final IDeviceID device,
        final List<String> measurements,
        final long startTime,
        final long endTime) {
      this.device = device;
      this.measurements = new ArrayList<>(measurements);
      this.startTime = startTime;
      this.endTime = endTime;
    }

    private LinkedHashMap<IDeviceID, List<String>> toDeviceMeasurementsMap() {
      final LinkedHashMap<IDeviceID, List<String>> deviceMeasurementsMap = new LinkedHashMap<>();
      deviceMeasurementsMap.put(device, measurements);
      return deviceMeasurementsMap;
    }

    private List<QueryTask> split(final long resumeStartTime) {
      final int middle = measurements.size() / 2;
      if (middle <= 0) {
        return Collections.emptyList();
      }

      final List<QueryTask> splitTasks = new ArrayList<>(2);
      splitTasks.add(
          new QueryTask(device, measurements.subList(0, middle), resumeStartTime, endTime));
      splitTasks.add(
          new QueryTask(
              device, measurements.subList(middle, measurements.size()), resumeStartTime, endTime));
      return splitTasks;
    }
  }
}
