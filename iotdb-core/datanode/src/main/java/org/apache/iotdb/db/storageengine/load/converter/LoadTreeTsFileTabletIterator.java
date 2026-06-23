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

import org.apache.iotdb.commons.exception.pipe.PipeRuntimeOutOfMemoryCriticalException;
import org.apache.iotdb.commons.pipe.datastructure.pattern.IoTDBPipePattern;
import org.apache.iotdb.commons.pipe.datastructure.pattern.PipePattern;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tsfile.container.query.TsFileInsertionQueryDataContainer;
import org.apache.iotdb.db.pipe.event.common.tsfile.container.scan.TsFileInsertionScanDataContainer;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.PlainDeviceID;
import org.apache.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Load uses scan parsing first for throughput. If scan parsing hits corruption, fall back to query
 * parsing for the remaining measurements and devices so later data can still be loaded.
 */
class LoadTreeTsFileTabletIterator
    implements Iterable<Pair<Tablet, Boolean>>, Iterator<Pair<Tablet, Boolean>>, AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(LoadTreeTsFileTabletIterator.class);

  private static final PipePattern LOAD_TREE_PATTERN = new IoTDBPipePattern(null);

  private final File file;
  private final boolean isWithMod;
  private final ArrayDeque<QueryTask> pendingQueryTasks = new ArrayDeque<>();

  private TsFileInsertionScanDataContainer scanParser;
  private QueryTask activeQueryTask;
  private TsFileInsertionQueryDataContainer activeQueryParser;
  private Iterator<Pair<Tablet, Boolean>> activeIterator;
  private boolean scanInitialized;
  private boolean fallbackTriggered;

  private IDeviceID lastEmittedDevice;
  private List<String> lastEmittedMeasurements = Collections.emptyList();
  private long lastEmittedTimestamp = Long.MIN_VALUE;

  private IDeviceID lastScanTabletDevice;
  private List<String> lastScanTabletMeasurements = Collections.emptyList();
  private final Map<IDeviceID, Set<String>> fullyEmittedMeasurementsByDevice =
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
          close();
          return false;
        }

        if (activeIterator.hasNext()) {
          return true;
        }

        if (!switchToNextIterator()) {
          close();
          return false;
        }
      } catch (final Exception e) {
        if (recoverFromIteratorFailure(e)) {
          continue;
        }
        close();
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
        close();
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
            new TsFileInsertionScanDataContainer(
                file, LOAD_TREE_PATTERN, Long.MIN_VALUE, Long.MAX_VALUE, null, null, isWithMod);
        activeIterator = scanParser.toTabletWithIsAligneds().iterator();
        return;
      } catch (final Exception e) {
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
          "Load: Query fallback failed for device {} measurements {} in TsFile {}. "
              + "Split or skip this query task and continue.",
          activeQueryTask.device,
          activeQueryTask.measurements,
          file.getAbsolutePath(),
          e);
      splitOrSkipActiveQueryTask();
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
    final List<String> currentMeasurements =
        Objects.nonNull(scanParser) ? scanParser.getCurrentMeasurements() : Collections.emptyList();

    markLastScanMeasurementsAsCompletedIfNeeded(currentDevice, currentMeasurements);

    closeScanParser();

    try {
      pendingQueryTasks.addAll(buildQueryTasks(currentDevice, currentMeasurements));
    } catch (final Exception queryInitException) {
      LOGGER.warn(
          "Load: Failed to initialize query fallback for TsFile {} after scan parser failure.",
          file.getAbsolutePath(),
          queryInitException);
      return false;
    }

    LOGGER.warn(
        "Load: Scan parser detected a corrupted section in TsFile {} at device {}. "
            + "Switch to query parsing for remaining devices.",
        file.getAbsolutePath(),
        currentDevice,
        e);
    return true;
  }

  private ArrayDeque<QueryTask> buildQueryTasks(
      final IDeviceID currentDevice, final List<String> currentMeasurements) throws IOException {
    final LinkedHashMap<IDeviceID, List<String>> deviceMeasurementsMap =
        readDeviceMeasurementsInOrder();
    if (deviceMeasurementsMap.isEmpty()) {
      return new ArrayDeque<>();
    }

    final ArrayDeque<QueryTask> tasks = new ArrayDeque<>();
    boolean includeCurrentAndFollowingDevices =
        Objects.isNull(currentDevice) || !deviceMeasurementsMap.containsKey(currentDevice);

    for (final Map.Entry<IDeviceID, List<String>> entry : deviceMeasurementsMap.entrySet()) {
      final IDeviceID device = entry.getKey();
      if (!includeCurrentAndFollowingDevices && device.equals(currentDevice)) {
        includeCurrentAndFollowingDevices = true;
      }
      if (!includeCurrentAndFollowingDevices) {
        continue;
      }

      if (device.equals(currentDevice)) {
        addCurrentDeviceQueryTasks(tasks, device, entry.getValue(), currentMeasurements);
      } else {
        addQueryTaskIfNecessary(tasks, device, entry.getValue(), Long.MIN_VALUE, Long.MAX_VALUE);
      }
    }

    return tasks;
  }

  private LinkedHashMap<IDeviceID, List<String>> readDeviceMeasurementsInOrder()
      throws IOException {
    final LinkedHashMap<IDeviceID, List<String>> deviceMeasurementsMap = new LinkedHashMap<>();
    try (final TsFileSequenceReader reader = new TsFileSequenceReader(file.getAbsolutePath())) {
      final Iterator<Pair<IDeviceID, List<TimeseriesMetadata>>> metadataIterator =
          reader.iterAllTimeseriesMetadata(false, false);
      while (metadataIterator.hasNext()) {
        final Pair<IDeviceID, List<TimeseriesMetadata>> deviceMetadata = metadataIterator.next();
        deviceMeasurementsMap.put(
            deviceMetadata.getLeft(),
            deviceMetadata.getRight().stream()
                .map(TimeseriesMetadata::getMeasurementId)
                .collect(Collectors.toList()));
      }
    }
    return deviceMeasurementsMap;
  }

  private void addCurrentDeviceQueryTasks(
      final ArrayDeque<QueryTask> tasks,
      final IDeviceID device,
      final List<String> allMeasurements,
      final List<String> currentMeasurements) {
    final Set<String> completedMeasurements =
        fullyEmittedMeasurementsByDevice.getOrDefault(device, Collections.emptySet());
    final Set<String> currentMeasurementSet = new LinkedHashSet<>(currentMeasurements);

    final List<String> currentMeasurementsToResume = new ArrayList<>();
    final List<String> remainingMeasurements = new ArrayList<>();
    for (final String measurement : allMeasurements) {
      if (completedMeasurements.contains(measurement)) {
        continue;
      }
      if (currentMeasurementSet.contains(measurement)) {
        currentMeasurementsToResume.add(measurement);
      } else {
        remainingMeasurements.add(measurement);
      }
    }

    addQueryTaskIfNecessary(
        tasks,
        device,
        currentMeasurementsToResume,
        determineTaskResumeStartTime(device, currentMeasurementsToResume, Long.MIN_VALUE),
        Long.MAX_VALUE);
    addQueryTaskIfNecessary(tasks, device, remainingMeasurements, Long.MIN_VALUE, Long.MAX_VALUE);
  }

  private boolean activateNextQueryParser() {
    closeActiveQueryParser();

    while (!pendingQueryTasks.isEmpty()) {
      activeQueryTask = pendingQueryTasks.removeFirst();
      try {
        activeQueryParser =
            new TsFileInsertionQueryDataContainer(
                file,
                LOAD_TREE_PATTERN,
                activeQueryTask.startTime,
                activeQueryTask.endTime,
                activeQueryTask.toDeviceMeasurementsMap(),
                isWithMod);
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
                      "Expected PipeRawTabletInsertionEvent but got " + event.getClass().getName());
                }

                final PipeRawTabletInsertionEvent rawTabletInsertionEvent =
                    (PipeRawTabletInsertionEvent) event;
                return new Pair<>(
                    rawTabletInsertionEvent.convertToTablet(), rawTabletInsertionEvent.isAligned());
              }
            };
        return true;
      } catch (final Exception e) {
        LOGGER.warn(
            "Load: Failed to initialize query fallback for device {} measurements {} in TsFile {}. "
                + "Split or skip this query task and continue.",
            activeQueryTask.device,
            activeQueryTask.measurements,
            file.getAbsolutePath(),
            e);
        splitOrSkipActiveQueryTask();
      }
    }

    activeIterator = null;
    return false;
  }

  private void recordProgress(final Pair<Tablet, Boolean> tabletWithIsAligned) {
    final Tablet tablet = tabletWithIsAligned.getLeft();
    if (Objects.isNull(tablet) || tablet.rowSize == 0) {
      return;
    }

    final IDeviceID device = new PlainDeviceID(tablet.deviceId);
    final List<String> measurements = extractMeasurementNames(tablet);

    if (Objects.isNull(activeQueryParser)) {
      recordScanProgress(device, measurements);
    }

    lastEmittedDevice = device;
    lastEmittedMeasurements = measurements;
    lastEmittedTimestamp = tablet.timestamps[tablet.rowSize - 1];
  }

  private boolean shouldRethrow(final Exception e) {
    Throwable current = e;
    while (Objects.nonNull(current)) {
      if (current instanceof InterruptedException
          || current instanceof PipeRuntimeOutOfMemoryCriticalException) {
        return true;
      }
      current = current.getCause();
    }
    return false;
  }

  private RuntimeException toRuntimeException(final Exception e) {
    return e instanceof RuntimeException
        ? (RuntimeException) e
        : new IllegalStateException("Failed to iterate tablets while loading TsFile.", e);
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

  private void recordScanProgress(final IDeviceID device, final List<String> measurements) {
    if (Objects.nonNull(lastScanTabletDevice)
        && (!lastScanTabletDevice.equals(device)
            || !measurementsEqual(lastScanTabletMeasurements, measurements))) {
      markMeasurementsFullyEmitted(lastScanTabletDevice, lastScanTabletMeasurements);
    }

    lastScanTabletDevice = device;
    lastScanTabletMeasurements = measurements;
  }

  private void markLastScanMeasurementsAsCompletedIfNeeded(
      final IDeviceID currentDevice, final List<String> currentMeasurements) {
    if (Objects.isNull(lastScanTabletDevice) || lastScanTabletMeasurements.isEmpty()) {
      return;
    }

    if (!lastScanTabletDevice.equals(currentDevice)
        || !currentMeasurements.isEmpty()
            && !measurementsEqual(lastScanTabletMeasurements, currentMeasurements)) {
      markMeasurementsFullyEmitted(lastScanTabletDevice, lastScanTabletMeasurements);
    }
  }

  private void markMeasurementsFullyEmitted(
      final IDeviceID device, final List<String> measurements) {
    if (Objects.isNull(device) || measurements.isEmpty()) {
      return;
    }

    fullyEmittedMeasurementsByDevice
        .computeIfAbsent(device, key -> new LinkedHashSet<>())
        .addAll(measurements);
  }

  private long determineTaskResumeStartTime(
      final IDeviceID device, final List<String> measurements, final long defaultStartTime) {
    if (measurements.isEmpty()
        || !device.equals(lastEmittedDevice)
        || lastEmittedTimestamp == Long.MIN_VALUE
        || !measurementsEqual(measurements, lastEmittedMeasurements)) {
      return defaultStartTime;
    }

    return lastEmittedTimestamp == Long.MAX_VALUE ? Long.MAX_VALUE : lastEmittedTimestamp + 1;
  }

  private void addQueryTaskIfNecessary(
      final ArrayDeque<QueryTask> tasks,
      final IDeviceID device,
      final List<String> measurements,
      final long startTime,
      final long endTime) {
    if (measurements.isEmpty() || startTime == Long.MAX_VALUE) {
      return;
    }

    tasks.addLast(new QueryTask(device, measurements, startTime, endTime));
  }

  private void splitOrSkipActiveQueryTask() {
    final QueryTask failedTask = activeQueryTask;
    closeActiveQueryParser();
    if (Objects.isNull(failedTask)) {
      return;
    }

    if (failedTask.measurements.size() <= 1) {
      return;
    }

    final long resumeStartTime =
        determineTaskResumeStartTime(
            failedTask.device, failedTask.measurements, failedTask.startTime);
    final List<QueryTask> splitTasks = failedTask.split(resumeStartTime);
    for (int i = splitTasks.size() - 1; i >= 0; --i) {
      pendingQueryTasks.addFirst(splitTasks.get(i));
    }
  }

  private List<String> extractMeasurementNames(final Tablet tablet) {
    return tablet.getSchemas().stream()
        .map(MeasurementSchema::getMeasurementId)
        .collect(Collectors.toList());
  }

  private boolean measurementsEqual(
      final List<String> leftMeasurements, final List<String> rightMeasurements) {
    return leftMeasurements.size() == rightMeasurements.size()
        && new LinkedHashSet<>(leftMeasurements).equals(new LinkedHashSet<>(rightMeasurements));
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
