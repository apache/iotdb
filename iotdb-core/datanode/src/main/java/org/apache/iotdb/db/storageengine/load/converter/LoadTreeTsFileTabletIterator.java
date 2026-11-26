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
import org.apache.iotdb.commons.path.PatternTreeMap;
import org.apache.iotdb.commons.pipe.datastructure.pattern.IoTDBPipePattern;
import org.apache.iotdb.commons.pipe.datastructure.pattern.PipePattern;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.exception.LoadAnalyzeInvalidPathException;
import org.apache.iotdb.db.exception.load.LoadRuntimeOutOfMemoryException;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tsfile.container.query.TsFileInsertionQueryDataContainer;
import org.apache.iotdb.db.pipe.event.common.tsfile.container.scan.TsFileInsertionScanDataContainer;
import org.apache.iotdb.db.pipe.event.common.tsfile.parser.util.ModsOperationUtil;
import org.apache.iotdb.db.pipe.event.common.tsfile.parser.util.ModsOperationUtil.ModsInfo;
import org.apache.iotdb.db.storageengine.dataregion.modification.Modification;
import org.apache.iotdb.db.storageengine.load.memory.LoadTsFileParserMemoryManager;
import org.apache.iotdb.db.utils.datastructure.PatternTreeMapFactory;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;

import org.apache.tsfile.exception.PathParseException;
import org.apache.tsfile.file.metadata.AlignedChunkMetadata;
import org.apache.tsfile.file.metadata.ChunkMetadata;
import org.apache.tsfile.file.metadata.IChunkMetadata;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.MetadataIndexNode;
import org.apache.tsfile.file.metadata.PlainDeviceID;
import org.apache.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.tsfile.read.TsFileDeviceIterator;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.TsFileSequenceReaderTimeseriesMetadataIterator;
import org.apache.tsfile.read.common.BatchData;
import org.apache.tsfile.read.common.Chunk;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.read.reader.IChunkReader;
import org.apache.tsfile.read.reader.chunk.AlignedChunkReader;
import org.apache.tsfile.read.reader.chunk.ChunkReader;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.TsPrimitiveType;
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

  private static final PipePattern LOAD_TREE_PATTERN =
      new IoTDBPipePattern(null) {
        @Override
        public boolean mayOverlapWithDevice(final String device) {
          try {
            getValidatedDevicePath(new PlainDeviceID(device));
          } catch (final LoadAnalyzeInvalidPathException e) {
            throw new IllegalArgumentException(e.getMessage(), e);
          }
          return super.mayOverlapWithDevice(device);
        }
      };

  private final File file;
  private final boolean isWithMod;
  private final ScanParserFactory scanParserFactory;
  private final ArrayDeque<QueryTask> pendingQueryTasks = new ArrayDeque<>();

  private TsFileInsertionScanDataContainer scanParser;
  private QueryTask activeQueryTask;
  private TsFileInsertionQueryDataContainer activeQueryParser;
  private Iterator<Pair<Tablet, Boolean>> activeIterator;
  private boolean scanInitialized;
  private boolean fallbackTriggered;
  private RuntimeException unrecoverableQueryFailure;
  private Boolean hasModifications;

  private final Map<IDeviceID, Map<String, Long>> lastEmittedTimestampsByDevice =
      new LinkedHashMap<>();
  private final Map<IDeviceID, Map<String, Long>> emittedPointCountsByDevice =
      new LinkedHashMap<>();
  private final Map<IDeviceID, Map<String, Long>> expectedQueryFallbackPointCountsByDevice =
      new LinkedHashMap<>();
  private final Map<IDeviceID, Map<String, Long>> queryFallbackPointCountsByDevice =
      new LinkedHashMap<>();

  LoadTreeTsFileTabletIterator(final File file, final boolean isWithMod) {
    this(
        file,
        isWithMod,
        sourceFile ->
            new TsFileInsertionScanDataContainer(
                sourceFile,
                LOAD_TREE_PATTERN,
                Long.MIN_VALUE,
                Long.MAX_VALUE,
                null,
                null,
                isWithMod,
                LoadTsFileParserMemoryManager.getInstance(),
                true));
  }

  @TestOnly
  LoadTreeTsFileTabletIterator(
      final File file, final boolean isWithMod, final ScanParserFactory scanParserFactory) {
    this.file = file;
    this.isWithMod = isWithMod;
    this.scanParserFactory = scanParserFactory;
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
        scanParser = scanParserFactory.create(file);
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

    final Map<IDeviceID, Map<String, Long>> parsedPointCounts =
        Objects.nonNull(scanParser) ? scanParser.getParsedPointCounts() : Collections.emptyMap();
    final Map<IDeviceID, Map<String, Long>> expectedPointCounts =
        Objects.nonNull(scanParser) ? scanParser.getExpectedPointCounts() : Collections.emptyMap();
    closeScanParser();
    if (fallbackTriggered) {
      return activateNextQueryParser();
    }

    try {
      final ArrayDeque<QueryTask> queryTasks =
          buildQueryTasks(parsedPointCounts, expectedPointCounts);
      if (!queryTasks.isEmpty()) {
        fallbackTriggered = true;
        pendingQueryTasks.addAll(queryTasks);
      }
    } catch (final Exception e) {
      fallbackTriggered = true;
      unrecoverableQueryFailure = toRuntimeException(e);
      return false;
    }
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
      pendingQueryTasks.addAll(buildQueryTasks(emittedPointCountsByDevice, null));
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
      final Map<IDeviceID, Map<String, Long>> parsedPointCounts,
      final Map<IDeviceID, Map<String, Long>> scannedExpectedPointCounts)
      throws IOException {
    final ArrayDeque<QueryTask> tasks = new ArrayDeque<>();
    final Map<IDeviceID, Map<String, Long>> metadataPointCounts = readMetadataPointCountsByDevice();
    final Map<IDeviceID, Map<String, Long>> expectedPointCounts =
        hasModifications()
            ? Objects.nonNull(scannedExpectedPointCounts)
                    && pointCountsMatch(parsedPointCounts, metadataPointCounts)
                ? scannedExpectedPointCounts
                : readExpectedPointCountsWithMods(metadataPointCounts)
            : metadataPointCounts;

    appendQueryTasksForIncompleteMeasurements(
        tasks, expectedPointCounts, emittedPointCountsByDevice);
    verifyCompletedPointCounts(emittedPointCountsByDevice, expectedPointCounts);
    return tasks;
  }

  private Map<IDeviceID, Map<String, Long>> readMetadataPointCountsByDevice() throws IOException {
    final Map<IDeviceID, Map<String, Long>> pointCountsByDevice = new LinkedHashMap<>();
    try (final TsFileSequenceReader reader = new TsFileSequenceReader(file.getAbsolutePath())) {
      final TsFileSequenceReaderTimeseriesMetadataIterator metadataIterator =
          new TsFileSequenceReaderTimeseriesMetadataIterator(reader, false);
      while (metadataIterator.hasNext()) {
        for (final Map.Entry<IDeviceID, List<TimeseriesMetadata>> deviceMetadata :
            metadataIterator.next().entrySet()) {
          for (final TimeseriesMetadata metadata : deviceMetadata.getValue()) {
            final String measurement = metadata.getMeasurementId();
            final long pointCount = metadata.getStatistics().getCount();
            if (measurement.isEmpty() || pointCount == 0) {
              continue;
            }
            pointCountsByDevice
                .computeIfAbsent(deviceMetadata.getKey(), key -> new LinkedHashMap<>())
                .merge(measurement, pointCount, Long::sum);
          }
        }
      }
    }
    return pointCountsByDevice;
  }

  private void appendQueryTasksForIncompleteMeasurements(
      final ArrayDeque<QueryTask> tasks,
      final Map<IDeviceID, Map<String, Long>> expectedPointCountsByDevice,
      final Map<IDeviceID, Map<String, Long>> completedPointCountsByDevice)
      throws IOException {
    for (final Map.Entry<IDeviceID, Map<String, Long>> deviceEntry :
        expectedPointCountsByDevice.entrySet()) {
      final IDeviceID device = deviceEntry.getKey();
      final Map<String, Long> completedPointCounts =
          completedPointCountsByDevice.getOrDefault(device, Collections.emptyMap());
      final List<String> fallbackMeasurements = new ArrayList<>();
      final Map<String, Long> fallbackPointCounts = new LinkedHashMap<>();
      for (final Map.Entry<String, Long> measurementEntry : deviceEntry.getValue().entrySet()) {
        final String measurement = measurementEntry.getKey();
        final long expected = measurementEntry.getValue();
        final long completed = completedPointCounts.getOrDefault(measurement, 0L);
        if (completed > expected) {
          throw new IOException("Load point count mismatch");
        }
        if (completed < expected) {
          fallbackMeasurements.add(measurement);
          fallbackPointCounts.put(measurement, expected - completed);
        }
      }
      if (!fallbackMeasurements.isEmpty()) {
        expectedQueryFallbackPointCountsByDevice.put(device, fallbackPointCounts);
        addQueryTasks(tasks, device, fallbackMeasurements, Long.MIN_VALUE, Long.MAX_VALUE);
      }
    }
  }

  private void verifyCompletedPointCounts(
      final Map<IDeviceID, Map<String, Long>> completedPointCountsByDevice,
      final Map<IDeviceID, Map<String, Long>> expectedPointCountsByDevice)
      throws IOException {
    for (final Map.Entry<IDeviceID, Map<String, Long>> deviceEntry :
        completedPointCountsByDevice.entrySet()) {
      final Map<String, Long> expectedPointCounts =
          expectedPointCountsByDevice.getOrDefault(deviceEntry.getKey(), Collections.emptyMap());
      for (final Map.Entry<String, Long> measurementEntry : deviceEntry.getValue().entrySet()) {
        if (measurementEntry.getValue()
            > expectedPointCounts.getOrDefault(measurementEntry.getKey(), 0L)) {
          throw new IOException();
        }
      }
    }
  }

  private boolean pointCountsMatch(
      final Map<IDeviceID, Map<String, Long>> actualPointCountsByDevice,
      final Map<IDeviceID, Map<String, Long>> expectedPointCountsByDevice) {
    for (final Map.Entry<IDeviceID, Map<String, Long>> deviceEntry :
        actualPointCountsByDevice.entrySet()) {
      if (!measurementPointCountsMatch(
          deviceEntry.getValue(),
          expectedPointCountsByDevice.getOrDefault(deviceEntry.getKey(), Collections.emptyMap()))) {
        return false;
      }
    }
    for (final Map.Entry<IDeviceID, Map<String, Long>> deviceEntry :
        expectedPointCountsByDevice.entrySet()) {
      if (!measurementPointCountsMatch(
          deviceEntry.getValue(),
          actualPointCountsByDevice.getOrDefault(deviceEntry.getKey(), Collections.emptyMap()))) {
        return false;
      }
    }
    return true;
  }

  private boolean measurementPointCountsMatch(
      final Map<String, Long> actualPointCounts, final Map<String, Long> expectedPointCounts) {
    for (final Map.Entry<String, Long> measurementEntry : actualPointCounts.entrySet()) {
      if (!Objects.equals(
          measurementEntry.getValue(),
          expectedPointCounts.getOrDefault(measurementEntry.getKey(), 0L))) {
        return false;
      }
    }
    for (final Map.Entry<String, Long> measurementEntry : expectedPointCounts.entrySet()) {
      if (!Objects.equals(
          measurementEntry.getValue(),
          actualPointCounts.getOrDefault(measurementEntry.getKey(), 0L))) {
        return false;
      }
    }
    return true;
  }

  private Map<IDeviceID, Map<String, Long>> readExpectedPointCountsWithMods(
      final Map<IDeviceID, Map<String, Long>> metadataPointCountsByDevice) throws IOException {
    final PatternTreeMap<Modification, PatternTreeMapFactory.ModsSerializer> modifications =
        ModsOperationUtil.loadModificationsFromTsFile(file);
    final Map<IDeviceID, Map<String, Long>> expectedPointCountsByDevice = new LinkedHashMap<>();

    try (final TsFileSequenceReader reader = new TsFileSequenceReader(file.getAbsolutePath())) {
      final TsFileDeviceIterator deviceIterator = reader.getAllDevicesIteratorWithIsAligned();
      while (deviceIterator.hasNext()) {
        final Pair<IDeviceID, Boolean> deviceWithAlignment = deviceIterator.next();
        final IDeviceID device = deviceWithAlignment.getLeft();
        final MetadataIndexNode metadataIndexNode =
            deviceIterator.getFirstMeasurementNodeOfCurrentDevice();
        final List<String> measurements =
            new ArrayList<>(
                reader
                    .getTimeseriesMetadataOffsetByDevice(
                        metadataIndexNode, Collections.emptySet(), false)
                    .keySet());
        measurements.removeIf(String::isEmpty);
        final List<ModsInfo> modsInfos =
            ModsOperationUtil.initializeMeasurementMods(
                device.toString(), measurements, modifications);
        final Map<String, Long> parsedPointCounts = new LinkedHashMap<>();
        final Map<String, Long> expectedPointCounts = new LinkedHashMap<>();

        if (Boolean.TRUE.equals(deviceWithAlignment.getRight())) {
          countAlignedPoints(
              reader,
              device,
              metadataIndexNode,
              measurements,
              modsInfos,
              parsedPointCounts,
              expectedPointCounts);
        } else {
          countNonAlignedPoints(
              reader, device, measurements, modsInfos, parsedPointCounts, expectedPointCounts);
        }

        for (final String measurement : measurements) {
          if (!Objects.equals(
              parsedPointCounts.getOrDefault(measurement, 0L),
              metadataPointCountsByDevice
                  .getOrDefault(device, Collections.emptyMap())
                  .getOrDefault(measurement, 0L))) {
            throw new IOException();
          }
        }
        expectedPointCountsByDevice.put(device, expectedPointCounts);
      }
    }
    return expectedPointCountsByDevice;
  }

  private void countAlignedPoints(
      final TsFileSequenceReader reader,
      final IDeviceID device,
      final MetadataIndexNode metadataIndexNode,
      final List<String> measurements,
      final List<ModsInfo> modsInfos,
      final Map<String, Long> parsedPointCounts,
      final Map<String, Long> expectedPointCounts)
      throws IOException {
    for (final AlignedChunkMetadata alignedChunkMetadata :
        reader.getAlignedChunkMetadataByMetadataIndexNode(device, metadataIndexNode)) {
      final List<IChunkMetadata> valueChunkMetadata =
          alignedChunkMetadata.getValueChunkMetadataList();
      if (valueChunkMetadata.size() != measurements.size()) {
        throw new IOException();
      }

      final Chunk timeChunk =
          reader.readMemChunk((ChunkMetadata) alignedChunkMetadata.getTimeChunkMetadata());
      final List<Chunk> valueChunks = new ArrayList<>(valueChunkMetadata.size());
      for (final IChunkMetadata metadata : valueChunkMetadata) {
        valueChunks.add(metadata == null ? null : reader.readMemChunk((ChunkMetadata) metadata));
      }
      final IChunkReader chunkReader = new AlignedChunkReader(timeChunk, valueChunks);
      while (chunkReader.hasNextSatisfiedPage()) {
        final BatchData pageData = chunkReader.nextPageData();
        while (pageData.hasCurrent()) {
          final TsPrimitiveType[] values = pageData.getVector();
          if (values.length != measurements.size()) {
            throw new IOException();
          }
          for (int column = 0; column < values.length; column++) {
            if (values[column] != null) {
              final String measurement = measurements.get(column);
              parsedPointCounts.merge(measurement, 1L, Long::sum);
              if (!ModsOperationUtil.isDelete(pageData.currentTime(), modsInfos.get(column))) {
                expectedPointCounts.merge(measurement, 1L, Long::sum);
              }
            }
          }
          pageData.next();
        }
      }
    }
  }

  private void countNonAlignedPoints(
      final TsFileSequenceReader reader,
      final IDeviceID device,
      final List<String> measurements,
      final List<ModsInfo> modsInfos,
      final Map<String, Long> parsedPointCounts,
      final Map<String, Long> expectedPointCounts)
      throws IOException {
    for (int index = 0; index < measurements.size(); index++) {
      final String measurement = measurements.get(index);
      for (final ChunkMetadata metadata :
          reader.getChunkMetadataList(new Path(device.toString(), measurement, false), true)) {
        final IChunkReader chunkReader = new ChunkReader(reader.readMemChunk(metadata));
        while (chunkReader.hasNextSatisfiedPage()) {
          final BatchData pageData = chunkReader.nextPageData();
          while (pageData.hasCurrent()) {
            parsedPointCounts.merge(measurement, 1L, Long::sum);
            if (!ModsOperationUtil.isDelete(pageData.currentTime(), modsInfos.get(index))) {
              expectedPointCounts.merge(measurement, 1L, Long::sum);
            }
            pageData.next();
          }
        }
      }
    }
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

  private boolean hasModifications() throws IOException {
    if (hasModifications == null) {
      hasModifications =
          isWithMod && !ModsOperationUtil.loadModificationsFromTsFile(file).isEmpty();
    }
    return hasModifications;
  }

  private boolean activateNextQueryParser() {
    closeActiveQueryParser();

    while (!pendingQueryTasks.isEmpty()) {
      activeQueryTask = pendingQueryTasks.removeFirst();
      try {
        getValidatedDevicePath(activeQueryTask.device);
        activeQueryParser =
            new TsFileInsertionQueryDataContainer(
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
        if (shouldRethrow(e)) {
          pendingQueryTasks.addFirst(activeQueryTask);
          activeQueryTask = null;
          throw toRuntimeException(e);
        }
        LOGGER.warn(
            "Load: Failed to initialize query fallback for device {} measurements {} in TsFile {}. "
                + "Split or skip this query task and continue.",
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
    if (Objects.isNull(tablet) || tablet.rowSize == 0) {
      return;
    }

    final IDeviceID device = new PlainDeviceID(tablet.deviceId);
    final Map<String, Long> emittedTimestamps =
        lastEmittedTimestampsByDevice.computeIfAbsent(device, key -> new LinkedHashMap<>());
    final Map<String, Long> emittedPointCounts =
        emittedPointCountsByDevice.computeIfAbsent(device, key -> new LinkedHashMap<>());
    final Map<String, Long> queryFallbackPointCounts =
        Objects.nonNull(activeQueryParser)
            ? queryFallbackPointCountsByDevice.computeIfAbsent(device, key -> new LinkedHashMap<>())
            : Collections.emptyMap();
    final List<String> measurements = extractMeasurementNames(tablet);
    for (int column = 0; column < measurements.size(); column++) {
      final String measurement = measurements.get(column);
      final BitMap bitMap = tablet.bitMaps == null ? null : tablet.bitMaps[column];
      for (int row = 0; row < tablet.rowSize; row++) {
        if (bitMap == null || !bitMap.isMarked(row)) {
          emittedTimestamps.merge(measurement, tablet.timestamps[row], Math::max);
          emittedPointCounts.merge(measurement, 1L, Long::sum);
          if (Objects.nonNull(activeQueryParser)) {
            queryFallbackPointCounts.merge(measurement, 1L, Long::sum);
          }
        }
      }
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

  private boolean finishIteration() {
    verifyQueryFallbackMeasurements();
    close();
    if (Objects.nonNull(unrecoverableQueryFailure)) {
      throw unrecoverableQueryFailure;
    }
    return false;
  }

  private void verifyQueryFallbackMeasurements() {
    if (Objects.nonNull(unrecoverableQueryFailure)) {
      return;
    }
    for (final Map.Entry<IDeviceID, Map<String, Long>> deviceEntry :
        expectedQueryFallbackPointCountsByDevice.entrySet()) {
      final Map<String, Long> queriedPointCounts =
          queryFallbackPointCountsByDevice.getOrDefault(
              deviceEntry.getKey(), Collections.emptyMap());
      for (final Map.Entry<String, Long> measurementEntry : deviceEntry.getValue().entrySet()) {
        if (!Objects.equals(
            measurementEntry.getValue(),
            queriedPointCounts.getOrDefault(measurementEntry.getKey(), 0L))) {
          unrecoverableQueryFailure = new IllegalStateException(deviceEntry.getKey().toString());
          return;
        }
      }
    }
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
    return tablet.getSchemas().stream()
        .map(MeasurementSchema::getMeasurementId)
        .collect(Collectors.toList());
  }

  @FunctionalInterface
  interface ScanParserFactory {
    TsFileInsertionScanDataContainer create(File file) throws IOException, IllegalPathException;
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
