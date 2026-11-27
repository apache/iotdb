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
import org.apache.iotdb.commons.pipe.datastructure.pattern.IoTDBTreePattern;
import org.apache.iotdb.commons.pipe.datastructure.pattern.TreePattern;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.exception.load.LoadAnalyzeInvalidPathException;
import org.apache.iotdb.db.exception.load.LoadRuntimeOutOfMemoryException;
import org.apache.iotdb.db.i18n.StorageEngineMessages;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tsfile.parser.TsFileParseFailureException;
import org.apache.iotdb.db.pipe.event.common.tsfile.parser.query.TsFileInsertionEventQueryParser;
import org.apache.iotdb.db.pipe.event.common.tsfile.parser.scan.TsFileInsertionEventScanParser;
import org.apache.iotdb.db.pipe.event.common.tsfile.parser.util.ModsOperationUtil;
import org.apache.iotdb.db.pipe.event.common.tsfile.parser.util.ModsOperationUtil.ModsInfo;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModEntry;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModificationFile;
import org.apache.iotdb.db.storageengine.load.TsFileStructureCorruptionUtils;
import org.apache.iotdb.db.storageengine.load.memory.LoadTsFileParserMemoryManager;
import org.apache.iotdb.db.utils.datastructure.PatternTreeMapFactory;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;

import org.apache.tsfile.exception.PathParseException;
import org.apache.tsfile.file.metadata.AbstractAlignedChunkMetadata;
import org.apache.tsfile.file.metadata.ChunkMetadata;
import org.apache.tsfile.file.metadata.IChunkMetadata;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.MetadataIndexNode;
import org.apache.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.tsfile.read.TsFileDeviceIterator;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.TsFileSequenceReaderTimeseriesMetadataIterator;
import org.apache.tsfile.read.common.BatchData;
import org.apache.tsfile.read.common.Chunk;
import org.apache.tsfile.read.reader.IChunkReader;
import org.apache.tsfile.read.reader.chunk.AlignedChunkReader;
import org.apache.tsfile.read.reader.chunk.ChunkReader;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.TsPrimitiveType;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;

import static org.apache.iotdb.db.storageengine.load.LoadTsFilePathUtils.getValidatedDevicePath;

/**
 * Load uses scan parsing first for throughput. If scan parsing hits corruption, fall back to query
 * parsing for the remaining measurements and devices. At normal EOF, query-parse devices whose
 * emitted point counts do not match their TsFile metadata. Report unrecoverable query failures so a
 * partial load cannot succeed.
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
  private final ScanParserFactory scanParserFactory;
  private final QueryParserFactory queryParserFactory;
  private final ArrayDeque<QueryTask> pendingQueryTasks = new ArrayDeque<>();

  private TsFileInsertionEventScanParser scanParser;
  private QueryTask activeQueryTask;
  private QueryParser activeQueryParser;
  private Iterator<Pair<Tablet, Boolean>> activeIterator;
  private boolean scanInitialized;
  private boolean fallbackTriggered;
  private long scanCorruptionOffset = -1;
  private long unrecoverableCorruptionOffset = -1;
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
            new TsFileInsertionEventScanParser(
                sourceFile,
                null,
                null,
                isWithMod,
                LoadTsFileParserMemoryManager.getInstance(),
                new TsFileInsertionEventScanParser.ScanOptions(
                    LOAD_TREE_PATTERN, Long.MIN_VALUE, Long.MAX_VALUE, true)));
  }

  @TestOnly
  LoadTreeTsFileTabletIterator(
      final File file, final boolean isWithMod, final ScanParserFactory scanParserFactory) {
    this(
        file,
        isWithMod,
        scanParserFactory,
        (sourceFile, device, measurements, startTime, endTime, withMod) -> {
          final TsFileInsertionEventQueryParser parser =
              new TsFileInsertionEventQueryParser(
                  sourceFile,
                  LOAD_TREE_PATTERN,
                  startTime,
                  endTime,
                  Collections.singletonMap(device, measurements),
                  withMod,
                  LoadTsFileParserMemoryManager.getInstance());
          return new QueryParser() {
            @Override
            public Iterator<Pair<Tablet, Boolean>> iterator() {
              final Iterator<TabletInsertionEvent> tabletIterator =
                  parser.toTabletInsertionEvents().iterator();
              return new Iterator<Pair<Tablet, Boolean>>() {
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
                      rawTabletInsertionEvent.convertToTablet(),
                      rawTabletInsertionEvent.isAligned());
                }
              };
            }

            @Override
            public void close() {
              parser.close();
            }

            @Override
            public long getCurrentOffset() {
              return parser.getCurrentOffset();
            }
          };
        });
  }

  @TestOnly
  LoadTreeTsFileTabletIterator(
      final File file,
      final boolean isWithMod,
      final ScanParserFactory scanParserFactory,
      final QueryParserFactory queryParserFactory) {
    this.file = file;
    this.isWithMod = isWithMod;
    this.scanParserFactory = scanParserFactory;
    this.queryParserFactory = queryParserFactory;
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

  private boolean switchToNextIterator() throws IOException {
    if (Objects.nonNull(activeQueryParser)) {
      closeActiveQueryParser();
      return activateNextQueryParser();
    }

    final Map<IDeviceID, Map<String, Long>> parsedPointCounts =
        Objects.nonNull(scanParser) ? scanParser.getParsedPointCounts() : Collections.emptyMap();
    final Map<IDeviceID, Map<String, Long>> expectedPointCounts =
        Objects.nonNull(scanParser) ? scanParser.getExpectedPointCounts() : Collections.emptyMap();
    closeScanParser();
    try {
      enqueueQueryTasksForUnemittedData(parsedPointCounts, expectedPointCounts);
    } catch (final Exception e) {
      if (shouldRethrow(e)) {
        throw toRuntimeException(e);
      }
      unrecoverableQueryFailure = toRuntimeException(e);
      return false;
    }
    return activateNextQueryParser();
  }

  private void enqueueQueryTasksForUnemittedData(
      final Map<IDeviceID, Map<String, Long>> parsedPointCounts,
      final Map<IDeviceID, Map<String, Long>> expectedPointCounts)
      throws IOException {
    if (fallbackTriggered) {
      return;
    }

    final ArrayDeque<QueryTask> queryTasks =
        buildQueryTasks(parsedPointCounts, expectedPointCounts);
    if (!queryTasks.isEmpty()) {
      fallbackTriggered = true;
      pendingQueryTasks.addAll(queryTasks);
    }
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
    if (e instanceof TsFileParseFailureException) {
      scanCorruptionOffset = ((TsFileParseFailureException) e).getOffset();
    } else if (Objects.nonNull(scanParser)
        && TsFileStructureCorruptionUtils.isStructureCorruption(e)) {
      scanCorruptionOffset = scanParser.getCurrentOffset();
    }
    if (scanCorruptionOffset >= 0) {
      unrecoverableCorruptionOffset = scanCorruptionOffset;
    }
    final IDeviceID currentDevice =
        Objects.nonNull(scanParser) ? scanParser.getCurrentDevice() : null;
    closeScanParser();

    try {
      pendingQueryTasks.addAll(buildQueryTasks(emittedPointCountsByDevice, null));
    } catch (final Exception queryInitException) {
      LOGGER.warn(
          StorageEngineMessages
              .MESSAGE_LOAD_FAILED_TO_INITIALIZE_QUERY_FALLBACK_FOR_TSFILE_ARG_AFTER_SCAN_PARSER_FAILURE_143B6037,
          file.getAbsolutePath(),
          queryInitException);
      if (scanCorruptionOffset >= 0
          && TsFileStructureCorruptionUtils.isStructureCorruption(queryInitException)) {
        unrecoverableQueryFailure = toRuntimeException(queryInitException);
        return true;
      }
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

  private ArrayDeque<QueryTask> buildQueryTasks(
      final Map<IDeviceID, Map<String, Long>> completedPointCountsByDevice,
      final Map<IDeviceID, Map<String, Long>> scannedExpectedPointCountsByDevice)
      throws IOException {
    final ArrayDeque<QueryTask> tasks = new ArrayDeque<>();
    final boolean containsModifications = hasModifications();
    final Map<IDeviceID, Map<String, Long>> metadataPointCountsByDevice =
        readMetadataPointCountsByDevice();

    final Set<IDeviceID> unverifiableDevices = new HashSet<>();
    final Map<IDeviceID, Map<String, Long>> expectedPointCountsByDevice =
        containsModifications
            ? Objects.nonNull(scannedExpectedPointCountsByDevice)
                    && pointCountsMatch(completedPointCountsByDevice, metadataPointCountsByDevice)
                ? scannedExpectedPointCountsByDevice
                : readExpectedPointCountsWithMods(metadataPointCountsByDevice, unverifiableDevices)
            : metadataPointCountsByDevice;
    final Map<IDeviceID, Map<String, Long>> progressPointCountsByDevice =
        containsModifications ? emittedPointCountsByDevice : completedPointCountsByDevice;

    appendQueryTasksForIncompleteMeasurements(
        tasks, expectedPointCountsByDevice, progressPointCountsByDevice, unverifiableDevices);
    verifyCompletedPointCounts(progressPointCountsByDevice, expectedPointCountsByDevice);
    return tasks;
  }

  private Map<IDeviceID, Map<String, Long>> readMetadataPointCountsByDevice() throws IOException {
    final Map<IDeviceID, Map<String, Long>> metadataPointCountsByDevice = new LinkedHashMap<>();
    try (final TsFileSequenceReader reader = new TsFileSequenceReader(file.getAbsolutePath())) {
      // iterAllTimeseriesMetadata treats an IOException as the end of iteration. Use the
      // throwing batch iterator so an incomplete metadata index cannot turn a partial load into a
      // success without rereading every device's metadata separately.
      final TsFileSequenceReaderTimeseriesMetadataIterator metadataIterator =
          new TsFileSequenceReaderTimeseriesMetadataIterator(reader, false);
      while (metadataIterator.hasNext()) {
        for (final Map.Entry<IDeviceID, List<TimeseriesMetadata>> deviceMetadata :
            metadataIterator.next().entrySet()) {
          final IDeviceID device = deviceMetadata.getKey();
          for (final TimeseriesMetadata metadata : deviceMetadata.getValue()) {
            final String measurement = metadata.getMeasurementId();
            if (measurement.isEmpty() || metadata.getStatistics().getCount() == 0) {
              continue;
            }

            metadataPointCountsByDevice
                .computeIfAbsent(device, key -> new LinkedHashMap<>())
                .merge(measurement, (long) metadata.getStatistics().getCount(), Long::sum);
          }
        }
      }
    }

    return metadataPointCountsByDevice;
  }

  private void appendQueryTasksForIncompleteMeasurements(
      final ArrayDeque<QueryTask> tasks,
      final Map<IDeviceID, Map<String, Long>> expectedPointCountsByDevice,
      final Map<IDeviceID, Map<String, Long>> progressPointCountsByDevice,
      final Set<IDeviceID> unverifiableDevices)
      throws IOException {
    for (final Map.Entry<IDeviceID, Map<String, Long>> deviceEntry :
        expectedPointCountsByDevice.entrySet()) {
      final IDeviceID device = deviceEntry.getKey();
      addQueryTasksForDevice(
          tasks,
          device,
          deviceEntry.getValue(),
          progressPointCountsByDevice.getOrDefault(device, Collections.emptyMap()),
          unverifiableDevices.contains(device));
    }
  }

  private void addQueryTasksForDevice(
      final ArrayDeque<QueryTask> tasks,
      final IDeviceID device,
      final Map<String, Long> expectedPointCounts,
      final Map<String, Long> completedPointCounts,
      final boolean unverifiable)
      throws IOException {
    final List<String> fallbackMeasurements = new ArrayList<>();
    final Map<String, Long> fallbackPointCounts = new LinkedHashMap<>();
    for (final Map.Entry<String, Long> measurementEntry : expectedPointCounts.entrySet()) {
      final String measurement = measurementEntry.getKey();
      final long completedPointCount = completedPointCounts.getOrDefault(measurement, 0L);
      if (unverifiable) {
        fallbackMeasurements.add(measurement);
        fallbackPointCounts.put(
            measurement, Math.max(0, measurementEntry.getValue() - completedPointCount));
        continue;
      }
      if (completedPointCount > measurementEntry.getValue()) {
        throw new IOException(
            StorageEngineMessages
                .EXCEPTION_FAILED_TO_ITERATE_TABLETS_WHILE_LOADING_TSFILE_4E6289F3);
      }
      if (completedPointCount < measurementEntry.getValue()) {
        fallbackMeasurements.add(measurement);
        fallbackPointCounts.put(measurement, measurementEntry.getValue() - completedPointCount);
      }
    }
    if (fallbackMeasurements.isEmpty()) {
      return;
    }

    expectedQueryFallbackPointCountsByDevice.put(device, fallbackPointCounts);
    addQueryTasks(tasks, device, fallbackMeasurements, Long.MIN_VALUE, Long.MAX_VALUE);
  }

  private void verifyCompletedPointCounts(
      final Map<IDeviceID, Map<String, Long>> progressPointCountsByDevice,
      final Map<IDeviceID, Map<String, Long>> expectedPointCountsByDevice)
      throws IOException {
    for (final Map.Entry<IDeviceID, Map<String, Long>> deviceEntry :
        progressPointCountsByDevice.entrySet()) {
      final Map<String, Long> expectedPointCounts =
          expectedPointCountsByDevice.getOrDefault(deviceEntry.getKey(), Collections.emptyMap());
      for (final Map.Entry<String, Long> measurementEntry : deviceEntry.getValue().entrySet()) {
        if (measurementEntry.getValue()
            > expectedPointCounts.getOrDefault(measurementEntry.getKey(), 0L)) {
          throw new IOException(
              StorageEngineMessages
                  .EXCEPTION_FAILED_TO_ITERATE_TABLETS_WHILE_LOADING_TSFILE_4E6289F3);
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
      final Map<IDeviceID, Map<String, Long>> metadataPointCountsByDevice,
      final Set<IDeviceID> unverifiableDevices)
      throws IOException {
    final PatternTreeMap<ModEntry, PatternTreeMapFactory.ModsSerializer> modifications =
        ModsOperationUtil.loadModificationsFromTsFile(file);
    final Map<IDeviceID, Map<String, Long>> expectedPointCountsByDevice = new LinkedHashMap<>();
    final Map<IDeviceID, Map<String, Long>> parsedPointCountsByDevice = new LinkedHashMap<>();

    try (final TsFileSequenceReader reader = new TsFileSequenceReader(file.getAbsolutePath())) {
      final TsFileDeviceIterator deviceIterator = reader.getAllDevicesIteratorWithIsAligned();
      while (deviceIterator.hasNext()) {
        final Pair<IDeviceID, Boolean> deviceWithAlignment = deviceIterator.next();
        final IDeviceID device = deviceWithAlignment.getLeft();
        try {
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
                  device, measurements, modifications, false);
          final Map<String, Long> parsedPointCounts = new LinkedHashMap<>();
          final Map<String, Long> expectedPointCounts = new LinkedHashMap<>();

          if (Boolean.TRUE.equals(deviceWithAlignment.getRight())) {
            countAlignedPoints(
                reader,
                reader.getAlignedChunkMetadataByMetadataIndexNode(device, metadataIndexNode, true),
                measurements,
                modsInfos,
                parsedPointCounts,
                expectedPointCounts);
          } else {
            countNonAlignedPoints(
                reader, device, measurements, modsInfos, parsedPointCounts, expectedPointCounts);
          }

          verifyParsedPointCounts(
              measurements,
              parsedPointCounts,
              metadataPointCountsByDevice.getOrDefault(device, Collections.emptyMap()));
          parsedPointCountsByDevice.put(device, parsedPointCounts);
          expectedPointCountsByDevice.put(device, expectedPointCounts);
        } catch (final Exception e) {
          if (shouldRethrow(e)) {
            throw toRuntimeException(e);
          }
          unverifiableDevices.add(device);
          expectedPointCountsByDevice.put(
              device, metadataPointCountsByDevice.getOrDefault(device, Collections.emptyMap()));
          if (unrecoverableQueryFailure == null) {
            unrecoverableQueryFailure = toRuntimeException(e);
          }
        }
      }
    }

    markDevicesWithoutParsedPointCounts(
        metadataPointCountsByDevice,
        parsedPointCountsByDevice,
        expectedPointCountsByDevice,
        unverifiableDevices);
    return expectedPointCountsByDevice;
  }

  private void markDevicesWithoutParsedPointCounts(
      final Map<IDeviceID, Map<String, Long>> metadataPointCountsByDevice,
      final Map<IDeviceID, Map<String, Long>> parsedPointCountsByDevice,
      final Map<IDeviceID, Map<String, Long>> expectedPointCountsByDevice,
      final Set<IDeviceID> unverifiableDevices) {
    for (final Map.Entry<IDeviceID, Map<String, Long>> deviceEntry :
        metadataPointCountsByDevice.entrySet()) {
      final IDeviceID device = deviceEntry.getKey();
      if (!parsedPointCountsByDevice.containsKey(device)) {
        unverifiableDevices.add(device);
        expectedPointCountsByDevice.put(device, deviceEntry.getValue());
        if (unrecoverableQueryFailure == null) {
          unrecoverableQueryFailure =
              toRuntimeException(
                  new IOException(
                      StorageEngineMessages
                          .EXCEPTION_FAILED_TO_ITERATE_TABLETS_WHILE_LOADING_TSFILE_4E6289F3));
        }
      }
    }
  }

  private void countAlignedPoints(
      final TsFileSequenceReader reader,
      final List<AbstractAlignedChunkMetadata> chunkMetadataList,
      final List<String> measurements,
      final List<ModsInfo> modsInfos,
      final Map<String, Long> parsedPointCounts,
      final Map<String, Long> expectedPointCounts)
      throws IOException {
    for (final AbstractAlignedChunkMetadata alignedChunkMetadata : chunkMetadataList) {
      countAlignedChunk(
          reader,
          alignedChunkMetadata,
          measurements,
          modsInfos,
          parsedPointCounts,
          expectedPointCounts);
    }
  }

  private void countAlignedChunk(
      final TsFileSequenceReader reader,
      final AbstractAlignedChunkMetadata alignedChunkMetadata,
      final List<String> measurements,
      final List<ModsInfo> modsInfos,
      final Map<String, Long> parsedPointCounts,
      final Map<String, Long> expectedPointCounts)
      throws IOException {
    final List<ChunkMetadata> valueChunkMetadata = new ArrayList<>();
    for (final IChunkMetadata metadata : alignedChunkMetadata.getValueChunkMetadataList()) {
      valueChunkMetadata.add(metadata == null ? null : (ChunkMetadata) metadata);
    }
    if (valueChunkMetadata.size() != measurements.size()) {
      throw new IOException(
          StorageEngineMessages.EXCEPTION_FAILED_TO_ITERATE_TABLETS_WHILE_LOADING_TSFILE_4E6289F3);
    }

    final Chunk timeChunk =
        reader.readMemChunk((ChunkMetadata) alignedChunkMetadata.getTimeChunkMetadata());
    final List<Chunk> valueChunks = new ArrayList<>(valueChunkMetadata.size());
    boolean chunksTransferredToReader = false;
    try {
      for (final ChunkMetadata metadata : valueChunkMetadata) {
        valueChunks.add(metadata == null ? null : reader.readMemChunk(metadata));
      }
      chunksTransferredToReader = true;
      countAlignedChunkData(
          timeChunk, valueChunks, measurements, modsInfos, parsedPointCounts, expectedPointCounts);
    } finally {
      if (!chunksTransferredToReader) {
        timeChunk.close();
        valueChunks.forEach(
            chunk -> {
              if (chunk != null) {
                chunk.close();
              }
            });
      }
    }
  }

  private void countAlignedChunkData(
      final Chunk timeChunk,
      final List<Chunk> valueChunks,
      final List<String> measurements,
      final List<ModsInfo> modsInfos,
      final Map<String, Long> parsedPointCounts,
      final Map<String, Long> expectedPointCounts)
      throws IOException {
    try (final IChunkReader chunkReader = new AlignedChunkReader(timeChunk, valueChunks)) {
      while (chunkReader.hasNextSatisfiedPage()) {
        final BatchData pageData = chunkReader.nextPageData();
        while (pageData.hasCurrent()) {
          countAlignedPage(
              pageData, measurements, modsInfos, parsedPointCounts, expectedPointCounts);
          pageData.next();
        }
      }
    }
  }

  private void countAlignedPage(
      final BatchData pageData,
      final List<String> measurements,
      final List<ModsInfo> modsInfos,
      final Map<String, Long> parsedPointCounts,
      final Map<String, Long> expectedPointCounts)
      throws IOException {
    final TsPrimitiveType[] values = pageData.getVector();
    if (values.length != measurements.size()) {
      throw new IOException(
          StorageEngineMessages.EXCEPTION_FAILED_TO_ITERATE_TABLETS_WHILE_LOADING_TSFILE_4E6289F3);
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
      for (final ChunkMetadata metadata : reader.getChunkMetadataList(device, measurement, true)) {
        try (final IChunkReader chunkReader = new ChunkReader(reader.readMemChunk(metadata))) {
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
  }

  private void verifyParsedPointCounts(
      final Iterable<String> measurements,
      final Map<String, Long> parsedPointCounts,
      final Map<String, Long> metadataPointCounts)
      throws IOException {
    for (final String measurement : measurements) {
      final long parsedPointCount = parsedPointCounts.getOrDefault(measurement, 0L);
      final long metadataPointCount = metadataPointCounts.getOrDefault(measurement, 0L);
      if (parsedPointCount != metadataPointCount) {
        throw new IOException(
            StorageEngineMessages
                .EXCEPTION_FAILED_TO_ITERATE_TABLETS_WHILE_LOADING_TSFILE_4E6289F3);
      }
    }
  }

  private boolean hasModifications() throws IOException {
    if (hasModifications == null) {
      hasModifications = isWithMod && !ModificationFile.readAllModifications(file, true).isEmpty();
    }
    return hasModifications;
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
            queryParserFactory.create(
                file,
                activeQueryTask.device,
                activeQueryTask.measurements,
                activeQueryTask.startTime,
                activeQueryTask.endTime,
                isWithMod);
        activeIterator = activeQueryParser.iterator();
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
    final Map<String, Long> emittedPointCounts =
        emittedPointCountsByDevice.computeIfAbsent(device, key -> new LinkedHashMap<>());
    final List<String> measurements = extractMeasurementNames(tablet);
    final Map<String, Long> queryFallbackPointCounts =
        Objects.nonNull(activeQueryParser)
            ? queryFallbackPointCountsByDevice.computeIfAbsent(device, key -> new LinkedHashMap<>())
            : Collections.emptyMap();
    for (int column = 0; column < measurements.size(); column++) {
      final String measurement = measurements.get(column);
      for (int row = 0; row < tablet.getRowSize(); row++) {
        if (!tablet.isNull(row, column)) {
          emittedTimestamps.merge(measurement, tablet.getTimestamp(row), Math::max);
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
    verifyQueryFallbackMeasurements();
    close();
    if (Objects.nonNull(unrecoverableQueryFailure)) {
      if (unrecoverableCorruptionOffset >= 0
          && TsFileStructureCorruptionUtils.isStructureCorruption(unrecoverableQueryFailure)
          && !TsFileStructureCorruptionUtils.hasDiagnosticMessage(unrecoverableQueryFailure)) {
        throw createStructureCorruptionFailure(
            unrecoverableCorruptionOffset, unrecoverableQueryFailure);
      }
      throw unrecoverableQueryFailure;
    }
    return false;
  }

  private RuntimeException createStructureCorruptionFailure(
      final long corruptionOffset, final Throwable cause) {
    return new IllegalStateException(
        TsFileStructureCorruptionUtils.createDiagnosticMessage(file, corruptionOffset, cause),
        cause);
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
        final long expectedPointCount = measurementEntry.getValue();
        final long queriedPointCount =
            queriedPointCounts.getOrDefault(measurementEntry.getKey(), 0L);
        if (expectedPointCount != queriedPointCount) {
          unrecoverableQueryFailure =
              new IllegalStateException(
                  String.format(
                      StorageEngineMessages
                          .EXCEPTION_QUERY_FALLBACK_POINT_COUNT_MISMATCH_FOR_DEVICE_ARG_MEASUREMENT_ARG_EXPECTED_ARG_POINTS_BUT_RECOVERED_ARG_POINTS_0BEC4004,
                      deviceEntry.getKey(),
                      measurementEntry.getKey(),
                      expectedPointCount,
                      queriedPointCount));
          return;
        }
      }
    }
  }

  private void splitOrSkipActiveQueryTask(final Exception failure) {
    final QueryTask failedTask = activeQueryTask;
    final long queryFailureOffset =
        TsFileStructureCorruptionUtils.isStructureCorruption(failure)
                && Objects.nonNull(activeQueryParser)
            ? activeQueryParser.getCurrentOffset()
            : -1;
    closeActiveQueryParser();
    if (Objects.isNull(failedTask)) {
      return;
    }

    if (failedTask.measurements.size() <= 1) {
      // Keep salvaging later tasks, but retain the first failure so the caller preserves the
      // source.
      if (Objects.isNull(unrecoverableQueryFailure)) {
        unrecoverableQueryFailure = toRuntimeException(failure);
        unrecoverableCorruptionOffset = queryFailureOffset;
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

  @FunctionalInterface
  interface ScanParserFactory {
    TsFileInsertionEventScanParser create(File file) throws IOException, IllegalPathException;
  }

  @FunctionalInterface
  interface QueryParserFactory {
    QueryParser create(
        File file,
        IDeviceID device,
        List<String> measurements,
        long startTime,
        long endTime,
        boolean isWithMod)
        throws IOException, IllegalPathException;
  }

  interface QueryParser extends AutoCloseable {
    Iterator<Pair<Tablet, Boolean>> iterator();

    default long getCurrentOffset() {
      return -1;
    }

    @Override
    void close();
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
