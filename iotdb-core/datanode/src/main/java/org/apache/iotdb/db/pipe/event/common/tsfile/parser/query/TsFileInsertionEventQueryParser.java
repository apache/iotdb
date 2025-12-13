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

package org.apache.iotdb.db.pipe.event.common.tsfile.parser.query;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.audit.IAuditEntity;
import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.auth.AccessDeniedException;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeTaskMeta;
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.commons.pipe.datastructure.pattern.TreePattern;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.pipe.event.common.PipeInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tsfile.parser.TsFileInsertionEventParser;
import org.apache.iotdb.db.pipe.event.common.tsfile.parser.util.ModsOperationUtil;
import org.apache.iotdb.db.pipe.resource.PipeDataNodeResourceManager;
import org.apache.iotdb.db.pipe.resource.memory.PipeMemoryBlock;
import org.apache.iotdb.db.pipe.resource.memory.PipeMemoryWeightUtil;
import org.apache.iotdb.db.pipe.resource.tsfile.PipeTsFileResourceManager;
import org.apache.iotdb.db.utils.datastructure.PatternTreeMapFactory;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.exception.PipeException;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.tsfile.read.TsFileDeviceIterator;
import org.apache.tsfile.read.TsFileReader;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.write.record.Tablet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;

public class TsFileInsertionEventQueryParser extends TsFileInsertionEventParser {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(TsFileInsertionEventQueryParser.class);

  private final PipeMemoryBlock allocatedMemoryBlock;
  private final TsFileReader tsFileReader;

  private final Iterator<Map.Entry<IDeviceID, List<String>>> deviceMeasurementsMapIterator;
  private final Map<IDeviceID, Boolean> deviceIsAlignedMap;
  private final Map<String, TSDataType> measurementDataTypeMap;

  @TestOnly
  public TsFileInsertionEventQueryParser(
      final File tsFile,
      final TreePattern pattern,
      final long startTime,
      final long endTime,
      final PipeInsertionEvent sourceEvent)
      throws IOException, IllegalPathException {
    this(null, 0, tsFile, pattern, startTime, endTime, null, sourceEvent, false);
  }

  public TsFileInsertionEventQueryParser(
      final String pipeName,
      final long creationTime,
      final File tsFile,
      final TreePattern pattern,
      final long startTime,
      final long endTime,
      final PipeTaskMeta pipeTaskMeta,
      final PipeInsertionEvent sourceEvent,
      final boolean isWithMod)
      throws IOException, IllegalPathException {
    this(
        pipeName,
        creationTime,
        tsFile,
        pattern,
        startTime,
        endTime,
        pipeTaskMeta,
        sourceEvent,
        null,
        false,
        null,
        isWithMod);
  }

  public TsFileInsertionEventQueryParser(
      final String pipeName,
      final long creationTime,
      final File tsFile,
      final TreePattern pattern,
      final long startTime,
      final long endTime,
      final PipeTaskMeta pipeTaskMeta,
      final PipeInsertionEvent sourceEvent,
      final IAuditEntity entity,
      final boolean skipIfNoPrivileges,
      final Map<IDeviceID, Boolean> deviceIsAlignedMap,
      final boolean isWithMod)
      throws IOException, IllegalPathException {
    super(
        pipeName,
        creationTime,
        pattern,
        null,
        startTime,
        endTime,
        pipeTaskMeta,
        entity,
        skipIfNoPrivileges,
        sourceEvent);

    try {
      currentModifications =
          isWithMod
              ? ModsOperationUtil.loadModificationsFromTsFile(tsFile)
              : PatternTreeMapFactory.getModsPatternTreeMap();
      allocatedMemoryBlockForModifications =
          PipeDataNodeResourceManager.memory()
              .forceAllocateForTabletWithRetry(currentModifications.ramBytesUsed());

      final PipeTsFileResourceManager tsFileResourceManager = PipeDataNodeResourceManager.tsfile();
      final Map<IDeviceID, List<String>> deviceMeasurementsMap;

      // TsFileReader is not thread-safe, so we need to create it here and close it later.
      long memoryRequiredInBytes =
          PipeConfig.getInstance().getPipeMemoryAllocateForTsFileSequenceReaderInBytes();
      tsFileSequenceReader = new TsFileSequenceReader(tsFile.getPath(), true, true);
      tsFileReader = new TsFileReader(tsFileSequenceReader);

      if (tsFileResourceManager.cacheObjectsIfAbsent(tsFile)) {
        // These read-only objects can be found in cache.
        this.deviceIsAlignedMap =
            Objects.nonNull(deviceIsAlignedMap)
                ? deviceIsAlignedMap
                : tsFileResourceManager.getDeviceIsAlignedMapFromCache(tsFile, true);
        measurementDataTypeMap = tsFileResourceManager.getMeasurementDataTypeMapFromCache(tsFile);
        deviceMeasurementsMap = tsFileResourceManager.getDeviceMeasurementsMapFromCache(tsFile);
      } else {
        // We need to create these objects here and remove them later.
        final Set<IDeviceID> devices;
        if (Objects.isNull(deviceIsAlignedMap)) {
          this.deviceIsAlignedMap = readDeviceIsAlignedMap();
          memoryRequiredInBytes +=
              PipeMemoryWeightUtil.memoryOfIDeviceId2Bool(this.deviceIsAlignedMap);

          // Filter devices that may overlap with pattern first
          // to avoid reading all time-series of all devices.
          devices = filterDevicesByPattern(this.deviceIsAlignedMap.keySet());
        } else {
          this.deviceIsAlignedMap = deviceIsAlignedMap;
          devices = deviceIsAlignedMap.keySet();
        }

        measurementDataTypeMap = readFilteredFullPathDataTypeMap(devices);
        memoryRequiredInBytes +=
            PipeMemoryWeightUtil.memoryOfStr2TSDataType(measurementDataTypeMap);

        deviceMeasurementsMap = readFilteredDeviceMeasurementsMap(devices);
        memoryRequiredInBytes +=
            PipeMemoryWeightUtil.memoryOfIDeviceID2StrList(deviceMeasurementsMap);
      }
      allocatedMemoryBlock =
          PipeDataNodeResourceManager.memory().forceAllocate(memoryRequiredInBytes);

      final Iterator<Map.Entry<IDeviceID, List<String>>> iterator =
          deviceMeasurementsMap.entrySet().iterator();
      while (isWithMod && iterator.hasNext()) {
        final Map.Entry<IDeviceID, List<String>> entry = iterator.next();
        final IDeviceID deviceId = entry.getKey();
        final List<String> measurements = entry.getValue();

        // Check if deviceId is deleted
        if (deviceId == null) {
          LOGGER.warn("Found null deviceId, removing entry");
          iterator.remove();
          continue;
        }

        // Check if measurements list is deleted or empty
        if (measurements == null || measurements.isEmpty()) {
          iterator.remove();
          continue;
        }

        if (!currentModifications.isEmpty()) {
          // Safely filter measurements, remove non-existent measurements
          measurements.removeIf(
              measurement -> {
                if (measurement == null) {
                  return true;
                }

                try {
                  TimeseriesMetadata meta =
                      tsFileSequenceReader.readTimeseriesMetadata(deviceId, measurement, true);
                  return ModsOperationUtil.isAllDeletedByMods(
                      deviceId,
                      measurement,
                      meta.getStatistics().getStartTime(),
                      meta.getStatistics().getEndTime(),
                      currentModifications);
                } catch (IOException e) {
                  LOGGER.warn(
                      "Failed to read metadata for deviceId: {}, measurement: {}, removing",
                      deviceId,
                      measurement,
                      e);
                  return true;
                }
              });
        }

        // If measurements list is empty after filtering, remove the entire entry
        if (measurements.isEmpty()) {
          iterator.remove();
        }
      }

      // Filter again to get the final deviceMeasurementsMap that exactly matches the pattern.
      deviceMeasurementsMapIterator =
          filterDeviceMeasurementsMapByPattern(deviceMeasurementsMap).entrySet().iterator();

      // No longer need this. Help GC.
      tsFileSequenceReader.clearCachedDeviceMetadata();
    } catch (final Exception e) {
      close();
      throw e;
    }
  }

  private Map<IDeviceID, List<String>> filterDeviceMeasurementsMapByPattern(
      final Map<IDeviceID, List<String>> originalDeviceMeasurementsMap)
      throws IllegalPathException {
    final Map<IDeviceID, List<String>> filteredDeviceMeasurementsMap = new HashMap<>();
    for (Map.Entry<IDeviceID, List<String>> entry : originalDeviceMeasurementsMap.entrySet()) {
      final IDeviceID deviceId = entry.getKey();

      // case 1: for example, pattern is root.a.b or pattern is null and device is root.a.b.c
      // in this case, all data can be matched without checking the measurements
      if (Objects.isNull(treePattern)
          || treePattern.isRoot()
          || treePattern.coversDevice(deviceId)) {
        if (!entry.getValue().isEmpty()) {
          filteredDeviceMeasurementsMap.put(deviceId, entry.getValue());
        }
      }

      // case 2: for example, pattern is root.a.b.c and device is root.a.b
      // in this case, we need to check the full path
      else if (treePattern.mayOverlapWithDevice(deviceId)) {
        final List<String> filteredMeasurements = new ArrayList<>();

        for (final String measurement : entry.getValue()) {
          if (treePattern.matchesMeasurement(deviceId, measurement)) {
            if (Objects.nonNull(entity)) {
              final TSStatus status =
                  AuthorityChecker.getAccessControl()
                      .checkSeriesPrivilege4Pipe(
                          entity,
                          Collections.singletonList(new MeasurementPath(deviceId, measurement)),
                          PrivilegeType.READ_DATA);
              if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
                if (skipIfNoPrivileges) {
                  continue;
                }
                throw new AccessDeniedException(status.getMessage());
              }
            }
            filteredMeasurements.add(measurement);
          }
        }

        if (!filteredMeasurements.isEmpty()) {
          filteredDeviceMeasurementsMap.put(deviceId, filteredMeasurements);
        }
      }
    }

    return filteredDeviceMeasurementsMap;
  }

  private Map<IDeviceID, Boolean> readDeviceIsAlignedMap() throws IOException {
    final Map<IDeviceID, Boolean> deviceIsAlignedResultMap = new HashMap<>();
    final TsFileDeviceIterator deviceIsAlignedIterator =
        tsFileSequenceReader.getAllDevicesIteratorWithIsAligned();
    while (deviceIsAlignedIterator.hasNext()) {
      final Pair<IDeviceID, Boolean> deviceIsAlignedPair = deviceIsAlignedIterator.next();
      deviceIsAlignedResultMap.put(deviceIsAlignedPair.getLeft(), deviceIsAlignedPair.getRight());
    }
    return deviceIsAlignedResultMap;
  }

  private Set<IDeviceID> filterDevicesByPattern(final Set<IDeviceID> devices) {
    if (Objects.isNull(treePattern) || treePattern.isRoot()) {
      return devices;
    }

    final Set<IDeviceID> filteredDevices = new HashSet<>();
    for (final IDeviceID device : devices) {
      if (treePattern.coversDevice(device) || treePattern.mayOverlapWithDevice(device)) {
        filteredDevices.add(device);
      }
    }
    return filteredDevices;
  }

  /**
   * This method is similar to {@link TsFileSequenceReader#getFullPathDataTypeMap()}, but only reads
   * the given devices.
   */
  private Map<String, TSDataType> readFilteredFullPathDataTypeMap(final Set<IDeviceID> devices)
      throws IOException {
    final Map<String, TSDataType> result = new HashMap<>();

    for (final IDeviceID device : devices) {
      tsFileSequenceReader
          .readDeviceMetadata(device)
          .values()
          .forEach(
              timeseriesMetadata ->
                  result.put(
                      device.toString() + "." + timeseriesMetadata.getMeasurementId(),
                      timeseriesMetadata.getTsDataType()));
    }

    return result;
  }

  /**
   * This method is similar to {@link TsFileSequenceReader#getDeviceMeasurementsMap()}, but only
   * reads the given devices.
   */
  private Map<IDeviceID, List<String>> readFilteredDeviceMeasurementsMap(
      final Set<IDeviceID> devices) throws IOException {
    final Map<IDeviceID, List<String>> result = new HashMap<>();

    for (final IDeviceID device : devices) {
      tsFileSequenceReader
          .readDeviceMetadata(device)
          .values()
          .forEach(
              timeseriesMetadata ->
                  result
                      .computeIfAbsent(device, d -> new ArrayList<>())
                      .add(timeseriesMetadata.getMeasurementId()));
    }

    return result;
  }

  @Override
  public Iterable<TabletInsertionEvent> toTabletInsertionEvents() {
    if (tabletInsertionIterable == null) {
      tabletInsertionIterable =
          () ->
              new Iterator<TabletInsertionEvent>() {

                private TsFileInsertionEventQueryParserTabletIterator tabletIterator = null;

                @Override
                public boolean hasNext() {
                  boolean hasNext = false;
                  while (tabletIterator == null || !tabletIterator.hasNext()) {
                    if (!deviceMeasurementsMapIterator.hasNext()) {
                      // Record end time when no more data
                      if (parseStartTimeRecorded && !parseEndTimeRecorded) {
                        recordParseEndTime();
                      }
                      close();
                      return false;
                    }

                    final Map.Entry<IDeviceID, List<String>> entry =
                        deviceMeasurementsMapIterator.next();

                    try {
                      tabletIterator =
                          new TsFileInsertionEventQueryParserTabletIterator(
                              tsFileReader,
                              measurementDataTypeMap,
                              entry.getKey(),
                              entry.getValue(),
                              timeFilterExpression,
                              allocatedMemoryBlockForTablet,
                              currentModifications);
                    } catch (final Exception e) {
                      close();
                      throw new PipeException(
                          "failed to create TsFileInsertionDataTabletIterator", e);
                    }
                  }

                  hasNext = true;
                  // Record start time on first hasNext() that returns true
                  if (!parseStartTimeRecorded) {
                    recordParseStartTime();
                  }
                  return hasNext;
                }

                @Override
                public TabletInsertionEvent next() {
                  if (!hasNext()) {
                    close();
                    throw new NoSuchElementException();
                  }

                  final Tablet tablet = tabletIterator.next();
                  // Record tablet metrics
                  recordTabletMetrics(tablet);
                  final boolean isAligned =
                      deviceIsAlignedMap.getOrDefault(
                          IDeviceID.Factory.DEFAULT_FACTORY.create(tablet.getDeviceId()), false);

                  final TabletInsertionEvent next;
                  if (!hasNext()) {
                    next =
                        sourceEvent == null
                            ? new PipeRawTabletInsertionEvent(
                                null,
                                null,
                                null,
                                null,
                                tablet,
                                isAligned,
                                null,
                                0,
                                pipeTaskMeta,
                                sourceEvent,
                                true)
                            : new PipeRawTabletInsertionEvent(
                                sourceEvent.getRawIsTableModelEvent(),
                                sourceEvent.getSourceDatabaseNameFromDataRegion(),
                                sourceEvent.getRawTableModelDataBase(),
                                sourceEvent.getRawTreeModelDataBase(),
                                tablet,
                                isAligned,
                                sourceEvent.getPipeName(),
                                sourceEvent.getCreationTime(),
                                pipeTaskMeta,
                                sourceEvent,
                                true);
                    close();
                  } else {
                    next =
                        sourceEvent == null
                            ? new PipeRawTabletInsertionEvent(
                                null,
                                null,
                                null,
                                null,
                                tablet,
                                isAligned,
                                null,
                                0,
                                pipeTaskMeta,
                                sourceEvent,
                                false)
                            : new PipeRawTabletInsertionEvent(
                                sourceEvent.getRawIsTableModelEvent(),
                                sourceEvent.getSourceDatabaseNameFromDataRegion(),
                                sourceEvent.getRawTableModelDataBase(),
                                sourceEvent.getRawTreeModelDataBase(),
                                tablet,
                                isAligned,
                                sourceEvent.getPipeName(),
                                sourceEvent.getCreationTime(),
                                pipeTaskMeta,
                                sourceEvent,
                                false);
                  }
                  return next;
                }
              };
    }

    return tabletInsertionIterable;
  }

  @Override
  public void close() {
    try {
      if (tsFileReader != null) {
        tsFileReader.close();
      }
    } catch (final IOException e) {
      LOGGER.warn("Failed to close TsFileReader", e);
    }

    super.close();

    if (allocatedMemoryBlock != null) {
      allocatedMemoryBlock.close();
    }
  }
}
