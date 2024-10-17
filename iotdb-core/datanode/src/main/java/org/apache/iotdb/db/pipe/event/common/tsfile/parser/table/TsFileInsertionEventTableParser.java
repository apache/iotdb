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

package org.apache.iotdb.db.pipe.event.common.tsfile.parser.table;

import org.apache.iotdb.commons.pipe.agent.task.meta.PipeTaskMeta;
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.commons.pipe.datastructure.pattern.TablePattern;
import org.apache.iotdb.commons.pipe.event.PipeInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tsfile.parser.TsFileInsertionEventParser;
import org.apache.iotdb.db.pipe.resource.PipeDataNodeResourceManager;
import org.apache.iotdb.db.pipe.resource.memory.PipeMemoryBlock;
import org.apache.iotdb.db.pipe.resource.memory.PipeMemoryWeightUtil;
import org.apache.iotdb.db.pipe.resource.tsfile.PipeTsFileResourceManager;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.exception.PipeException;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.read.TsFileDeviceIterator;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.controller.CachedChunkLoaderImpl;
import org.apache.tsfile.read.controller.MetadataQuerierByFileImpl;
import org.apache.tsfile.read.query.executor.TableQueryExecutor;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.write.record.Tablet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;

public class TsFileInsertionEventTableParser extends TsFileInsertionEventParser {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(TsFileInsertionEventTableParser.class);

  private final PipeMemoryBlock allocatedMemoryBlock;
  private final TableQueryExecutor tableQueryExecutor;

  private final Map<String, TableSchema> tableSchemaMap;

  private final Iterator<Map.Entry<IDeviceID, List<String>>> deviceMeasurementsMapIterator;
  private final Map<IDeviceID, Boolean> deviceIsAlignedMap;

  public TsFileInsertionEventTableParser(
      final File tsFile,
      final TablePattern pattern,
      final long startTime,
      final long endTime,
      final PipeTaskMeta pipeTaskMeta,
      final PipeInsertionEvent sourceEvent)
      throws IOException {
    this(tsFile, pattern, startTime, endTime, pipeTaskMeta, sourceEvent, null);
  }

  private TsFileInsertionEventTableParser(
      final File tsFile,
      final TablePattern pattern,
      final long startTime,
      final long endTime,
      final PipeTaskMeta pipeTaskMeta,
      final PipeInsertionEvent sourceEvent,
      final Map<IDeviceID, Boolean> deviceIsAlignedMap)
      throws IOException {
    super(null, pattern, startTime, endTime, pipeTaskMeta, sourceEvent);

    try {
      final PipeTsFileResourceManager tsFileResourceManager = PipeDataNodeResourceManager.tsfile();
      final Map<IDeviceID, List<String>> deviceMeasurementsMap;

      // TsFileReader is not thread-safe, so we need to create it here and close it later.
      long memoryRequiredInBytes =
          PipeConfig.getInstance().getPipeMemoryAllocateForTsFileSequenceReaderInBytes();
      tsFileSequenceReader = new TsFileSequenceReader(tsFile.getPath(), true, true);

      tableSchemaMap = tsFileSequenceReader.readFileMetadata().getTableSchemaMap();

      tableQueryExecutor =
          new TableQueryExecutor(
              new MetadataQuerierByFileImpl(tsFileSequenceReader),
              new CachedChunkLoaderImpl(tsFileSequenceReader),
              TableQueryExecutor.TableQueryOrdering.DEVICE);

      if (tsFileResourceManager.cacheObjectsIfAbsent(tsFile)) {
        // These read-only objects can be found in cache.
        this.deviceIsAlignedMap =
            Objects.nonNull(deviceIsAlignedMap)
                ? deviceIsAlignedMap
                : tsFileResourceManager.getDeviceIsAlignedMapFromCache(tsFile, true);
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

        deviceMeasurementsMap = readFilteredDeviceMeasurementsMap(devices);
        memoryRequiredInBytes +=
            PipeMemoryWeightUtil.memoryOfIDeviceID2StrList(deviceMeasurementsMap);
      }
      allocatedMemoryBlock =
          PipeDataNodeResourceManager.memory().forceAllocate(memoryRequiredInBytes);

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
    if (Objects.isNull(tablePattern)
        || !tablePattern.hasUserSpecifiedDatabasePatternOrTablePattern()) {
      return devices;
    }

    final Set<IDeviceID> filteredDevices = new HashSet<>();
    for (final IDeviceID device : devices) {
      if (tablePattern.matchesTable(device.getTableName())) {
        filteredDevices.add(device);
      }
    }
    return filteredDevices;
  }

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

  private Map<IDeviceID, List<String>> filterDeviceMeasurementsMapByPattern(
      final Map<IDeviceID, List<String>> originalDeviceMeasurementsMap) {
    final Map<IDeviceID, List<String>> filteredDeviceMeasurementsMap = new HashMap<>();
    for (Map.Entry<IDeviceID, List<String>> entry : originalDeviceMeasurementsMap.entrySet()) {
      final IDeviceID deviceId = entry.getKey();
      if (Objects.isNull(tablePattern) || tablePattern.matchesTable(deviceId.getTableName())) {
        filteredDeviceMeasurementsMap.put(deviceId, entry.getValue());
      }
    }
    return filteredDeviceMeasurementsMap;
  }

  @Override
  public Iterable<TabletInsertionEvent> toTabletInsertionEvents() {
    return () ->
        new Iterator<TabletInsertionEvent>() {

          private TsFileInsertionEventTableParserTabletIterator tabletIterator = null;

          @Override
          public boolean hasNext() {
            while (tabletIterator == null || !tabletIterator.hasNext()) {
              if (!deviceMeasurementsMapIterator.hasNext()) {
                close();
                return false;
              }

              final Map.Entry<IDeviceID, List<String>> entry = deviceMeasurementsMapIterator.next();

              try {
                tabletIterator =
                    new TsFileInsertionEventTableParserTabletIterator(
                        tableQueryExecutor,
                        entry.getKey(),
                        entry.getValue(),
                        tableSchemaMap,
                        timeFilterExpression,
                        startTime,
                        endTime,
                        allocatedMemoryBlockForTablet);
              } catch (final Exception e) {
                close();
                throw new PipeException("failed to create TsFileInsertionDataTabletIterator", e);
              }
            }

            return true;
          }

          @Override
          public TabletInsertionEvent next() {
            if (!hasNext()) {
              close();
              throw new NoSuchElementException();
            }

            final Tablet tablet = tabletIterator.next();

            final TabletInsertionEvent next;
            if (!hasNext()) {
              next =
                  new PipeRawTabletInsertionEvent(
                      Boolean.TRUE,
                      sourceEvent != null ? sourceEvent.getTreeModelDatabaseName() : null,
                      tablet,
                      true,
                      sourceEvent != null ? sourceEvent.getPipeName() : null,
                      sourceEvent != null ? sourceEvent.getCreationTime() : 0,
                      pipeTaskMeta,
                      sourceEvent,
                      true);
              close();
            } else {
              next =
                  new PipeRawTabletInsertionEvent(
                      Boolean.TRUE,
                      sourceEvent != null ? sourceEvent.getTreeModelDatabaseName() : null,
                      tablet,
                      true,
                      sourceEvent != null ? sourceEvent.getPipeName() : null,
                      sourceEvent != null ? sourceEvent.getCreationTime() : 0,
                      pipeTaskMeta,
                      sourceEvent,
                      false);
            }
            return next;
          }
        };
  }

  @Override
  public void close() {
    super.close();

    if (allocatedMemoryBlock != null) {
      allocatedMemoryBlock.close();
    }
  }
}
