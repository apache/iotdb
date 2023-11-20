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

package org.apache.iotdb.db.pipe.event.common.tsfile;

import java.util.Collections;
import org.apache.iotdb.commons.pipe.task.meta.PipeTaskMeta;
import org.apache.iotdb.db.pipe.event.EnrichedEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.exception.PipeException;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.TsFileDeviceIterator;
import org.apache.iotdb.tsfile.read.TsFileReader;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.impl.BinaryExpression;
import org.apache.iotdb.tsfile.read.expression.impl.GlobalTimeExpression;
import org.apache.iotdb.tsfile.read.filter.TimeFilter;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.record.Tablet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

public class TsFileListInsertionDataContainer implements AutoCloseable {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(TsFileListInsertionDataContainer.class);

  private final String pattern; // used to filter data
  private final IExpression timeFilterExpression; // used to filter data

  private final PipeTaskMeta pipeTaskMeta; // used to report progress
  private final EnrichedEvent sourceEvent; // used to report progress

  private final List<TsFileSequenceReader> tsFileSequenceReaders;
  private final List<TsFileReader> tsFileReaders;
  private int currFileIndex = 0;

  private final List<Iterator<Map.Entry<String, List<String>>>> deviceMeasurementsMapIterators;
  private final List<Map<String, Boolean>> deviceIsAlignedMaps;
  private final List<Map<String, TSDataType>> measurementDataTypeMaps;

  public TsFileListInsertionDataContainer(
      List<File> tsFiles, String pattern, long startTime, long endTime) throws IOException {
    this(tsFiles, pattern, startTime, endTime, null, null);
  }

  public TsFileListInsertionDataContainer(
      List<File> tsFiles,
      String pattern,
      long startTime,
      long endTime,
      PipeTaskMeta pipeTaskMeta,
      EnrichedEvent sourceEvent)
      throws IOException {
    this.pattern = pattern;
    timeFilterExpression =
        (startTime == Long.MIN_VALUE && endTime == Long.MAX_VALUE)
            ? null
            : BinaryExpression.and(
                new GlobalTimeExpression(TimeFilter.gtEq(startTime)),
                new GlobalTimeExpression(TimeFilter.ltEq(endTime)));

    this.pipeTaskMeta = pipeTaskMeta;
    this.sourceEvent = sourceEvent;

    try {
      tsFileSequenceReaders = new ArrayList<>();
      tsFileReaders = new ArrayList<>();
      for (File tsFile : tsFiles) {
        TsFileSequenceReader tsFileSequenceReader =
            new TsFileSequenceReader(tsFile.getAbsolutePath(), true, true);
        tsFileSequenceReaders.add(tsFileSequenceReader);
        tsFileReaders.add(new TsFileReader(tsFileSequenceReader));
      }

      deviceMeasurementsMapIterators = new ArrayList<>();
      deviceIsAlignedMaps = new ArrayList<>();
      for (int i = 0; i < tsFiles.size(); i++) {
        deviceMeasurementsMapIterators.add(
            filterDeviceMeasurementsMapByPattern(i).entrySet().iterator());
        deviceIsAlignedMaps.add(readDeviceIsAlignedMap(i));
      }
      measurementDataTypeMaps = new ArrayList<>();
      for (TsFileSequenceReader tsFileSequenceReader : tsFileSequenceReaders) {
        measurementDataTypeMaps.add(tsFileSequenceReader.getFullPathDataTypeMap());
      }
    } catch (Exception e) {
      close();
      throw e;
    }
  }

  private Map<String, List<String>> filterDeviceMeasurementsMapByPattern(int i) throws IOException {
    final Map<String, List<String>> filteredDeviceMeasurementsMap = new HashMap<>();

    for (Map.Entry<String, List<String>> entry :
        tsFileSequenceReaders.get(i).getDeviceMeasurementsMap().entrySet()) {
      final String deviceId = entry.getKey();
      List<String> filteredMeasurements = Collections.emptyList();

      // case 1: for example, pattern is root.a.b or pattern is null and device is root.a.b.c
      // in this case, all data can be matched without checking the measurements
      if (pattern == null
          || pattern.length() <= deviceId.length() && deviceId.startsWith(pattern)) {
        filteredMeasurements = entry.getValue();
      }
      // case 2: for example, pattern is root.a.b.c and device is root.a.b
      // in this case, we need to check the full path
      else if (pattern.length() > deviceId.length() && pattern.startsWith(deviceId)) {
        filteredMeasurements = filterMeasurements(entry.getValue(), deviceId);
      }
      if (!filteredMeasurements.isEmpty()) {
        filteredDeviceMeasurementsMap.put(deviceId, filteredMeasurements);
      }
    }

    return filteredDeviceMeasurementsMap;
  }

  private List<String> filterMeasurements(List<String> measurements, String deviceId) {
    for (final String measurement : measurements) {
      // low cost check comes first
      if (pattern.length() == deviceId.length() + measurement.length() + 1
          // high cost check comes later
          && pattern.endsWith(TsFileConstant.PATH_SEPARATOR + measurement)) {
        // if one measurement matches, other cannot match
        return Collections.singletonList(measurement);
      }
    }
    return Collections.emptyList();
  }

  private Map<String, Boolean> readDeviceIsAlignedMap(int i) throws IOException {
    final Map<String, Boolean> deviceIsAlignedResultMap = new HashMap<>();
    final TsFileDeviceIterator deviceIsAlignedIterator =
        tsFileSequenceReaders.get(i).getAllDevicesIteratorWithIsAligned();
    while (deviceIsAlignedIterator.hasNext()) {
      final Pair<String, Boolean> deviceIsAlignedPair = deviceIsAlignedIterator.next();
      deviceIsAlignedResultMap.put(deviceIsAlignedPair.getLeft(), deviceIsAlignedPair.getRight());
    }
    return deviceIsAlignedResultMap;
  }

  /** @return TabletInsertionEvent in a streaming way */
  public Iterable<TabletInsertionEvent> toTabletInsertionEvents() {
    return this::tabletInsertionEventsIterator;
  }

  public Iterator<TabletInsertionEvent> tabletInsertionEventsIterator() {
    return new TabletInsertionEventIterator();

  }

  public class TabletInsertionEventIterator implements Iterator<TabletInsertionEvent> {
    private TsFileInsertionDataTabletIterator tabletIterator = null;

    @Override
    public boolean hasNext() {
      while (tabletIterator == null || !tabletIterator.hasNext()) {
        if (!deviceMeasurementsMapIterators.get(currFileIndex).hasNext()) {
          if (currFileIndex >= tsFileReaders.size()) {
            close();
            return false;
          } else {
            currFileIndex++;
            continue;
          }
        }

        final Map.Entry<String, List<String>> entry =
            deviceMeasurementsMapIterators.get(currFileIndex).next();

        try {
          tabletIterator =
              new TsFileInsertionDataTabletIterator(
                  tsFileReaders.get(currFileIndex),
                  measurementDataTypeMaps.get(currFileIndex),
                  entry.getKey(),
                  entry.getValue(),
                  timeFilterExpression);
        } catch (IOException e) {
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
      final boolean isAligned =
          deviceIsAlignedMaps.get(currFileIndex).getOrDefault(tablet.deviceId, false);

      final TabletInsertionEvent next;
      if (!hasNext()) {
        next =
            new PipeRawTabletInsertionEvent(
                tablet, isAligned, pipeTaskMeta, sourceEvent, true);
        close();
      } else {
        next =
            new PipeRawTabletInsertionEvent(
                tablet, isAligned, pipeTaskMeta, sourceEvent, false);
      }
      return next;
    }
  }

  @Override
  public void close() {
    for (TsFileReader tsFileReader : tsFileReaders) {
      try {
        if (tsFileReader != null) {
          tsFileReader.close();
        }
      } catch (IOException e) {
        LOGGER.warn("Failed to close TsFileReader", e);
      }
    }

    for (TsFileSequenceReader tsFileSequenceReader : tsFileSequenceReaders) {
      try {
        if (tsFileSequenceReader != null) {
          tsFileSequenceReader.close();
        }
      } catch (IOException e) {
        LOGGER.warn("Failed to close TsFileSequenceReader", e);
      }
    }
  }
}
