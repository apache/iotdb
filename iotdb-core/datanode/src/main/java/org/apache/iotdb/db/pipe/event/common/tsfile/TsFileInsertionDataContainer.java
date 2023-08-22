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

public class TsFileInsertionDataContainer implements AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(TsFileInsertionDataContainer.class);

  private final String pattern; // used to filter data
  private final IExpression timeFilterExpression; // used to filter data

  private final PipeTaskMeta pipeTaskMeta; // used to report progress
  private final EnrichedEvent sourceEvent; // used to report progress

  private final TsFileSequenceReader tsFileSequenceReader;
  private final TsFileReader tsFileReader;

  private final Iterator<Map.Entry<String, List<String>>> deviceMeasurementsMapIterator;
  private final Map<String, Boolean> deviceIsAlignedMap;
  private final Map<String, TSDataType> measurementDataTypeMap;

  public TsFileInsertionDataContainer(File tsFile, String pattern, long startTime, long endTime)
      throws IOException {
    this(tsFile, pattern, startTime, endTime, null, null);
  }

  public TsFileInsertionDataContainer(
      File tsFile,
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
      tsFileSequenceReader = new TsFileSequenceReader(tsFile.getAbsolutePath());
      tsFileReader = new TsFileReader(tsFileSequenceReader);

      deviceMeasurementsMapIterator = filterDeviceMeasurementsMapByPattern().entrySet().iterator();
      deviceIsAlignedMap = readDeviceIsAlignedMap();
      measurementDataTypeMap = tsFileSequenceReader.getFullPathDataTypeMap();
    } catch (Exception e) {
      close();
      throw e;
    }
  }

  private Map<String, List<String>> filterDeviceMeasurementsMapByPattern() throws IOException {
    final Map<String, List<String>> filteredDeviceMeasurementsMap = new HashMap<>();

    for (Map.Entry<String, List<String>> entry :
        tsFileSequenceReader.getDeviceMeasurementsMap().entrySet()) {
      final String deviceId = entry.getKey();

      // case 1: for example, pattern is root.a.b or pattern is null and device is root.a.b.c
      // in this case, all data can be matched without checking the measurements
      if (pattern == null
          || pattern.length() <= deviceId.length() && deviceId.startsWith(pattern)) {
        if (!entry.getValue().isEmpty()) {
          filteredDeviceMeasurementsMap.put(deviceId, entry.getValue());
        }
      }

      // case 2: for example, pattern is root.a.b.c and device is root.a.b
      // in this case, we need to check the full path
      else if (pattern.length() > deviceId.length() && pattern.startsWith(deviceId)) {
        final List<String> filteredMeasurements = new ArrayList<>();

        for (final String measurement : entry.getValue()) {
          // low cost check comes first
          if (pattern.length() == deviceId.length() + measurement.length() + 1
              // high cost check comes later
              && pattern.endsWith(TsFileConstant.PATH_SEPARATOR + measurement)) {
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

  private Map<String, Boolean> readDeviceIsAlignedMap() throws IOException {
    final Map<String, Boolean> deviceIsAlignedResultMap = new HashMap<>();
    final TsFileDeviceIterator deviceIsAlignedIterator =
        tsFileSequenceReader.getAllDevicesIteratorWithIsAligned();
    while (deviceIsAlignedIterator.hasNext()) {
      final Pair<String, Boolean> deviceIsAlignedPair = deviceIsAlignedIterator.next();
      deviceIsAlignedResultMap.put(deviceIsAlignedPair.getLeft(), deviceIsAlignedPair.getRight());
    }
    return deviceIsAlignedResultMap;
  }

  /** @return TabletInsertionEvent in a streaming way */
  public Iterable<TabletInsertionEvent> toTabletInsertionEvents() {
    return () ->
        new Iterator<TabletInsertionEvent>() {

          private TsFileInsertionDataTabletIterator tabletIterator = null;

          @Override
          public boolean hasNext() {
            while (tabletIterator == null || !tabletIterator.hasNext()) {
              if (!deviceMeasurementsMapIterator.hasNext()) {
                return false;
              }

              final Map.Entry<String, List<String>> entry = deviceMeasurementsMapIterator.next();

              try {
                tabletIterator =
                    new TsFileInsertionDataTabletIterator(
                        tsFileReader,
                        measurementDataTypeMap,
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
            final boolean isAligned = deviceIsAlignedMap.getOrDefault(tablet.deviceId, false);

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
        };
  }

  @Override
  public void close() {
    try {
      if (tsFileReader != null) {
        tsFileReader.close();
      }
    } catch (IOException e) {
      LOGGER.warn("Failed to close TsFileReader", e);
    }

    try {
      if (tsFileSequenceReader != null) {
        tsFileSequenceReader.close();
      }
    } catch (IOException e) {
      LOGGER.warn("Failed to close TsFileSequenceReader", e);
    }
  }
}
