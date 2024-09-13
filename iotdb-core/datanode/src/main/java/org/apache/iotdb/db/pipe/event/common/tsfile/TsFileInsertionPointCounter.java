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

import org.apache.iotdb.commons.pipe.pattern.PipePattern;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class TsFileInsertionPointCounter implements AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(TsFileInsertionPointCounter.class);

  private final PipePattern pattern;

  private final TsFileSequenceReader tsFileSequenceReader;

  final Map<IDeviceID, List<TimeseriesMetadata>> allDeviceTimeseriesMetadataMap;

  private boolean shouldParsePattern = false;

  private long count = 0;

  public TsFileInsertionPointCounter(final File tsFile, final PipePattern pattern)
      throws IOException {
    this.pattern = pattern;

    try {
      tsFileSequenceReader = new TsFileSequenceReader(tsFile.getPath(), true, true);
      allDeviceTimeseriesMetadataMap = tsFileSequenceReader.getAllTimeseriesMetadata(false);

      if (Objects.isNull(this.pattern) || pattern.isRoot()) {
        countAllTimeseriesPoints();
      } else {
        final Map<IDeviceID, Set<String>> filteredDeviceMeasurementMap =
            filterDeviceMeasurementsMapByPatternAndJudgeShouldParsePattern();
        if (shouldParsePattern) {
          countMatchedTimeseriesPoints(filteredDeviceMeasurementMap);
        } else {
          countAllTimeseriesPoints();
        }
      }

      // No longer need this. Help GC.
      tsFileSequenceReader.clearCachedDeviceMetadata();
    } catch (final Exception e) {
      close();
      throw e;
    }
  }

  private Map<IDeviceID, Set<String>>
      filterDeviceMeasurementsMapByPatternAndJudgeShouldParsePattern() throws IOException {
    // pattern should be non-null here
    final Map<IDeviceID, List<String>> originalDeviceMeasurementsMap =
        tsFileSequenceReader.getDeviceMeasurementsMap();
    final Map<IDeviceID, Set<String>> filteredDeviceMeasurementsMap = new HashMap<>();

    for (final Map.Entry<IDeviceID, List<String>> entry :
        originalDeviceMeasurementsMap.entrySet()) {
      final IDeviceID deviceId = entry.getKey();

      // case 1: for example, pattern is root.a.b or pattern is null and device is root.a.b.c
      // in this case, all data can be matched without checking the measurements
      if (pattern.coversDevice(deviceId)) {
        if (!entry.getValue().isEmpty()) {
          filteredDeviceMeasurementsMap.put(deviceId, new HashSet<>(entry.getValue()));
        }
      }

      // case 2: for example, pattern is root.a.b.c and device is root.a.b
      // in this case, we need to check the full path
      else if (pattern.mayOverlapWithDevice(deviceId)) {
        final Set<String> filteredMeasurements = new HashSet<>();

        for (final String measurement : entry.getValue()) {
          if (pattern.matchesMeasurement(deviceId, measurement)) {
            filteredMeasurements.add(measurement);
          } else {
            // Parse pattern iff there are measurements filtered out
            shouldParsePattern = true;
          }
        }

        if (!filteredMeasurements.isEmpty()) {
          filteredDeviceMeasurementsMap.put(deviceId, filteredMeasurements);
        }
      }

      // case 3: for example, pattern is root.a.b.c and device is root.a.b.d
      // in this case, no data can be matched
      else {
        // Parse pattern iff there are measurements filtered out
        shouldParsePattern = true;
      }
    }

    return filteredDeviceMeasurementsMap;
  }

  private void countMatchedTimeseriesPoints(
      final Map<IDeviceID, Set<String>> filteredDeviceMeasurementMap) {
    for (final Map.Entry<IDeviceID, List<TimeseriesMetadata>> deviceTimeseriesMetadataEntry :
        allDeviceTimeseriesMetadataMap.entrySet()) {
      final IDeviceID deviceId = deviceTimeseriesMetadataEntry.getKey();
      if (!filteredDeviceMeasurementMap.containsKey(deviceId)) {
        continue;
      }

      final List<TimeseriesMetadata> allTimeseriesMetadata =
          deviceTimeseriesMetadataEntry.getValue();
      final Set<String> filteredMeasurements = filteredDeviceMeasurementMap.get(deviceId);
      for (final TimeseriesMetadata timeseriesMetadata : allTimeseriesMetadata) {
        if (!filteredMeasurements.contains(timeseriesMetadata.getMeasurementId())) {
          continue;
        }

        count += timeseriesMetadata.getStatistics().getCount();
      }
    }
  }

  private void countAllTimeseriesPoints() {
    for (final List<TimeseriesMetadata> allTimeseriesMetadata :
        allDeviceTimeseriesMetadataMap.values()) {
      for (final TimeseriesMetadata timeseriesMetadata : allTimeseriesMetadata) {
        count += timeseriesMetadata.getStatistics().getCount();
      }
    }
  }

  public long count() {
    return count;
  }

  @Override
  public void close() {
    try {
      if (tsFileSequenceReader != null) {
        tsFileSequenceReader.close();
      }
    } catch (final IOException e) {
      LOGGER.warn("Failed to close TsFileSequenceReader", e);
    }
  }
}
