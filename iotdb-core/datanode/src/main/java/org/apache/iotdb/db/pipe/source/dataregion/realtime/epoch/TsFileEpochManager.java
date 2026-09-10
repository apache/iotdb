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

package org.apache.iotdb.db.pipe.source.dataregion.realtime.epoch;

import org.apache.iotdb.db.i18n.DataNodePipeMessages;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeInsertNodeTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tsfile.PipeTsFileInsertionEvent;
import org.apache.iotdb.db.pipe.event.realtime.PipeRealtimeEvent;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowsNode;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import com.google.common.base.Functions;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

public class TsFileEpochManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(TsFileEpochManager.class);

  private static final String[] EMPTY_MEASUREMENT_ARRAY = new String[0];

  private final ConcurrentMap<String, TsFileEpoch> filePath2Epoch = new ConcurrentHashMap<>();

  public PipeRealtimeEvent bindPipeTsFileInsertionEvent(
      PipeTsFileInsertionEvent event, TsFileResource resource) {
    final String filePath = resource.getTsFilePath();

    // This would not happen, but just in case
    filePath2Epoch.computeIfAbsent(
        filePath,
        path -> {
          LOGGER.info(DataNodePipeMessages.TSFILEEPOCH_NOT_FOUND_FOR_TSFILE_CREATING_A, path);
          return new TsFileEpoch(resource);
        });

    final TsFileEpoch epoch = filePath2Epoch.remove(filePath);

    LOGGER.info(DataNodePipeMessages.ALL_DATA_IN_TSFILEEPOCH_WAS_EXTRACTED, epoch);
    return new PipeRealtimeEvent(
        event,
        epoch,
        resource.getDevices().stream()
            .collect(Collectors.toMap(Functions.identity(), device -> EMPTY_MEASUREMENT_ARRAY)));
  }

  public PipeRealtimeEvent bindPipeInsertNodeTabletInsertionEvent(
      PipeInsertNodeTabletInsertionEvent event, InsertNode node, TsFileResource resource) {
    final TsFileEpoch epoch =
        filePath2Epoch.computeIfAbsent(resource.getTsFilePath(), k -> new TsFileEpoch(resource));
    epoch.updateInsertNodeMinTime(node.getMinTime());
    return new PipeRealtimeEvent(
        event,
        epoch,
        node instanceof InsertRowsNode
            ? getDevice2MeasurementsMapFromInsertRowsNode((InsertRowsNode) node)
            : Collections.singletonMap(node.getDeviceID(), node.getMeasurements()));
  }

  static Map<IDeviceID, String[]> getDevice2MeasurementsMapFromInsertRowsNode(
      InsertRowsNode insertRowsNode) {
    final Map<IDeviceID, String[]> device2Measurements = new HashMap<>();
    final Map<IDeviceID, Set<String>> device2DistinctMeasurements = new HashMap<>();

    // This method runs synchronously in the write path. Rebuilding a stream, a hash set, and an
    // array for every row is expensive when an InsertRowsNode contains many rows for one device.
    // Keep one set per repeated device instead and materialize its array only once.
    for (final InsertNode insertRowNode : insertRowsNode.getInsertRowNodeList()) {
      final IDeviceID deviceID = insertRowNode.getDeviceID();
      final String[] measurements = Objects.requireNonNull(insertRowNode.getMeasurements());
      final String[] firstMeasurements = device2Measurements.putIfAbsent(deviceID, measurements);
      if (firstMeasurements == null) {
        continue;
      }

      final Set<String> distinctMeasurements =
          device2DistinctMeasurements.computeIfAbsent(
              deviceID, key -> new LinkedHashSet<>(Arrays.asList(firstMeasurements)));
      addDistinctMeasurements(firstMeasurements, measurements, distinctMeasurements);
    }

    device2DistinctMeasurements.forEach(
        (deviceID, measurements) ->
            device2Measurements.put(
                deviceID, measurements.toArray(new String[measurements.size()])));
    return device2Measurements;
  }

  private static void addDistinctMeasurements(
      final String[] firstMeasurements,
      final String[] measurements,
      final Set<String> distinctMeasurements) {
    if (measurements.length >= firstMeasurements.length) {
      if (!Arrays.equals(firstMeasurements, measurements)) {
        Collections.addAll(distinctMeasurements, measurements);
      }
      return;
    }

    // Skip the longest prefix that is already present as an ordered subsequence of the first row.
    int firstMeasurementIndex = 0;
    int measurementIndex = 0;
    while (firstMeasurementIndex < firstMeasurements.length
        && measurementIndex < measurements.length) {
      if (Objects.equals(
          firstMeasurements[firstMeasurementIndex], measurements[measurementIndex])) {
        ++measurementIndex;
      }
      ++firstMeasurementIndex;
    }
    while (measurementIndex < measurements.length) {
      distinctMeasurements.add(measurements[measurementIndex++]);
    }
  }
}
