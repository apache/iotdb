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

package org.apache.iotdb.db.pipe.extractor.dataregion.realtime.epoch;

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
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
          LOGGER.info("TsFileEpoch not found for TsFile {}, creating a new one", path);
          return new TsFileEpoch(path);
        });

    final TsFileEpoch epoch = filePath2Epoch.remove(filePath);
    // When all data corresponding to this TsFileEpoch have been extracted, update the state
    // of the extractors processing this TsFileEpoch.
    epoch.setExtractorsRecentProcessedTsFileEpochState();

    LOGGER.info("All data in TsFileEpoch {} was extracted", epoch);
    return new PipeRealtimeEvent(
        event,
        epoch,
        resource.getDevices().stream()
            .collect(Collectors.toMap(Functions.identity(), device -> EMPTY_MEASUREMENT_ARRAY)),
        event.getPipePattern());
  }

  public PipeRealtimeEvent bindPipeInsertNodeTabletInsertionEvent(
      PipeInsertNodeTabletInsertionEvent event, InsertNode node, TsFileResource resource) {
    final TsFileEpoch epoch =
        filePath2Epoch.computeIfAbsent(resource.getTsFilePath(), TsFileEpoch::new);
    epoch.updateInsertNodeMinTime(node.getMinTime());
    return new PipeRealtimeEvent(
        event,
        epoch,
        node instanceof InsertRowsNode
            ? getDevice2MeasurementsMapFromInsertRowsNode((InsertRowsNode) node)
            : Collections.singletonMap(node.getDeviceID(), node.getMeasurements()),
        event.getPipePattern());
  }

  private Map<IDeviceID, String[]> getDevice2MeasurementsMapFromInsertRowsNode(
      InsertRowsNode insertRowsNode) {
    return insertRowsNode.getInsertRowNodeList().stream()
        .collect(
            Collectors.toMap(
                InsertNode::getDeviceID,
                InsertNode::getMeasurements,
                (oldMeasurements, newMeasurements) ->
                    Stream.of(Arrays.asList(oldMeasurements), Arrays.asList(newMeasurements))
                        .flatMap(Collection::stream)
                        .distinct()
                        .toArray(String[]::new)));
  }
}
