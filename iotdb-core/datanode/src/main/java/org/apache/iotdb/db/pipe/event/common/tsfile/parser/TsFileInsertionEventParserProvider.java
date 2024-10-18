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

package org.apache.iotdb.db.pipe.event.common.tsfile.parser;

import org.apache.iotdb.commons.pipe.agent.task.meta.PipeTaskMeta;
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.commons.pipe.datastructure.pattern.IoTDBTreePattern;
import org.apache.iotdb.commons.pipe.datastructure.pattern.TablePattern;
import org.apache.iotdb.commons.pipe.datastructure.pattern.TreePattern;
import org.apache.iotdb.db.pipe.event.common.tsfile.PipeTsFileInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tsfile.parser.query.TsFileInsertionEventQueryParser;
import org.apache.iotdb.db.pipe.event.common.tsfile.parser.scan.TsFileInsertionEventScanParser;
import org.apache.iotdb.db.pipe.event.common.tsfile.parser.table.TsFileInsertionEventTableParser;
import org.apache.iotdb.db.pipe.resource.PipeDataNodeResourceManager;

import org.apache.tsfile.file.metadata.IDeviceID;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class TsFileInsertionEventParserProvider {

  private final File tsFile;
  private final TreePattern treePattern;
  private final TablePattern tablePattern;
  private final long startTime;
  private final long endTime;

  protected final PipeTaskMeta pipeTaskMeta;
  protected final PipeTsFileInsertionEvent sourceEvent;

  public TsFileInsertionEventParserProvider(
      final File tsFile,
      final TreePattern treePattern,
      final TablePattern tablePattern,
      final long startTime,
      final long endTime,
      final PipeTaskMeta pipeTaskMeta,
      final PipeTsFileInsertionEvent sourceEvent) {
    this.tsFile = tsFile;
    this.treePattern = treePattern;
    this.tablePattern = tablePattern;
    this.startTime = startTime;
    this.endTime = endTime;
    this.pipeTaskMeta = pipeTaskMeta;
    this.sourceEvent = sourceEvent;
  }

  public TsFileInsertionEventParser provide() throws IOException {
    if (sourceEvent.isTableModelEvent()) {
      return new TsFileInsertionEventTableParser(
          tsFile, tablePattern, startTime, endTime, pipeTaskMeta, sourceEvent);
    }

    if (startTime != Long.MIN_VALUE
        || endTime != Long.MAX_VALUE
        || treePattern instanceof IoTDBTreePattern
            && !((IoTDBTreePattern) treePattern).mayMatchMultipleTimeSeriesInOneDevice()) {
      // 1. If time filter exists, use query here because the scan container may filter it
      // row by row in single page chunk.
      // 2. If the pattern matches only one time series in one device, use query container here
      // because there is no timestamps merge overhead.
      //
      // Note: We judge prefix pattern as matching multiple timeseries in one device because it's
      // hard to know whether it only matches one timeseries, while matching multiple is often the
      // case.
      return new TsFileInsertionEventQueryParser(
          tsFile, treePattern, startTime, endTime, pipeTaskMeta, sourceEvent);
    }

    final Map<IDeviceID, Boolean> deviceIsAlignedMap =
        PipeDataNodeResourceManager.tsfile().getDeviceIsAlignedMapFromCache(tsFile, false);
    if (Objects.isNull(deviceIsAlignedMap)) {
      // If we failed to get from cache, it indicates that the memory usage is high.
      // We use scan data container because it requires less memory.
      return new TsFileInsertionEventScanParser(
          tsFile, treePattern, startTime, endTime, pipeTaskMeta, sourceEvent);
    }

    final int originalSize = deviceIsAlignedMap.size();
    final Map<IDeviceID, Boolean> filteredDeviceIsAlignedMap =
        filterDeviceIsAlignedMapByPattern(deviceIsAlignedMap);
    // Use scan data container if we need enough amount to data thus it's better to scan than query.
    return (double) filteredDeviceIsAlignedMap.size() / originalSize
            > PipeConfig.getInstance().getPipeTsFileScanParsingThreshold()
        ? new TsFileInsertionEventScanParser(
            tsFile, treePattern, startTime, endTime, pipeTaskMeta, sourceEvent)
        : new TsFileInsertionEventQueryParser(
            tsFile,
            treePattern,
            startTime,
            endTime,
            pipeTaskMeta,
            sourceEvent,
            filteredDeviceIsAlignedMap);
  }

  private Map<IDeviceID, Boolean> filterDeviceIsAlignedMapByPattern(
      final Map<IDeviceID, Boolean> deviceIsAlignedMap) {
    if (Objects.isNull(treePattern) || treePattern.isRoot()) {
      return deviceIsAlignedMap;
    }

    return deviceIsAlignedMap.entrySet().stream()
        .filter(
            entry -> {
              final IDeviceID deviceId = entry.getKey();
              return treePattern.coversDevice(deviceId)
                  || treePattern.mayOverlapWithDevice(deviceId);
            })
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }
}
