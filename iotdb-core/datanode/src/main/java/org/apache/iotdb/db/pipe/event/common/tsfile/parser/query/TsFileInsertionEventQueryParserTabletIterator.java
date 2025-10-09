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

import org.apache.iotdb.commons.path.PatternTreeMap;
import org.apache.iotdb.db.pipe.resource.PipeDataNodeResourceManager;
import org.apache.iotdb.db.pipe.resource.memory.PipeMemoryBlock;
import org.apache.iotdb.db.pipe.resource.memory.PipeMemoryWeightUtil;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModEntry;
import org.apache.iotdb.db.utils.datastructure.PatternTreeMapFactory;
import org.apache.iotdb.pipe.api.exception.PipeException;

import org.apache.tsfile.common.constant.TsFileConstant;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.read.TsFileReader;
import org.apache.tsfile.read.common.Field;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.read.common.RowRecord;
import org.apache.tsfile.read.expression.IExpression;
import org.apache.tsfile.read.expression.QueryExpression;
import org.apache.tsfile.read.query.dataset.QueryDataSet;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.stream.Collectors;

public class TsFileInsertionEventQueryParserTabletIterator implements Iterator<Tablet> {

  private final TsFileReader tsFileReader;
  private final Map<String, TSDataType> measurementDataTypeMap;

  private final IDeviceID deviceId;
  private final List<String> measurements;

  private final IExpression timeFilterExpression;

  private final QueryDataSet queryDataSet;

  private final PipeMemoryBlock allocatedBlockForTablet;

  private RowRecord rowRecord;

  // mods entry
  private final PatternTreeMap<ModEntry, PatternTreeMapFactory.ModsSerializer> currentModifications;

  // Maintain sorted mods list and current index for each measurement
  private final Map<String, Pair<List<ModEntry>, Integer>> measurementModsMap = new HashMap<>();

  TsFileInsertionEventQueryParserTabletIterator(
      final TsFileReader tsFileReader,
      final Map<String, TSDataType> measurementDataTypeMap,
      final IDeviceID deviceId,
      final List<String> measurements,
      final IExpression timeFilterExpression,
      final PipeMemoryBlock allocatedBlockForTablet,
      final PatternTreeMap<ModEntry, PatternTreeMapFactory.ModsSerializer> currentModifications)
      throws IOException {
    this.tsFileReader = tsFileReader;
    this.measurementDataTypeMap = measurementDataTypeMap;

    this.deviceId = deviceId;
    this.measurements =
        measurements.stream()
            .filter(
                measurement ->
                    // time column in aligned time-series should not be a query column
                    measurement != null && !measurement.isEmpty())
            .sorted()
            .collect(Collectors.toList());

    this.timeFilterExpression = timeFilterExpression;

    this.queryDataSet = buildQueryDataSet();

    this.allocatedBlockForTablet = Objects.requireNonNull(allocatedBlockForTablet);
    this.currentModifications = Objects.requireNonNull(currentModifications);

    // 初始化每个measurement的mods列表
    initializeMeasurementMods();
  }

  private QueryDataSet buildQueryDataSet() throws IOException {
    final List<Path> paths = new ArrayList<>();
    for (final String measurement : measurements) {
      paths.add(new Path(deviceId, measurement, false));
    }
    return tsFileReader.query(QueryExpression.create(paths, timeFilterExpression));
  }

  @Override
  public boolean hasNext() {
    try {
      return queryDataSet.hasNext();
    } catch (final IOException e) {
      throw new PipeException("Failed to check next", e);
    }
  }

  @Override
  public Tablet next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }

    try {
      return buildNextTablet();
    } catch (final IOException e) {
      throw new PipeException("Failed to build tablet", e);
    }
  }

  private Tablet buildNextTablet() throws IOException {
    final List<IMeasurementSchema> schemas = new ArrayList<>();
    for (final String measurement : measurements) {
      final TSDataType dataType =
          measurementDataTypeMap.get(deviceId + TsFileConstant.PATH_SEPARATOR + measurement);
      schemas.add(new MeasurementSchema(measurement, dataType));
    }

    Tablet tablet = null;
    if (!queryDataSet.hasNext()) {
      tablet =
          new Tablet(
              // Used for tree model
              deviceId.toString(), schemas, 1);
      tablet.initBitMaps();
      return tablet;
    }

    boolean isFirstRow = true;
    while (queryDataSet.hasNext()) {
      final RowRecord rowRecord = this.rowRecord != null ? this.rowRecord : queryDataSet.next();
      if (isFirstRow) {
        // Calculate row count and memory size of the tablet based on the first row
        this.rowRecord = rowRecord; // Save the first row for later use
        Pair<Integer, Integer> rowCountAndMemorySize =
            PipeMemoryWeightUtil.calculateTabletRowCountAndMemory(rowRecord);
        tablet =
            new Tablet(
                // Used for tree model
                deviceId.toString(), schemas, rowCountAndMemorySize.getLeft());
        tablet.initBitMaps();
        if (allocatedBlockForTablet.getMemoryUsageInBytes() < rowCountAndMemorySize.getRight()) {
          PipeDataNodeResourceManager.memory()
              .forceResize(allocatedBlockForTablet, rowCountAndMemorySize.getRight());
        }
        this.rowRecord = null; // Clear the saved first row
        isFirstRow = false;
      }

      final int rowIndex = tablet.getRowSize();

      tablet.addTimestamp(rowIndex, rowRecord.getTimestamp());

      final List<Field> fields = rowRecord.getFields();
      final int fieldSize = fields.size();
      for (int i = 0; i < fieldSize; i++) {
        final Field field = fields.get(i);
        final String measurement = measurements.get(i);

        // Check if this value is deleted by mods
        if (field == null || isDelete(measurement, rowRecord.getTimestamp())) {
          tablet.getBitMaps()[i].mark(rowIndex);
        } else {
          tablet.addValue(measurement, rowIndex, field.getObjectValue(schemas.get(i).getType()));
        }
      }

      if (tablet.getRowSize() == tablet.getMaxRowNumber()) {
        break;
      }
    }

    return tablet;
  }

  private void initializeMeasurementMods() {
    for (final String measurement : measurements) {
      final List<ModEntry> mods = currentModifications.getOverlapped(deviceId, measurement);
      if (mods == null || mods.isEmpty()) {
        // No mods, use empty list and index 0
        measurementModsMap.put(measurement, new Pair<>(Collections.EMPTY_LIST, 0));
        continue;
      }

      // Sort by time range for efficient lookup
      final List<ModEntry> sortedMods =
          mods.stream()
              .filter(modification -> (!deviceId.isTableModel() || modification.affects(deviceId)))
              .sorted(
                  (m1, m2) -> Long.compare(m1.getTimeRange().getMin(), m2.getTimeRange().getMin()))
              .collect(Collectors.toList());

      // Store sorted mods and start index
      measurementModsMap.put(measurement, new Pair<>(sortedMods, 0));
    }
  }

  private boolean isDelete(String measurementID, long time) {
    final Pair<List<ModEntry>, Integer> modsPair = measurementModsMap.get(measurementID);
    if (modsPair == null) {
      return false;
    }

    final List<ModEntry> mods = modsPair.getLeft();
    if (mods == null || mods.isEmpty()) {
      return false;
    }

    Integer currentIndex = modsPair.getRight();
    if (currentIndex == null || currentIndex < 0) {
      return false;
    }

    // Search from current index
    for (int i = currentIndex; i < mods.size(); i++) {
      final ModEntry mod = mods.get(i);
      final long modStartTime = mod.getTimeRange().getMin();
      final long modEndTime = mod.getTimeRange().getMax();

      if (time < modStartTime) {
        // Current time is before mod start time, update index and return false
        measurementModsMap.put(measurementID, new Pair<>(mods, i));
        return false;
      } else if (time <= modEndTime) {
        // Current time is within mod time range, update index and return true
        measurementModsMap.put(measurementID, new Pair<>(mods, i));
        return true;
      }
      // If time > modEndTime, continue to next mod
    }

    // All mods checked, clear mods list and reset index to 0
    measurementModsMap.put(measurementID, new Pair<>(Collections.EMPTY_LIST, 0));
    return false;
  }
}
