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
import org.apache.iotdb.db.i18n.DataNodePipeMessages;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeTabletUtils;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeTabletUtils.TabletStringInternPool;
import org.apache.iotdb.db.pipe.event.common.tsfile.parser.util.ModsOperationUtil;
import org.apache.iotdb.db.pipe.event.common.util.PipeDataLossDebugUtil;
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
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.stream.Collectors;

public class TsFileInsertionEventQueryParserTabletIterator implements Iterator<Tablet> {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(TsFileInsertionEventQueryParserTabletIterator.class);

  private final TsFileReader tsFileReader;
  private final Map<String, TSDataType> measurementDataTypeMap;

  private final IDeviceID deviceId;
  private final String deviceIdString;
  private final List<String> measurements;
  private final List<IMeasurementSchema> schemas;

  private final IExpression timeFilterExpression;

  private final QueryDataSet queryDataSet;

  private final PipeMemoryBlock allocatedBlockForTablet;

  // Maintain sorted mods list and current index for each measurement
  private final List<ModsOperationUtil.ModsInfo> measurementModsList;

  private RowRecord rowRecord;

  private final String pipeName;
  private final long creationTime;
  private final String tsFilePath;

  TsFileInsertionEventQueryParserTabletIterator(
      final TsFileReader tsFileReader,
      final Map<String, TSDataType> measurementDataTypeMap,
      final IDeviceID deviceId,
      final List<String> measurements,
      final IExpression timeFilterExpression,
      final PipeMemoryBlock allocatedBlockForTablet,
      final PatternTreeMap<ModEntry, PatternTreeMapFactory.ModsSerializer> currentModifications,
      final TabletStringInternPool tabletStringInternPool,
      final String pipeName,
      final long creationTime,
      final String tsFilePath)
      throws IOException {
    this.tsFileReader = tsFileReader;
    this.measurementDataTypeMap = measurementDataTypeMap;

    this.deviceId = deviceId;
    this.deviceIdString = tabletStringInternPool.intern(deviceId.toString());
    this.pipeName = pipeName;
    this.creationTime = creationTime;
    this.tsFilePath = tsFilePath;
    this.measurements =
        measurements.stream()
            .filter(
                measurement ->
                    // time column in aligned time-series should not be a query column
                    measurement != null && !measurement.isEmpty())
            .map(tabletStringInternPool::intern)
            .sorted()
            .collect(Collectors.toList());
    this.schemas = new ArrayList<>();
    for (final String measurement : this.measurements) {
      final TSDataType dataType =
          measurementDataTypeMap.get(deviceIdString + TsFileConstant.PATH_SEPARATOR + measurement);
      schemas.add(new MeasurementSchema(measurement, dataType));
    }

    this.timeFilterExpression = timeFilterExpression;

    this.queryDataSet = buildQueryDataSet();

    this.allocatedBlockForTablet = Objects.requireNonNull(allocatedBlockForTablet);

    this.measurementModsList =
        ModsOperationUtil.initializeMeasurementMods(
            deviceId, this.measurements, currentModifications);
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
      throw new PipeException(DataNodePipeMessages.FAILED_TO_CHECK_NEXT, e);
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
      throw new PipeException(DataNodePipeMessages.FAILED_TO_BUILD_TABLET, e);
    }
  }

  private Tablet buildNextTablet() throws IOException {
    Tablet tablet = null;
    if (!queryDataSet.hasNext()) {
      tablet =
          new Tablet(
              // Used for tree model
              deviceIdString, schemas, 1);
      return tablet;
    }

    int inputRowCount = 0;
    int droppedAllNullRowCount = 0;
    int nullFieldCount = 0;
    int deletedFieldCount = 0;
    int nonNullFieldCount = 0;
    long firstInputTime = Long.MIN_VALUE;
    long lastInputTime = Long.MIN_VALUE;
    long firstOutputTime = Long.MIN_VALUE;
    long lastOutputTime = Long.MIN_VALUE;

    boolean isFirstRow = true;
    while (queryDataSet.hasNext()) {
      final RowRecord rowRecord = this.rowRecord != null ? this.rowRecord : queryDataSet.next();
      final long timestamp = rowRecord.getTimestamp();
      ++inputRowCount;
      if (firstInputTime == Long.MIN_VALUE) {
        firstInputTime = timestamp;
      }
      lastInputTime = timestamp;
      if (isFirstRow) {
        // Calculate row count and memory size of the tablet based on the first row
        this.rowRecord = rowRecord; // Save the first row for later use
        Pair<Integer, Integer> rowCountAndMemorySize =
            PipeMemoryWeightUtil.calculateTabletRowCountAndMemory(rowRecord);
        tablet =
            new Tablet(
                // Used for tree model
                deviceIdString, schemas, rowCountAndMemorySize.getLeft());
        if (allocatedBlockForTablet.getMemoryUsageInBytes() < rowCountAndMemorySize.getRight()) {
          PipeDataNodeResourceManager.memory()
              .forceResize(allocatedBlockForTablet, rowCountAndMemorySize.getRight());
        }
        this.rowRecord = null; // Clear the saved first row
        isFirstRow = false;
      }

      final int rowIndex = tablet.getRowSize();

      boolean isNeedFillTime = false;
      final List<Field> fields = rowRecord.getFields();
      final int fieldSize = fields.size();
      for (int i = 0; i < fieldSize; i++) {
        final Field field = fields.get(i);
        final String measurement = measurements.get(i);
        final TSDataType dataType = schemas.get(i).getType();
        final boolean isDeleted =
            field != null && ModsOperationUtil.isDelete(timestamp, measurementModsList.get(i));
        // Check if this value is deleted by mods
        if (field == null || isDeleted) {
          if (field == null) {
            ++nullFieldCount;
          } else {
            ++deletedFieldCount;
          }
          if (dataType.isBinary()) {
            PipeTabletUtils.putValue(tablet, rowIndex, i, dataType, Binary.EMPTY_VALUE);
          }
          PipeTabletUtils.markNullValue(tablet, rowIndex, i);
        } else {
          ++nonNullFieldCount;
          PipeTabletUtils.putValue(
              tablet, rowIndex, i, dataType, field.getObjectValue(schemas.get(i).getType()));
          isNeedFillTime = true;
        }
      }
      if (isNeedFillTime) {
        if (firstOutputTime == Long.MIN_VALUE) {
          firstOutputTime = timestamp;
        }
        lastOutputTime = timestamp;
        PipeTabletUtils.putTimestamp(tablet, rowIndex, timestamp);
      } else {
        ++droppedAllNullRowCount;
      }

      if (tablet.getRowSize() == tablet.getMaxRowNumber()) {
        break;
      }
    }

    PipeTabletUtils.compactBitMaps(tablet);
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          "{} query parser emitted tablet, {}, tsFile={}, device={}, measurements={}, inputRows={}, outputRows={}, droppedAllNullRows={}, nullFields={}, deletedFields={}, nonNullFields={}, firstInputTime={}, lastInputTime={}, firstOutputTime={}, lastOutputTime={}, tablet={}",
          PipeDataLossDebugUtil.PREFIX,
          PipeDataLossDebugUtil.formatPipe(pipeName, creationTime),
          tsFilePath,
          deviceIdString,
          measurements,
          inputRowCount,
          tablet.getRowSize(),
          droppedAllNullRowCount,
          nullFieldCount,
          deletedFieldCount,
          nonNullFieldCount,
          firstInputTime == Long.MIN_VALUE ? "null" : String.valueOf(firstInputTime),
          lastInputTime == Long.MIN_VALUE ? "null" : String.valueOf(lastInputTime),
          firstOutputTime == Long.MIN_VALUE ? "null" : String.valueOf(firstOutputTime),
          lastOutputTime == Long.MIN_VALUE ? "null" : String.valueOf(lastOutputTime),
          PipeDataLossDebugUtil.formatTablet(tablet));
    }
    return tablet;
  }
}
