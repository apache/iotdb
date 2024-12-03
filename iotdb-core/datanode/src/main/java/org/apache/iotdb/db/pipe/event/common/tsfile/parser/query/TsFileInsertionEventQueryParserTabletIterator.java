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

import org.apache.iotdb.db.pipe.resource.PipeDataNodeResourceManager;
import org.apache.iotdb.db.pipe.resource.memory.PipeMemoryBlock;
import org.apache.iotdb.db.pipe.resource.memory.PipeMemoryWeightUtil;
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

  TsFileInsertionEventQueryParserTabletIterator(
      final TsFileReader tsFileReader,
      final Map<String, TSDataType> measurementDataTypeMap,
      final IDeviceID deviceId,
      final List<String> measurements,
      final IExpression timeFilterExpression,
      final PipeMemoryBlock allocatedBlockForTablet)
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
      // Ignore the memory cost of tablet
      PipeDataNodeResourceManager.memory().forceResize(allocatedBlockForTablet, 0);
      return tablet;
    }

    boolean isFirstRow = true;
    while (queryDataSet.hasNext()) {
      final RowRecord rowRecord = queryDataSet.next();
      if (isFirstRow) {
        // Calculate row count and memory size of the tablet based on the first row
        Pair<Integer, Integer> rowCountAndMemorySize =
            PipeMemoryWeightUtil.calculateTabletRowCountAndMemory(rowRecord);
        tablet =
            new Tablet(
                // Used for tree model
                deviceId.toString(), schemas, rowCountAndMemorySize.getLeft());
        tablet.initBitMaps();
        PipeDataNodeResourceManager.memory()
            .forceResize(allocatedBlockForTablet, rowCountAndMemorySize.getRight());
        isFirstRow = false;
      }

      final int rowIndex = tablet.getRowSize();

      tablet.addTimestamp(rowIndex, rowRecord.getTimestamp());

      final List<Field> fields = rowRecord.getFields();
      final int fieldSize = fields.size();
      for (int i = 0; i < fieldSize; i++) {
        final Field field = fields.get(i);
        tablet.addValue(
            measurements.get(i),
            rowIndex,
            field == null ? null : field.getObjectValue(schemas.get(i).getType()));
      }

      if (tablet.getRowSize() == tablet.getMaxRowNumber()) {
        break;
      }
    }

    return tablet;
  }
}
