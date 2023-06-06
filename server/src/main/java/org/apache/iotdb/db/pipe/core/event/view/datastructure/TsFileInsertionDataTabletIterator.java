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

package org.apache.iotdb.db.pipe.core.event.view.datastructure;

import org.apache.iotdb.pipe.api.exception.PipeException;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.TsFileReader;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.expression.QueryExpression;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;

public class TsFileInsertionDataTabletIterator implements Iterator<Tablet> {

  private final TsFileReader tsFileReader;
  private final Map<String, TSDataType> measurementDataTypeMap;

  private final String deviceId;
  private final List<String> measurements;

  private final QueryDataSet queryDataSet;

  public TsFileInsertionDataTabletIterator(
      TsFileReader tsFileReader,
      Map<String, TSDataType> measurementDataTypeMap,
      String deviceId,
      List<String> measurements)
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

    this.queryDataSet = buildQueryDataSet();
  }

  private QueryDataSet buildQueryDataSet() throws IOException {
    final List<Path> paths = new ArrayList<>();
    for (String measurement : measurements) {
      paths.add(new Path(deviceId, measurement, false));
    }
    return tsFileReader.query(QueryExpression.create(paths, null));
  }

  @Override
  public boolean hasNext() {
    try {
      return queryDataSet.hasNext();
    } catch (IOException e) {
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
    } catch (IOException e) {
      throw new PipeException("Failed to build tablet", e);
    }
  }

  private Tablet buildNextTablet() throws IOException {
    final List<MeasurementSchema> schemas = new ArrayList<>();
    for (final String measurement : measurements) {
      final TSDataType dataType =
          measurementDataTypeMap.get(deviceId + TsFileConstant.PATH_SEPARATOR + measurement);
      schemas.add(new MeasurementSchema(measurement, dataType));
    }
    final Tablet tablet = new Tablet(deviceId, schemas);
    tablet.initBitMaps();

    while (queryDataSet.hasNext()) {
      final RowRecord rowRecord = queryDataSet.next();

      final int rowIndex = tablet.rowSize;

      tablet.addTimestamp(rowIndex, rowRecord.getTimestamp());

      final List<Field> fields = rowRecord.getFields();
      final int fieldSize = fields.size();
      for (int i = 0; i < fieldSize; i++) {
        final Field field = fields.get(i);
        if (field == null || field.getDataType() == null) {
          tablet.bitMaps[i].mark(rowIndex);
        } else {
          tablet.addValue(measurements.get(i), rowIndex, field.getObjectValue(field.getDataType()));
        }
      }

      tablet.rowSize++;

      if (tablet.rowSize == tablet.getMaxRowNumber()) {
        break;
      }
    }

    return tablet;
  }
}
