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

package org.apache.iotdb.db.pipe.event.common.tsfile.parser.table;

import org.apache.iotdb.pipe.api.exception.PipeException;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.read.ReadProcessException;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.read.common.Field;
import org.apache.tsfile.read.common.RowRecord;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.query.executor.TableQueryExecutor;
import org.apache.tsfile.read.reader.block.TsBlockReader;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;

public class TsFileInsertionEventTableParserTabletIterator implements Iterator<Tablet> {

  private final String tableName;

  private final long startTime;
  private final long endTime;

  private final List<IMeasurementSchema> columnSchemas;
  private final List<String> columnNames;
  private final TsBlockReader tsBlockReader;

  public TsFileInsertionEventTableParserTabletIterator(
      final TableQueryExecutor tableQueryExecutor,
      final String tableName,
      final TableSchema tableSchema,
      final long startTime,
      final long endTime) {
    this.tableName = tableName;
    this.startTime = startTime;
    this.endTime = endTime;

    try {
      columnSchemas =
          tableSchema.getColumnSchemas().stream()
              // time column in aligned time-series should not be a query column
              .filter(
                  schema ->
                      schema.getMeasurementId() != null && !schema.getMeasurementId().isEmpty())
              .collect(Collectors.toList());
      columnNames =
          columnSchemas.stream()
              .map(IMeasurementSchema::getMeasurementId)
              .collect(Collectors.toList());
      tsBlockReader = tableQueryExecutor.query(tableName, columnNames, null, null, null);
    } catch (final ReadProcessException e) {
      throw new PipeException("Failed to build query data set", e);
    }
  }

  @Override
  public boolean hasNext() {
    return tsBlockReader.hasNext();
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

  // TODO: memory control
  private Tablet buildNextTablet() throws IOException {
    final TsBlock tsBlock = tsBlockReader.next();

    final Tablet tablet = new Tablet(tableName, columnSchemas, tsBlock.getPositionCount());
    tablet.initBitMaps();

    final TsBlock.TsBlockRowIterator rowIterator = tsBlock.getTsBlockRowIterator();
    while (rowIterator.hasNext()) {
      final Object[] row = rowIterator.next();

      final long timestamp = (Long) row[row.length - 1];
      if (timestamp < startTime || timestamp > endTime) {
        continue;
      }

      final List<Field> fields = new ArrayList<>();
      for (int i = 0; i < row.length - 1; ++i) {
        final TSDataType dataType = columnSchemas.get(i).getType();
        if (dataType == null) {
          fields.add(null);
          continue;
        }
        if (row[i] == null) {
          fields.add(null);
          continue;
        }
        final Field field = new Field(dataType);
        fields.add(field);
        switch (dataType) {
          case BOOLEAN:
            field.setBoolV((Boolean) row[i]);
            break;
          case INT32:
          case DATE:
            field.setIntV((Integer) row[i]);
            break;
          case INT64:
          case TIMESTAMP:
            field.setLongV((Long) row[i]);
            break;
          case FLOAT:
            field.setFloatV((Float) row[i]);
            break;
          case DOUBLE:
            field.setDoubleV((Double) row[i]);
            break;
          case STRING:
          case BLOB:
          case TEXT:
            field.setBinaryV((Binary) row[i]);
            break;
          default:
            throw new UnsupportedOperationException("Unsupported data type: " + dataType);
        }
      }
      final RowRecord rowRecord = new RowRecord(timestamp, fields);

      final int rowIndex = tablet.rowSize;
      tablet.addTimestamp(rowIndex, rowRecord.getTimestamp());
      final int fieldSize = fields.size();
      for (int i = 0; i < fieldSize; i++) {
        final Field field = fields.get(i);
        tablet.addValue(
            columnNames.get(i),
            rowIndex,
            field == null ? null : field.getObjectValue(columnSchemas.get(i).getType()));
      }
      tablet.rowSize++;
      if (tablet.rowSize == tablet.getMaxRowNumber()) {
        break;
      }
    }

    return tablet;
  }
}
