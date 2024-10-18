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

import org.apache.tsfile.exception.read.ReadProcessException;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.query.executor.TableQueryExecutor;
import org.apache.tsfile.read.reader.block.TsBlockReader;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class TsFileInsertionEventTableParserTabletIterator implements Iterator<Tablet> {

  private final String tableName;

  private final long startTime;
  private final long endTime;

  private final List<IMeasurementSchema> columnSchemas;
  private final List<Tablet.ColumnType> columnTypes;
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

    columnSchemas = new ArrayList<>();
    columnTypes = new ArrayList<>();
    columnNames = new ArrayList<>();
    try {
      for (int i = 0, size = tableSchema.getColumnSchemas().size(); i < size; i++) {
        final IMeasurementSchema schema = tableSchema.getColumnSchemas().get(i);
        if (schema.getMeasurementId() != null && !schema.getMeasurementId().isEmpty()) {
          columnSchemas.add(schema);
          columnTypes.add(tableSchema.getColumnTypes().get(i));
          columnNames.add(schema.getMeasurementId());
        }
      }

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

    final Tablet tablet =
        new Tablet(tableName, columnSchemas, columnTypes, tsBlock.getPositionCount());
    tablet.initBitMaps();

    final TsBlock.TsBlockRowIterator rowIterator = tsBlock.getTsBlockRowIterator();
    while (rowIterator.hasNext()) {
      final Object[] row = rowIterator.next();

      final long timestamp = (Long) row[row.length - 1];
      if (timestamp < startTime || timestamp > endTime) {
        continue;
      }

      final int rowIndex = tablet.rowSize;
      tablet.addTimestamp(rowIndex, timestamp);
      for (int i = 0, fieldSize = row.length - 1; i < fieldSize; i++) {
        tablet.addValue(columnNames.get(i), rowIndex, row[i]);
      }
      tablet.rowSize++;
    }

    return tablet;
  }
}
