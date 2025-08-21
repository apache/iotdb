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

package org.apache.iotdb.it.utils;

import org.apache.iotdb.db.storageengine.dataregion.modification.DeletionPredicate;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModificationFile;
import org.apache.iotdb.db.storageengine.dataregion.modification.TableDeletionEntry;

import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.write.TsFileWriter;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeSet;
import java.util.stream.Collectors;

public class TsFileTableGenerator implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(TsFileTableGenerator.class);

  private final File tsFile;
  private final TsFileWriter writer;
  private final Map<String, TreeSet<Long>> table2TimeSet;
  private final Map<String, List<IMeasurementSchema>> table2MeasurementSchema;
  private final Map<String, List<ColumnCategory>> table2ColumnCategory;
  private Random random;

  public TsFileTableGenerator(final File tsFile) throws IOException {
    this.tsFile = tsFile;
    this.writer = new TsFileWriter(tsFile);
    this.table2TimeSet = new HashMap<>();
    this.table2MeasurementSchema = new HashMap<>();
    this.table2ColumnCategory = new HashMap<>();
    this.random = new Random();
  }

  public void registerTable(
      final String tableName,
      final List<IMeasurementSchema> columnSchemasList,
      final List<ColumnCategory> columnCategoryList) {
    if (table2MeasurementSchema.containsKey(tableName)) {
      LOGGER.warn("Table {} already exists", tableName);
      return;
    }

    writer.registerTableSchema(new TableSchema(tableName, columnSchemasList, columnCategoryList));
    table2TimeSet.put(tableName, new TreeSet<>());
    table2MeasurementSchema.put(tableName, columnSchemasList);
    table2ColumnCategory.put(tableName, columnCategoryList);
  }

  public void generateData(final String tableName, final int number, final long timeGap)
      throws IOException, WriteProcessException {
    final List<IMeasurementSchema> schemas = table2MeasurementSchema.get(tableName);
    final List<String> columnNameList =
        schemas.stream().map(IMeasurementSchema::getMeasurementName).collect(Collectors.toList());
    final List<TSDataType> dataTypeList =
        schemas.stream().map(IMeasurementSchema::getType).collect(Collectors.toList());
    final List<ColumnCategory> columnCategoryList = table2ColumnCategory.get(tableName);
    final TreeSet<Long> timeSet = table2TimeSet.get(tableName);
    final Tablet tablet = new Tablet(tableName, columnNameList, dataTypeList, columnCategoryList);
    final Object[] values = tablet.getValues();
    final long sensorNum = schemas.size();
    long startTime = timeSet.isEmpty() ? 0L : timeSet.last();

    for (long r = 0; r < number; r++) {
      final int row = tablet.getRowSize();
      startTime += timeGap;
      tablet.addTimestamp(row, startTime);
      timeSet.add(startTime);
      for (int i = 0; i < sensorNum; i++) {
        generateDataPoint(tablet, i, row, schemas.get(i));
      }
      // write
      if (tablet.getRowSize() == tablet.getMaxRowNumber()) {
        writer.writeTable(tablet);
        tablet.reset();
      }
    }
    // write
    if (tablet.getRowSize() != 0) {
      writer.writeTable(tablet);
      tablet.reset();
    }

    LOGGER.info("Write {} points into table {}", number, tableName);
  }

  private void generateDataPoint(
      final Tablet tablet, final int column, final int row, final IMeasurementSchema schema) {
    switch (schema.getType()) {
      case INT32:
        generateINT32(tablet, column, row);
        break;
      case DATE:
        generateDATE(tablet, column, row);
        break;
      case INT64:
      case TIMESTAMP:
        generateINT64(tablet, column, row);
        break;
      case FLOAT:
        generateFLOAT(tablet, column, row);
        break;
      case DOUBLE:
        generateDOUBLE(tablet, column, row);
        break;
      case BOOLEAN:
        generateBOOLEAN(tablet, column, row);
        break;
      case TEXT:
      case BLOB:
      case STRING:
        generateTEXT(tablet, column, row);
        break;
      default:
        LOGGER.error("Wrong data type {}.", schema.getType());
    }
  }

  private void generateINT32(final Tablet tablet, final int column, final int row) {
    tablet.addValue(row, column, random.nextInt());
  }

  private void generateDATE(final Tablet tablet, final int column, final int row) {
    tablet.addValue(
        row,
        column,
        LocalDate.of(1000 + random.nextInt(9000), 1 + random.nextInt(12), 1 + random.nextInt(28)));
  }

  private void generateINT64(final Tablet tablet, final int column, final int row) {
    tablet.addValue(row, column, random.nextLong());
  }

  private void generateFLOAT(final Tablet tablet, final int column, final int row) {
    tablet.addValue(row, column, random.nextFloat());
  }

  private void generateDOUBLE(final Tablet tablet, final int column, final int row) {
    tablet.addValue(row, column, random.nextDouble());
  }

  private void generateBOOLEAN(final Tablet tablet, final int column, final int row) {
    tablet.addValue(row, column, random.nextBoolean());
  }

  private void generateTEXT(final Tablet tablet, final int column, final int row) {
    tablet.addValue(row, column, String.format("test point %d", random.nextInt()));
  }

  public void generateDeletion(final String table) throws IOException {
    try (final ModificationFile modificationFile =
        new ModificationFile(ModificationFile.getExclusiveMods(tsFile), false)) {
      modificationFile.write(
          new TableDeletionEntry(
              new DeletionPredicate(table), new TimeRange(Long.MIN_VALUE, Long.MAX_VALUE)));
    }
  }

  @Override
  public void close() throws Exception {
    writer.close();
  }
}
