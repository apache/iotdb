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

package org.apache.iotdb.db.queryengine.plan.relational.function.tvf.readTsFile;

import org.apache.iotdb.udf.api.exception.UDFArgumentNotValidException;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.common.constant.TsFileConstant;
import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Stream;

import static org.apache.iotdb.commons.schema.table.TsTable.TIME_COLUMN_NAME;

final class TsFileSchemaCollector {
  private final String specifiedTableName;
  private String tableName;
  private final List<File> tsFiles = new ArrayList<>();
  private MergedTableSchemaBuilder schemaBuilder;
  private TableSchema mergedTableSchema;

  TsFileSchemaCollector(String specifiedTableName) {
    this.specifiedTableName =
        specifiedTableName == null ? null : specifiedTableName.toLowerCase(Locale.ENGLISH);
    this.tableName = this.specifiedTableName;
  }

  void collect(List<String> tsFilePaths) {
    for (String tsFilePath : tsFilePaths) {
      Path path = new File(tsFilePath).toPath();
      if (!Files.exists(path)) {
        throw new UDFArgumentNotValidException("TsFile path does not exist: " + tsFilePath);
      }
      if (Files.isRegularFile(path)) {
        TableSchema tableSchema = readTableSchema(specifiedTableName, path.toFile(), true);
        collect(path.toFile(), tableSchema);
        continue;
      }
      if (!Files.isDirectory(path)) {
        throw new UDFArgumentNotValidException(
            "TsFile path is neither a file nor a directory: " + tsFilePath);
      }
      collectFromDirectory(tsFilePath, path);
    }
    if (tsFiles.isEmpty()) {
      throw new UDFArgumentNotValidException("No valid TsFiles found");
    }
  }

  String getTableName() {
    return tableName;
  }

  List<File> getTsFiles() {
    return tsFiles;
  }

  TableSchema getMergedTableSchema() {
    return mergedTableSchema;
  }

  private void collectFromDirectory(String tsFilePath, Path path) {
    try (Stream<Path> walkedPaths = Files.walk(path)) {
      Iterator<Path> iterator =
          walkedPaths.filter(Files::isRegularFile).filter(this::hasTsFileSuffix).iterator();
      while (iterator.hasNext()) {
        Path filePath = iterator.next();
        TableSchema tableSchema = readTableSchema(specifiedTableName, filePath.toFile(), false);
        collect(filePath.toFile(), tableSchema);
      }
    } catch (IOException e) {
      throw new UDFArgumentNotValidException("Failed to scan TsFile path: " + tsFilePath);
    }
  }

  private boolean hasTsFileSuffix(Path filePath) {
    Path fileName = filePath.getFileName();
    return fileName != null && fileName.toString().endsWith(TsFileConstant.TSFILE_SUFFIX);
  }

  private void collect(File tsFile, TableSchema tableSchema) {
    if (tableSchema == null) {
      return;
    }
    String currentTableName = tableSchema.getTableName().toLowerCase(Locale.ENGLISH);
    if (tableName == null) {
      tableName = currentTableName;
    } else if (!tableName.equals(currentTableName)) {
      throw new UDFArgumentNotValidException(
          String.format(
              "Cannot infer table name from TsFiles because multiple tables are found: %s and %s",
              tableName, currentTableName));
    }
    tsFiles.add(tsFile);
    if (schemaBuilder == null) {
      schemaBuilder = new MergedTableSchemaBuilder(tableName, tableSchema);
    } else {
      schemaBuilder.merge(tableSchema);
    }
    mergedTableSchema = schemaBuilder.build();
  }

  private TableSchema readTableSchema(
      String specifiedTableName, File tsFile, boolean failOnInvalidTsFile) {
    if (!tsFile.canRead()) {
      if (failOnInvalidTsFile) {
        throw invalidTsFileException(tsFile);
      }
      return null;
    }
    try (TsFileSequenceReader reader = new TsFileSequenceReader(tsFile.getAbsolutePath())) {
      if (!isValidTsFile(reader)) {
        if (failOnInvalidTsFile) {
          throw invalidTsFileException(tsFile);
        }
        return null;
      }
      Map<String, TableSchema> tableSchemaMap = reader.getTableSchemaMap();
      if (specifiedTableName != null) {
        return tableSchemaMap.get(specifiedTableName.toLowerCase(Locale.ENGLISH));
      }
      if (tableSchemaMap.isEmpty()) {
        throw new UDFArgumentNotValidException(
            "Cannot infer table name from TsFile because no table schema is found in "
                + tsFile.getAbsolutePath());
      }
      if (tableSchemaMap.size() > 1) {
        throw new UDFArgumentNotValidException(
            "Cannot infer table name from TsFile because multiple tables are found in "
                + tsFile.getAbsolutePath());
      }
      return tableSchemaMap.values().iterator().next();
    } catch (UDFArgumentNotValidException e) {
      throw e;
    } catch (Exception e) {
      if (failOnInvalidTsFile) {
        throw invalidTsFileException(tsFile);
      }
      return null;
    }
  }

  private boolean isValidTsFile(TsFileSequenceReader reader) throws IOException {
    return TSFileConfig.MAGIC_STRING.equals(reader.readHeadMagic())
        && reader.readVersionNumber() == TSFileConfig.VERSION_NUMBER
        && TSFileConfig.MAGIC_STRING.equals(reader.readTailMagic());
  }

  private UDFArgumentNotValidException invalidTsFileException(File tsFile) {
    return new UDFArgumentNotValidException(
        "File is not a valid TsFile: " + tsFile.getAbsolutePath());
  }

  private static class MergedTableSchemaBuilder {
    private final String tableName;
    private IMeasurementSchema timeColumnSchema;
    private final List<IMeasurementSchema> tagColumnSchemas = new ArrayList<>();
    private final Map<String, IMeasurementSchema> fieldColumnSchemaMap = new LinkedHashMap<>();

    private MergedTableSchemaBuilder(String tableName, TableSchema tableSchema) {
      this.tableName = tableName.toLowerCase(Locale.ENGLISH);
      merge(tableSchema);
    }

    private void merge(TableSchema tableSchema) {
      IMeasurementSchema currentTimeColumn = null;
      List<IMeasurementSchema> currentTagColumns = new ArrayList<>();
      List<IMeasurementSchema> currentFieldColumns = new ArrayList<>();
      List<IMeasurementSchema> columnSchemas = tableSchema.getColumnSchemas();
      List<ColumnCategory> columnCategories = tableSchema.getColumnTypes();

      for (int i = 0; i < columnCategories.size(); i++) {
        if (columnCategories.get(i) == ColumnCategory.TIME) {
          if (currentTimeColumn != null) {
            throw new UDFArgumentNotValidException(
                "Multiple time columns found when merging table schema for table " + tableName);
          }
          currentTimeColumn = columnSchemas.get(i);
        } else if (columnCategories.get(i) == ColumnCategory.TAG) {
          currentTagColumns.add(columnSchemas.get(i));
        } else if (columnCategories.get(i) == ColumnCategory.FIELD) {
          currentFieldColumns.add(columnSchemas.get(i));
        }
      }

      mergeTimeColumn(currentTimeColumn);
      mergeTagColumns(currentTagColumns);
      mergeFieldColumns(currentFieldColumns);
    }

    private void mergeTimeColumn(IMeasurementSchema currentTimeColumn) {
      if (currentTimeColumn == null) {
        return;
      }
      if (timeColumnSchema == null) {
        timeColumnSchema = currentTimeColumn;
        return;
      }
      if (!timeColumnSchema.getMeasurementName().equals(currentTimeColumn.getMeasurementName())
          || currentTimeColumn.getType() != TSDataType.TIMESTAMP) {
        throw new UDFArgumentNotValidException(
            "Time column conflicts when merging table schema for table " + tableName);
      }
    }

    private void mergeTagColumns(List<IMeasurementSchema> currentTagColumns) {
      int prefixLength = Math.min(tagColumnSchemas.size(), currentTagColumns.size());
      for (int i = 0; i < prefixLength; i++) {
        if (!tagColumnSchemas
            .get(i)
            .getMeasurementName()
            .equals(currentTagColumns.get(i).getMeasurementName())) {
          throw new UDFArgumentNotValidException(
              "Tag columns conflict when merging table schema for table " + tableName);
        }
      }
      tagColumnSchemas.addAll(currentTagColumns.subList(prefixLength, currentTagColumns.size()));
    }

    private void mergeFieldColumns(List<IMeasurementSchema> currentFieldColumns) {
      for (IMeasurementSchema fieldColumn : currentFieldColumns) {
        String fieldName = fieldColumn.getMeasurementName().toLowerCase(Locale.ENGLISH);
        IMeasurementSchema existingColumn = fieldColumnSchemaMap.get(fieldName);
        if (existingColumn != null
            && !existingColumn.getType().isCompatible(fieldColumn.getType())) {
          throw new UDFArgumentNotValidException(
              "Field column "
                  + fieldColumn.getMeasurementName()
                  + " has conflicting data types when merging table schema for table "
                  + tableName);
        }
        fieldColumnSchemaMap.putIfAbsent(fieldName, fieldColumn);
      }
    }

    private TableSchema build() {
      List<IMeasurementSchema> columnSchemas = new ArrayList<>();
      List<ColumnCategory> columnCategories = new ArrayList<>();
      columnSchemas.add(
          timeColumnSchema == null
              ? new MeasurementSchema(TIME_COLUMN_NAME, TSDataType.TIMESTAMP)
              : timeColumnSchema);
      columnCategories.add(ColumnCategory.TIME);
      for (IMeasurementSchema tagColumnSchema : tagColumnSchemas) {
        columnSchemas.add(tagColumnSchema);
        columnCategories.add(ColumnCategory.TAG);
      }
      for (IMeasurementSchema fieldColumnSchema : fieldColumnSchemaMap.values()) {
        columnSchemas.add(fieldColumnSchema);
        columnCategories.add(ColumnCategory.FIELD);
      }
      return new TableSchema(tableName, columnSchemas, columnCategories);
    }
  }
}
