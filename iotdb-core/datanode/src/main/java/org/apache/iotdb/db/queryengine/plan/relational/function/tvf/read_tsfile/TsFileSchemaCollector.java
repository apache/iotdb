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

package org.apache.iotdb.db.queryengine.plan.relational.function.tvf.read_tsfile;

import org.apache.iotdb.db.i18n.DataNodeQueryMessages;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
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
  private final MPPQueryContext mppQueryContext;
  private String tableName;
  private final List<File> tsFiles = new ArrayList<>();
  private MergedTableSchemaBuilder schemaBuilder;
  private TableSchema mergedTableSchema;

  TsFileSchemaCollector(String specifiedTableName, MPPQueryContext mppQueryContext) {
    this.specifiedTableName =
        specifiedTableName == null ? null : specifiedTableName.toLowerCase(Locale.ENGLISH);
    this.mppQueryContext = mppQueryContext;
    this.tableName = this.specifiedTableName;
  }

  void collect(List<String> tsFilePaths) {
    for (String tsFilePath : tsFilePaths) {
      Path path = new File(tsFilePath).toPath();
      if (!Files.exists(path)) {
        throw new UDFArgumentNotValidException(
            DataNodeQueryMessages.TSFILE_PATH_DOES_NOT_EXIST + tsFilePath);
      }
      if (Files.isRegularFile(path)) {
        checkTimeOutIfNeeded();
        TableSchema tableSchema = readTableSchema(specifiedTableName, path.toFile(), true);
        collect(path.toFile(), tableSchema);
        continue;
      }
      if (!Files.isDirectory(path)) {
        throw new UDFArgumentNotValidException(
            DataNodeQueryMessages.TSFILE_PATH_IS_NEITHER_A_FILE_NOR_A_DIRECTORY + tsFilePath);
      }
      collectFromDirectory(tsFilePath, path);
    }
    if (tsFiles.isEmpty()) {
      throw new UDFArgumentNotValidException(DataNodeQueryMessages.NO_VALID_TSFILES_FOUND);
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
        checkTimeOutIfNeeded();
        TableSchema tableSchema = readTableSchema(specifiedTableName, filePath.toFile(), false);
        collect(filePath.toFile(), tableSchema);
      }
    } catch (IOException e) {
      throw new UDFArgumentNotValidException(
          DataNodeQueryMessages.FAILED_TO_SCAN_TSFILE_PATH + tsFilePath);
    }
  }

  private boolean hasTsFileSuffix(Path filePath) {
    Path fileName = filePath.getFileName();
    return fileName != null && fileName.toString().endsWith(TsFileConstant.TSFILE_SUFFIX);
  }

  private void checkTimeOutIfNeeded() {
    if (mppQueryContext != null) {
      mppQueryContext.checkTimeOut();
    }
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
              DataNodeQueryMessages.CANNOT_INFER_TABLE_NAME_FROM_TSFILES_MULTIPLE_TABLES,
              tableName,
              currentTableName));
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
      return readTableSchemaFromValidTsFile(specifiedTableName, tsFile, reader);
    } catch (UDFArgumentNotValidException e) {
      throw e;
    } catch (Exception e) {
      if (failOnInvalidTsFile) {
        throw invalidTsFileException(tsFile);
      }
      return null;
    }
  }

  private TableSchema readTableSchemaFromValidTsFile(
      String specifiedTableName, File tsFile, TsFileSequenceReader reader) {
    try {
      return selectTableSchema(specifiedTableName, tsFile, reader.getTableSchemaMap());
    } catch (UDFArgumentNotValidException e) {
      throw e;
    } catch (Exception e) {
      throw failedToReadTableSchemaException(tsFile);
    }
  }

  private TableSchema selectTableSchema(
      String specifiedTableName, File tsFile, Map<String, TableSchema> tableSchemaMap) {
    try {
      if (specifiedTableName != null) {
        return tableSchemaMap.get(specifiedTableName.toLowerCase(Locale.ENGLISH));
      }
      if (tableSchemaMap.isEmpty()) {
        throw new UDFArgumentNotValidException(
            DataNodeQueryMessages.CANNOT_INFER_TABLE_NAME_FROM_TSFILE_NO_TABLE_SCHEMA
                + tsFile.getAbsolutePath());
      }
      if (tableSchemaMap.size() > 1) {
        throw new UDFArgumentNotValidException(
            DataNodeQueryMessages.CANNOT_INFER_TABLE_NAME_FROM_TSFILE_MULTIPLE_TABLES
                + tsFile.getAbsolutePath());
      }
      return tableSchemaMap.values().iterator().next();
    } catch (UDFArgumentNotValidException e) {
      throw e;
    } catch (Exception e) {
      throw failedToReadTableSchemaException(tsFile);
    }
  }

  private boolean isValidTsFile(TsFileSequenceReader reader) throws IOException {
    return TSFileConfig.MAGIC_STRING.equals(reader.readHeadMagic())
        && reader.readVersionNumber() == TSFileConfig.VERSION_NUMBER
        && TSFileConfig.MAGIC_STRING.equals(reader.readTailMagic());
  }

  private UDFArgumentNotValidException invalidTsFileException(File tsFile) {
    return new UDFArgumentNotValidException(
        DataNodeQueryMessages.FILE_IS_NOT_A_VALID_TSFILE + tsFile.getAbsolutePath());
  }

  private UDFArgumentNotValidException failedToReadTableSchemaException(File tsFile) {
    return new UDFArgumentNotValidException(
        DataNodeQueryMessages.FAILED_TO_READ_TABLE_SCHEMA_FROM_TSFILE + tsFile.getAbsolutePath());
  }

  private static class MergedTableSchemaBuilder {
    private final String tableName;
    private IMeasurementSchema timeColumnSchema;
    private final List<IMeasurementSchema> tagColumnSchemas = new ArrayList<>();
    private final Map<String, IMeasurementSchema> fieldColumnSchemaMap = new LinkedHashMap<>();
    private final Map<String, ColumnCategory> columnCategoryMap = new LinkedHashMap<>();

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
        ColumnCategory currentCategory = columnCategories.get(i);
        if (currentCategory == ColumnCategory.TIME) {
          if (currentTimeColumn != null) {
            throw new UDFArgumentNotValidException(
                DataNodeQueryMessages.MULTIPLE_TIME_COLUMNS_FOUND_WHEN_MERGING_TABLE_SCHEMA
                    + tableName);
          }
          currentTimeColumn = columnSchemas.get(i);
        } else if (currentCategory == ColumnCategory.TAG) {
          checkAndRecordColumnCategory(columnSchemas.get(i), currentCategory);
          currentTagColumns.add(columnSchemas.get(i));
        } else if (currentCategory == ColumnCategory.FIELD) {
          checkAndRecordColumnCategory(columnSchemas.get(i), currentCategory);
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
            DataNodeQueryMessages.TIME_COLUMN_CONFLICTS_WHEN_MERGING_TABLE_SCHEMA + tableName);
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
              DataNodeQueryMessages.TAG_COLUMNS_CONFLICT_WHEN_MERGING_TABLE_SCHEMA + tableName);
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
              String.format(
                  DataNodeQueryMessages
                      .FIELD_COLUMN_HAS_CONFLICTING_DATA_TYPES_WHEN_MERGING_TABLE_SCHEMA,
                  fieldColumn.getMeasurementName(),
                  tableName));
        }
        fieldColumnSchemaMap.putIfAbsent(fieldName, fieldColumn);
      }
    }

    private void checkAndRecordColumnCategory(
        IMeasurementSchema columnSchema, ColumnCategory currentCategory) {
      String columnName = columnSchema.getMeasurementName().toLowerCase(Locale.ENGLISH);
      ColumnCategory existingCategory = columnCategoryMap.get(columnName);
      if (existingCategory != null && existingCategory != currentCategory) {
        throw new UDFArgumentNotValidException(
            String.format(
                DataNodeQueryMessages.COLUMN_HAS_CONFLICTING_CATEGORIES_WHEN_MERGING_TABLE_SCHEMA,
                columnSchema.getMeasurementName(),
                tableName));
      }
      columnCategoryMap.putIfAbsent(columnName, currentCategory);
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
