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

import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.commons.udf.utils.UDFDataTypeTransformer;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.udf.api.exception.UDFArgumentNotValidException;
import org.apache.iotdb.udf.api.exception.UDFException;
import org.apache.iotdb.udf.api.relational.TableFunction;
import org.apache.iotdb.udf.api.relational.table.TableFunctionAnalysis;
import org.apache.iotdb.udf.api.relational.table.TableFunctionHandle;
import org.apache.iotdb.udf.api.relational.table.TableFunctionProcessorProvider;
import org.apache.iotdb.udf.api.relational.table.argument.Argument;
import org.apache.iotdb.udf.api.relational.table.argument.DescribedSchema;
import org.apache.iotdb.udf.api.relational.table.argument.ScalarArgument;
import org.apache.iotdb.udf.api.relational.table.specification.ParameterSpecification;
import org.apache.iotdb.udf.api.relational.table.specification.ScalarParameterSpecification;
import org.apache.iotdb.udf.api.type.Type;

import org.apache.tsfile.common.conf.TSFileConfig;
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
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.iotdb.commons.schema.table.TsTable.TIME_COLUMN_NAME;

/** Reads one or more TsFiles as a table function source. */
public class ReadTsFileTableFunction implements TableFunction {
  private static final String TABLE_NAME_PARAMETER_NAME = "TABLE_NAME";
  private static final String PATHS_PARAMETER_NAME = "PATHS";

  @Override
  public List<ParameterSpecification> getArgumentsSpecifications() {
    return Arrays.asList(
        ScalarParameterSpecification.builder().name(PATHS_PARAMETER_NAME).type(Type.STRING).build(),
        ScalarParameterSpecification.builder()
            .name(TABLE_NAME_PARAMETER_NAME)
            .type(Type.STRING)
            .defaultValue("")
            .build());
  }

  @Override
  public TableFunctionAnalysis analyze(Map<String, Argument> arguments) throws UDFException {
    String tableName = getOptionalStringArgument(arguments, TABLE_NAME_PARAMETER_NAME);
    List<String> tsFilePaths =
        parseTsFilePaths(getRequiredStringArgument(arguments, PATHS_PARAMETER_NAME));
    checkTsFilePathsAreOutsideDataDirs(tsFilePaths);
    TsFileSchemaCollection schemaCollection =
        collectTsFilesAndResolveSchema(tableName.isEmpty() ? null : tableName, tsFilePaths);
    if (schemaCollection.mergedTableSchema == null) {
      throw new UDFArgumentNotValidException(
          tableName.isEmpty()
              ? "No table schema found in TsFiles"
              : "No table schema found for table " + tableName + " in TsFiles");
    }
    DescribedSchema outputSchema = convertToDescribedSchema(schemaCollection.mergedTableSchema);

    ReadTsFileTableFunctionHandle handle =
        new ReadTsFileTableFunctionHandle(
            schemaCollection.tableName,
            schemaCollection.tsFiles.stream()
                .map(File::getAbsolutePath)
                .collect(Collectors.toList()),
            schemaCollection.mergedTableSchema.getColumnTypes().stream()
                .map(TsTableColumnCategory::fromTsFileColumnCategory)
                .collect(Collectors.toList()),
            outputSchema);

    return TableFunctionAnalysis.builder().properColumnSchema(outputSchema).handle(handle).build();
  }

  @Override
  public TableFunctionHandle createTableFunctionHandle() {
    return new ReadTsFileTableFunctionHandle();
  }

  @Override
  public TableFunctionProcessorProvider getProcessorProvider(
      TableFunctionHandle tableFunctionHandle) {
    throw new UnsupportedOperationException(
        "readTsFile must be planned as an ExternalTsFileScanNode");
  }

  private static String getRequiredStringArgument(Map<String, Argument> arguments, String name) {
    Argument argument = arguments.get(name);
    if (!(argument instanceof ScalarArgument)) {
      throw new UDFArgumentNotValidException("Missing scalar argument: " + name);
    }
    Object value = ((ScalarArgument) argument).getValue();
    if (!(value instanceof String) || ((String) value).trim().isEmpty()) {
      throw new UDFArgumentNotValidException("Argument " + name + " should not be empty");
    }
    return ((String) value).trim();
  }

  private static String getOptionalStringArgument(Map<String, Argument> arguments, String name) {
    Argument argument = arguments.get(name);
    if (argument == null) {
      return "";
    }
    if (!(argument instanceof ScalarArgument)) {
      throw new UDFArgumentNotValidException("Invalid scalar argument: " + name);
    }
    Object value = ((ScalarArgument) argument).getValue();
    if (!(value instanceof String)) {
      throw new UDFArgumentNotValidException("Argument " + name + " should be a string");
    }
    return ((String) value).trim();
  }

  private static List<String> parseTsFilePaths(String tsFilePaths) {
    List<String> paths =
        Arrays.stream(tsFilePaths.split(","))
            .map(String::trim)
            .filter(path -> !path.isEmpty())
            .collect(Collectors.toList());
    if (paths.isEmpty()) {
      throw new UDFArgumentNotValidException(
          "Argument " + PATHS_PARAMETER_NAME + " should contain at least one path");
    }
    return paths;
  }

  private static void checkTsFilePathsAreOutsideDataDirs(List<String> tsFilePaths) {
    List<Path> dataDirs =
        Arrays.stream(IoTDBDescriptor.getInstance().getConfig().getDataDirs())
            .map(ReadTsFileTableFunction::normalizePath)
            .collect(Collectors.toList());
    for (String tsFilePath : tsFilePaths) {
      Path normalizedTsFilePath = normalizePath(tsFilePath);
      for (Path dataDir : dataDirs) {
        if (normalizedTsFilePath.startsWith(dataDir) || dataDir.startsWith(normalizedTsFilePath)) {
          throw new UDFArgumentNotValidException(
              String.format(
                  "readTsFile path %s is not allowed because it may access IoTDB data directory %s",
                  tsFilePath, dataDir));
        }
      }
    }
  }

  private static Path normalizePath(String path) {
    Path normalizedPath = Paths.get(path).toAbsolutePath().normalize();
    try {
      return normalizedPath.toRealPath();
    } catch (IOException e) {
      return normalizedPath;
    }
  }

  private static TsFileSchemaCollection collectTsFilesAndResolveSchema(
      String specifiedTableName, List<String> tsFilePaths) {
    List<File> tsFiles = new ArrayList<>();
    MergedTableSchemaBuilder schemaBuilder = null;
    String resolvedTableName =
        specifiedTableName == null ? null : specifiedTableName.toLowerCase(Locale.ENGLISH);
    for (String tsFilePath : tsFilePaths) {
      Path path = new File(tsFilePath).toPath();
      if (!Files.exists(path)) {
        throw new UDFArgumentNotValidException("TsFile path does not exist: " + tsFilePath);
      }
      try (Stream<Path> walkedPaths = Files.walk(path)) {
        Iterator<Path> iterator = walkedPaths.filter(Files::isRegularFile).iterator();
        while (iterator.hasNext()) {
          Path filePath = iterator.next();
          TableSchema tableSchema =
              specifiedTableName == null
                  ? tryInferTableSchema(filePath.toFile())
                  : tryReadTableSchema(specifiedTableName, filePath.toFile());
          if (tableSchema == null) {
            continue;
          }
          String currentTableName = tableSchema.getTableName().toLowerCase(Locale.ENGLISH);
          if (resolvedTableName == null) {
            resolvedTableName = currentTableName;
          } else if (!resolvedTableName.equals(currentTableName)) {
            throw new UDFArgumentNotValidException(
                String.format(
                    "Cannot infer table name from TsFiles because multiple tables are found: %s and %s",
                    resolvedTableName, currentTableName));
          }
          tsFiles.add(filePath.toFile());
          if (schemaBuilder == null) {
            schemaBuilder = new MergedTableSchemaBuilder(resolvedTableName, tableSchema);
          } else {
            schemaBuilder.merge(tableSchema);
          }
        }
      } catch (IOException e) {
        throw new UDFArgumentNotValidException("Failed to scan TsFile path: " + tsFilePath);
      }
    }
    if (tsFiles.isEmpty()) {
      throw new UDFArgumentNotValidException("No valid TsFiles found");
    }
    return new TsFileSchemaCollection(resolvedTableName, tsFiles, schemaBuilder.build());
  }

  private static TableSchema tryReadTableSchema(String tableName, File tsFile) {
    if (!tsFile.canRead()) {
      return null;
    }
    try (TsFileSequenceReader reader = new TsFileSequenceReader(tsFile.getAbsolutePath())) {
      if (!reader.isComplete() || reader.readVersionNumber() != TSFileConfig.VERSION_NUMBER) {
        return null;
      }
      Map<String, TableSchema> tableSchemaMap = reader.getTableSchemaMap();
      return tableSchemaMap.get(tableName.toLowerCase(Locale.ENGLISH));
    } catch (Exception e) {
      return null;
    }
  }

  private static TableSchema tryInferTableSchema(File tsFile) {
    if (!tsFile.canRead()) {
      return null;
    }
    try (TsFileSequenceReader reader = new TsFileSequenceReader(tsFile.getAbsolutePath())) {
      if (!reader.isComplete() || reader.readVersionNumber() != TSFileConfig.VERSION_NUMBER) {
        return null;
      }
      Map<String, TableSchema> tableSchemaMap = reader.getTableSchemaMap();
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
      return null;
    }
  }

  private static class TsFileSchemaCollection {
    private final String tableName;
    private final List<File> tsFiles;
    private final TableSchema mergedTableSchema;

    private TsFileSchemaCollection(
        String tableName, List<File> tsFiles, TableSchema mergedTableSchema) {
      this.tableName = tableName;
      this.tsFiles = tsFiles;
      this.mergedTableSchema = mergedTableSchema;
    }
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
        if (existingColumn != null && existingColumn.getType() != fieldColumn.getType()) {
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

  private static DescribedSchema convertToDescribedSchema(TableSchema tableSchema) {
    DescribedSchema.Builder builder = DescribedSchema.builder();
    for (IMeasurementSchema columnSchema : tableSchema.getColumnSchemas()) {
      builder.addField(
          columnSchema.getMeasurementName(),
          UDFDataTypeTransformer.transformToUDFDataType(columnSchema.getType()));
    }
    return builder.build();
  }

  public static class ReadTsFileTableFunctionHandle implements TableFunctionHandle {
    private String tableName;
    private List<String> tsFilePaths;
    private List<String> outputColumnNames;
    private List<Type> outputColumnTypes;
    private List<TsTableColumnCategory> outputColumnCategories;

    public ReadTsFileTableFunctionHandle() {
      this(
          "",
          Collections.emptyList(),
          Collections.emptyList(),
          Collections.emptyList(),
          Collections.emptyList());
    }

    public ReadTsFileTableFunctionHandle(
        String tableName,
        List<String> tsFilePaths,
        List<TsTableColumnCategory> outputColumnCategories,
        DescribedSchema outputSchema) {
      this(
          tableName,
          tsFilePaths,
          outputSchema.getFields().stream()
              .map(field -> field.getName().orElse(""))
              .collect(Collectors.toList()),
          outputSchema.getFields().stream()
              .map(DescribedSchema.Field::getType)
              .collect(Collectors.toList()),
          outputColumnCategories);
    }

    private ReadTsFileTableFunctionHandle(
        String tableName,
        List<String> tsFilePaths,
        List<String> outputColumnNames,
        List<Type> outputColumnTypes,
        List<TsTableColumnCategory> outputColumnCategories) {
      if (outputColumnNames.size() != outputColumnTypes.size()) {
        throw new IllegalArgumentException("Output column names and types size mismatch");
      }
      if (outputColumnNames.size() != outputColumnCategories.size()) {
        throw new IllegalArgumentException("Output column names and categories size mismatch");
      }
      this.tableName = tableName;
      this.tsFilePaths = Collections.unmodifiableList(new ArrayList<>(tsFilePaths));
      this.outputColumnNames = Collections.unmodifiableList(new ArrayList<>(outputColumnNames));
      this.outputColumnTypes = Collections.unmodifiableList(new ArrayList<>(outputColumnTypes));
      this.outputColumnCategories =
          Collections.unmodifiableList(new ArrayList<>(outputColumnCategories));
    }

    public String getTableName() {
      return tableName;
    }

    public List<String> getTsFilePaths() {
      return tsFilePaths;
    }

    public List<String> getOutputColumnNames() {
      return outputColumnNames;
    }

    public List<Type> getOutputColumnTypes() {
      return outputColumnTypes;
    }

    public List<TsTableColumnCategory> getOutputColumnCategories() {
      return outputColumnCategories;
    }

    @Override
    public byte[] serialize() {
      throw new UnsupportedOperationException(
          "ReadTsFileTableFunctionHandle does not support serialization");
    }

    @Override
    public void deserialize(byte[] bytes) {
      throw new UnsupportedOperationException(
          "ReadTsFileTableFunctionHandle does not support deserialization");
    }

    @Override
    public String toString() {
      return "ReadTsFileTableFunctionHandle{"
          + "tableName='"
          + tableName
          + '\''
          + ", tsFilePaths="
          + tsFilePaths
          + ", outputColumnNames="
          + outputColumnNames
          + ", outputColumnTypes="
          + outputColumnTypes
          + ", outputColumnCategories="
          + outputColumnCategories
          + '}';
    }
  }
}
