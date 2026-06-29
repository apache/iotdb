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

import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.commons.udf.utils.UDFDataTypeTransformer;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.i18n.DataNodeQueryMessages;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
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

import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.write.schema.IMeasurementSchema;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** Reads one or more TsFiles as a table function source. */
public class ReadTsFileTableFunction implements TableFunction {
  private static final String TABLE_NAME_PARAMETER_NAME = "TABLE_NAME";
  private static final String PATHS_PARAMETER_NAME = "PATHS";

  private MPPQueryContext mppQueryContext;

  public void setMPPQueryContext(MPPQueryContext mppQueryContext) {
    this.mppQueryContext = mppQueryContext;
  }

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
    TsFileSchemaCollector schemaCollector =
        new TsFileSchemaCollector(tableName.isEmpty() ? null : tableName, mppQueryContext);
    schemaCollector.collect(tsFilePaths);
    TableSchema mergedTableSchema = schemaCollector.getMergedTableSchema();
    if (mergedTableSchema == null) {
      throw new UDFArgumentNotValidException(
          tableName.isEmpty()
              ? DataNodeQueryMessages.NO_TABLE_SCHEMA_FOUND_IN_TSFILES
              : String.format(
                  DataNodeQueryMessages.NO_TABLE_SCHEMA_FOUND_FOR_TABLE_IN_TSFILES, tableName));
    }
    DescribedSchema outputSchema = convertToDescribedSchema(mergedTableSchema);

    ReadTsFileTableFunctionHandle handle =
        new ReadTsFileTableFunctionHandle(
            schemaCollector.getTableName(),
            schemaCollector.getTsFiles().stream()
                .map(File::getAbsolutePath)
                .collect(Collectors.toList()),
            mergedTableSchema.getColumnTypes().stream()
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
        DataNodeQueryMessages.READ_TSFILE_MUST_BE_PLANNED_AS_EXTERNAL_TSFILE_SCAN_NODE);
  }

  private static String getRequiredStringArgument(Map<String, Argument> arguments, String name) {
    Argument argument = arguments.get(name);
    if (!(argument instanceof ScalarArgument)) {
      throw new UDFArgumentNotValidException(DataNodeQueryMessages.MISSING_SCALAR_ARGUMENT + name);
    }
    Object value = ((ScalarArgument) argument).getValue();
    if (!(value instanceof String) || ((String) value).trim().isEmpty()) {
      throw new UDFArgumentNotValidException(
          String.format(DataNodeQueryMessages.ARGUMENT_SHOULD_NOT_BE_EMPTY, name));
    }
    return ((String) value).trim();
  }

  private static String getOptionalStringArgument(Map<String, Argument> arguments, String name) {
    Argument argument = arguments.get(name);
    if (argument == null) {
      return "";
    }
    if (!(argument instanceof ScalarArgument)) {
      throw new UDFArgumentNotValidException(DataNodeQueryMessages.INVALID_SCALAR_ARGUMENT + name);
    }
    Object value = ((ScalarArgument) argument).getValue();
    if (!(value instanceof String)) {
      throw new UDFArgumentNotValidException(
          String.format(DataNodeQueryMessages.ARGUMENT_SHOULD_BE_A_STRING, name));
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
          String.format(
              DataNodeQueryMessages.ARGUMENT_SHOULD_CONTAIN_AT_LEAST_ONE_PATH,
              PATHS_PARAMETER_NAME));
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
                  DataNodeQueryMessages.READ_TSFILE_PATH_IS_NOT_ALLOWED, tsFilePath, dataDir));
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
        throw new IllegalArgumentException(
            DataNodeQueryMessages.OUTPUT_COLUMN_NAMES_AND_TYPES_SIZE_MISMATCH);
      }
      if (outputColumnNames.size() != outputColumnCategories.size()) {
        throw new IllegalArgumentException(
            DataNodeQueryMessages.OUTPUT_COLUMN_NAMES_AND_CATEGORIES_SIZE_MISMATCH);
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
          DataNodeQueryMessages.READ_TSFILE_TABLE_FUNCTION_HANDLE_DOES_NOT_SUPPORT_SERIALIZATION);
    }

    @Override
    public void deserialize(byte[] bytes) {
      throw new UnsupportedOperationException(
          DataNodeQueryMessages.READ_TSFILE_TABLE_FUNCTION_HANDLE_DOES_NOT_SUPPORT_DESERIALIZATION);
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
