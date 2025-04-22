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

package org.apache.iotdb.commons.udf.builtin.relational.tvf;

import org.apache.iotdb.udf.api.exception.UDFException;
import org.apache.iotdb.udf.api.relational.TableFunction;
import org.apache.iotdb.udf.api.relational.access.Record;
import org.apache.iotdb.udf.api.relational.table.TableFunctionAnalysis;
import org.apache.iotdb.udf.api.relational.table.TableFunctionProcessorProvider;
import org.apache.iotdb.udf.api.relational.table.argument.Argument;
import org.apache.iotdb.udf.api.relational.table.argument.DescribedSchema;
import org.apache.iotdb.udf.api.relational.table.argument.ScalarArgument;
import org.apache.iotdb.udf.api.relational.table.argument.TableArgument;
import org.apache.iotdb.udf.api.relational.table.processor.TableFunctionDataProcessor;
import org.apache.iotdb.udf.api.relational.table.specification.ParameterSpecification;
import org.apache.iotdb.udf.api.relational.table.specification.ScalarParameterSpecification;
import org.apache.iotdb.udf.api.relational.table.specification.TableParameterSpecification;
import org.apache.iotdb.udf.api.type.Type;

import org.apache.tsfile.block.column.ColumnBuilder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.iotdb.commons.udf.builtin.relational.tvf.WindowTVFUtils.findColumnIndex;

public class ForecastTableFunction implements TableFunction {

  private static final String INPUT_PARAMETER_NAME = "INPUT";
  private static final String MODEL_ID_PARAMETER_NAME = "MODEL_ID";
  private static final String INPUT_LENGTH_PARAMETER_NAME = "INPUT_LENGTH";
  private static final String OUTPUT_LENGTH_PARAMETER_NAME = "OUTPUT_LENGTH";
  private static final Long DEFAULT_OUTPUT_LENGTH = 96L;
  private static final String PREDICATED_COLUMNS_PARAMETER_NAME = "PREDICATED_COLUMNS";
  private static final String DEFAULT_PREDICATED_COLUMNS = "";
  private static final String TIMECOL_PARAMETER_NAME = "TIMECOL";
  private static final String DEFAULT_TIME_COL = "time";
  private static final String KEEP_INPUT_PARAMETER_NAME = "KEEP_INPUT";
  private static final Boolean DEFAULT_KEEP_INPUT = Boolean.FALSE;
  private static final String OPTIONS_PARAMETER_NAME = "OPTIONS";
  private static final String DEFAULT_OPTIONS = "";

  private static final Set<Type> ALLOWED_INPUT_TYPES = new HashSet<>();

  static {
    ALLOWED_INPUT_TYPES.add(Type.INT32);
    ALLOWED_INPUT_TYPES.add(Type.INT64);
    ALLOWED_INPUT_TYPES.add(Type.FLOAT);
    ALLOWED_INPUT_TYPES.add(Type.DOUBLE);
  }

  @Override
  public List<ParameterSpecification> getArgumentsSpecifications() {
    return Arrays.asList(
        TableParameterSpecification.builder().name(INPUT_PARAMETER_NAME).setSemantics().build(),
        ScalarParameterSpecification.builder()
            .name(MODEL_ID_PARAMETER_NAME)
            .type(Type.STRING)
            .build(),
        ScalarParameterSpecification.builder()
            .name(INPUT_LENGTH_PARAMETER_NAME)
            .type(Type.INT32)
            .build(),
        ScalarParameterSpecification.builder()
            .name(OUTPUT_LENGTH_PARAMETER_NAME)
            .type(Type.INT32)
            .defaultValue(DEFAULT_OUTPUT_LENGTH)
            .build(),
        ScalarParameterSpecification.builder()
            .name(PREDICATED_COLUMNS_PARAMETER_NAME)
            .type(Type.STRING)
            .defaultValue(DEFAULT_PREDICATED_COLUMNS)
            .build(),
        ScalarParameterSpecification.builder()
            .name(TIMECOL_PARAMETER_NAME)
            .type(Type.STRING)
            .defaultValue(DEFAULT_TIME_COL)
            .build(),
        ScalarParameterSpecification.builder()
            .name(KEEP_INPUT_PARAMETER_NAME)
            .type(Type.BOOLEAN)
            .defaultValue(DEFAULT_KEEP_INPUT)
            .build(),
        ScalarParameterSpecification.builder()
            .name(OPTIONS_PARAMETER_NAME)
            .type(Type.STRING)
            .defaultValue(DEFAULT_OPTIONS)
            .build());
  }

  @Override
  public TableFunctionAnalysis analyze(Map<String, Argument> arguments) throws UDFException {
    TableArgument input = (TableArgument) arguments.get(INPUT_PARAMETER_NAME);
    String modelId = (String) ((ScalarArgument) arguments.get(MODEL_ID_PARAMETER_NAME)).getValue();
    // modelId should never be null or empty
    if (modelId == null || modelId.isEmpty()) {
      throw new UDFException(
          String.format("%s should never be null or empty", MODEL_ID_PARAMETER_NAME));
    }

    // make sure modelId exists
    if (!checkModelIdExist(modelId)) {
      throw new UDFException(
          String.format(
              "%s %s doesn't exist, you can use `show models` command to choose existing model.",
              MODEL_ID_PARAMETER_NAME, modelId));
    }

    int outputLength =
        (int) ((ScalarArgument) arguments.get(OUTPUT_LENGTH_PARAMETER_NAME)).getValue();
    if (outputLength <= 0) {
      throw new UDFException(
          String.format("%s should be greater than 0", OUTPUT_LENGTH_PARAMETER_NAME));
    }

    String predicatedColumns =
        (String) ((ScalarArgument) arguments.get(PREDICATED_COLUMNS_PARAMETER_NAME)).getValue();

    String timeColumn =
        (String) ((ScalarArgument) arguments.get(TIMECOL_PARAMETER_NAME)).getValue();

    // predicated columns should never contain partition by columns and time column
    Set<String> excludedColumns = new HashSet<>(input.getPartitionBy());
    excludedColumns.add(timeColumn);
    int timeColumnIndex = findColumnIndex(input, timeColumn, Collections.singleton(Type.TIMESTAMP));

    List<Integer> requiredIndexList = new ArrayList<>();
    requiredIndexList.add(timeColumnIndex);
    DescribedSchema.Builder properColumnSchemaBuilder =
        new DescribedSchema.Builder().addField(timeColumn, Type.TIMESTAMP);

    List<Optional<String>> allInputColumnsName = input.getFieldNames();
    List<Type> allInputColumnsType = input.getFieldTypes();
    if (predicatedColumns.isEmpty()) {
      // predicated columns by default include all columns from input table except for timecol and
      // partition by columns
      for (int i = 0, size = allInputColumnsName.size(); i < size; i++) {
        Optional<String> fieldName = allInputColumnsName.get(i);
        if (!fieldName.isPresent() || !excludedColumns.contains(fieldName.get())) {
          Type columnType = allInputColumnsType.get(i);
          checkType(columnType, fieldName.orElse(""));
          requiredIndexList.add(i);
          properColumnSchemaBuilder.addField(fieldName, columnType);
        }
      }
    } else {
      String[] predictedColumnsArray = predicatedColumns.split(",");
      Map<String, Integer> inputColumnIndexMap = new HashMap<>();
      for (int i = 0, size = allInputColumnsName.size(); i < size; i++) {
        Optional<String> fieldName = allInputColumnsName.get(i);
        if (!fieldName.isPresent()) {
          continue;
        }
        inputColumnIndexMap.put(fieldName.get(), i);
      }

      Set<Integer> requiredIndexSet = new HashSet<>(predictedColumnsArray.length);
      // columns need to be predicated
      for (String outputColumn : predictedColumnsArray) {
        if (excludedColumns.contains(outputColumn)) {
          throw new UDFException(
              String.format("%s is in partition by clause or is time column", outputColumn));
        }
        Integer inputColumnIndex = inputColumnIndexMap.get(outputColumn);
        if (inputColumnIndex == null) {
          throw new UDFException(String.format("Column %s don't exist in input", outputColumn));
        }
        if (!requiredIndexSet.add(inputColumnIndex)) {
          throw new UDFException(String.format("Duplicate column %s", outputColumn));
        }

        Type columnType = allInputColumnsType.get(inputColumnIndex);
        checkType(columnType, outputColumn);
        requiredIndexList.add(inputColumnIndex);
        properColumnSchemaBuilder.addField(outputColumn, columnType);
      }
    }

    // outputColumnSchema
    return TableFunctionAnalysis.builder()
        .properColumnSchema(properColumnSchemaBuilder.build())
        .requiredColumns(INPUT_PARAMETER_NAME, requiredIndexList)
        .build();
  }

  private boolean checkModelIdExist(String modelId) {
    return true;
  }

  // only allow for INT32, INT64, FLOAT, DOUBLE
  private void checkType(Type type, String columnName) {
    if (!ALLOWED_INPUT_TYPES.contains(type)) {
      throw new UDFException(
          String.format(
              "The type of the column [%s] is [%s], only INT32, INT64, FLOAT, DOUBLE is allowed",
              columnName, type));
    }
  }

  @Override
  public TableFunctionProcessorProvider getProcessorProvider(Map<String, Argument> arguments) {
    int inputLength =
        (int) ((ScalarArgument) arguments.get(INPUT_LENGTH_PARAMETER_NAME)).getValue();
    int outputLength =
        (int) ((ScalarArgument) arguments.get(OUTPUT_LENGTH_PARAMETER_NAME)).getValue();
    boolean keepInput =
        (boolean) ((ScalarArgument) arguments.get(KEEP_INPUT_PARAMETER_NAME)).getValue();
    String options = (String) ((ScalarArgument) arguments.get(OPTIONS_PARAMETER_NAME)).getValue();
    return new TableFunctionProcessorProvider() {
      @Override
      public TableFunctionDataProcessor getDataProcessor() {
        return new ForecastDataProcessor(inputLength, outputLength, keepInput, options);
      }
    };
  }

  private static class ForecastDataProcessor implements TableFunctionDataProcessor {

    private final int maxInputLength;
    private final int outputLength;
    private final boolean keepInput;
    private final String options;

    private final List<Record> inputRecords;

    public ForecastDataProcessor(
        int maxInputLength, int outputLength, boolean keepInput, String options) {
      this.maxInputLength = maxInputLength;
      this.outputLength = outputLength;
      this.keepInput = keepInput;
      this.options = options;
      this.inputRecords = new LinkedList<>();
    }

    @Override
    public void process(
        Record input,
        List<ColumnBuilder> properColumnBuilders,
        ColumnBuilder passThroughIndexBuilder) {
      inputRecords.add(input);
    }

    @Override
    public void finish(List<ColumnBuilder> columnBuilders, ColumnBuilder passThroughIndexBuilder) {
      TableFunctionDataProcessor.super.finish(columnBuilders, passThroughIndexBuilder);
    }
  }
}
