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

package org.apache.iotdb.commons.queryengine.plan.relational.function.tvf;

import org.apache.iotdb.ainode.rpc.thrift.TClassifyReq;
import org.apache.iotdb.ainode.rpc.thrift.TClassifyResp;
import org.apache.iotdb.commons.exception.IoTDBRuntimeException;
import org.apache.iotdb.commons.exception.SemanticException;
import org.apache.iotdb.commons.i18n.QueryMessages;
import org.apache.iotdb.commons.queryengine.plan.relational.utils.ResultColumnAppender;
import org.apache.iotdb.commons.queryengine.plan.udf.TableUDFUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.udf.api.exception.UDFException;
import org.apache.iotdb.udf.api.relational.TableFunction;
import org.apache.iotdb.udf.api.relational.access.Record;
import org.apache.iotdb.udf.api.relational.table.TableFunctionAnalysis;
import org.apache.iotdb.udf.api.relational.table.TableFunctionHandle;
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

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.block.column.TsBlockSerde;
import org.apache.tsfile.utils.PublicBAOS;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.iotdb.commons.queryengine.plan.relational.function.tvf.TableFunctionUtils.checkType;
import static org.apache.iotdb.commons.queryengine.plan.relational.function.tvf.TableFunctionUtils.parseOptions;
import static org.apache.iotdb.commons.queryengine.plan.relational.utils.ResultColumnAppender.createResultColumnAppender;
import static org.apache.iotdb.commons.udf.builtin.relational.tvf.WindowTVFUtils.findColumnIndex;

public class ClassifyTableFunction implements TableFunction {

  public static class ClassifyTableFunctionHandle implements TableFunctionHandle {
    String modelId;
    int maxInputLength;
    Map<String, String> options;
    List<Type> inputColumnTypes;

    public ClassifyTableFunctionHandle() {}

    public ClassifyTableFunctionHandle(
        String modelId,
        int maxInputLength,
        Map<String, String> options,
        List<Type> inputColumnTypes) {
      this.modelId = modelId;
      this.maxInputLength = maxInputLength;
      this.options = options;
      this.inputColumnTypes = inputColumnTypes;
    }

    @Override
    public byte[] serialize() {
      try (PublicBAOS publicBAOS = new PublicBAOS();
          DataOutputStream outputStream = new DataOutputStream(publicBAOS)) {
        ReadWriteIOUtils.write(modelId, outputStream);
        ReadWriteIOUtils.write(maxInputLength, outputStream);
        ReadWriteIOUtils.write(options, outputStream);
        ReadWriteIOUtils.write(inputColumnTypes.size(), outputStream);
        for (Type type : inputColumnTypes) {
          ReadWriteIOUtils.write(type.getType(), outputStream);
        }
        outputStream.flush();
        return publicBAOS.toByteArray();
      } catch (IOException e) {
        throw new IoTDBRuntimeException(
            String.format(
                "Error occurred while serializing ClassifyTableFunctionHandle: %s", e.getMessage()),
            TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode());
      }
    }

    @Override
    public void deserialize(byte[] bytes) {
      ByteBuffer buffer = ByteBuffer.wrap(bytes);
      this.modelId = ReadWriteIOUtils.readString(buffer);
      this.maxInputLength = ReadWriteIOUtils.readInt(buffer);
      this.options = ReadWriteIOUtils.readMap(buffer);
      int size = ReadWriteIOUtils.readInt(buffer);
      this.inputColumnTypes = new ArrayList<>(size);
      for (int i = 0; i < size; i++) {
        inputColumnTypes.add(Type.valueOf(ReadWriteIOUtils.readString(buffer)));
      }
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      ClassifyTableFunctionHandle that = (ClassifyTableFunctionHandle) o;
      return maxInputLength == that.maxInputLength
          && Objects.equals(modelId, that.modelId)
          && Objects.equals(options, that.options)
          && Objects.equals(inputColumnTypes, that.inputColumnTypes);
    }

    @Override
    public int hashCode() {
      return Objects.hash(modelId, maxInputLength, options, inputColumnTypes);
    }
  }

  private static final String INPUT_PARAMETER_NAME = "INPUTS";
  private static final String MODEL_ID_PARAMETER_NAME = "MODEL_ID";
  public static final String TIMECOL_PARAMETER_NAME = "TIMECOL";
  private static final String DEFAULT_TIME_COL = "time";
  private static final String DEFAULT_OUTPUT_COLUMN_NAME = "category";
  protected static final String OPTIONS_PARAMETER_NAME = "MODEL_OPTIONS";
  protected static final String DEFAULT_OPTIONS = "";
  private static final int MAX_INPUT_LENGTH = 2880;

  @Override
  public List<ParameterSpecification> getArgumentsSpecifications() {
    return Arrays.asList(
        TableParameterSpecification.builder().name(INPUT_PARAMETER_NAME).setSemantics().build(),
        ScalarParameterSpecification.builder()
            .name(MODEL_ID_PARAMETER_NAME)
            .type(Type.STRING)
            .build(),
        ScalarParameterSpecification.builder()
            .name(TIMECOL_PARAMETER_NAME)
            .type(Type.STRING)
            .defaultValue(DEFAULT_TIME_COL)
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
      throw new SemanticException(
          String.format(QueryMessages.PARAM_SHOULD_NOT_BE_NULL_OR_EMPTY, MODEL_ID_PARAMETER_NAME));
    }

    String timeColumn =
        ((String) ((ScalarArgument) arguments.get(TIMECOL_PARAMETER_NAME)).getValue())
            .toLowerCase(Locale.ENGLISH);

    if (timeColumn.isEmpty()) {
      throw new SemanticException(
          String.format(
              QueryMessages.PARAM_SHOULD_NOT_BE_NULL_OR_EMPTY_DOT, TIMECOL_PARAMETER_NAME));
    }

    // predicated columns should never contain partition by columns and time column
    Set<String> excludedColumns =
        input.getPartitionBy().stream()
            .map(s -> s.toLowerCase(Locale.ENGLISH))
            .collect(Collectors.toSet());
    excludedColumns.add(timeColumn);
    int timeColumnIndex = findColumnIndex(input, timeColumn, Collections.singleton(Type.TIMESTAMP));

    // List of required column indexes
    List<Integer> requiredIndexList = new ArrayList<>();
    requiredIndexList.add(timeColumnIndex);

    List<Type> inputColumnTypes = new ArrayList<>();
    List<Optional<String>> allInputColumnsName = input.getFieldNames();
    List<Type> allInputColumnsType = input.getFieldTypes();

    for (int i = 0, size = allInputColumnsName.size(); i < size; i++) {
      Optional<String> fieldName = allInputColumnsName.get(i);
      // All input value columns are required for model classification
      if (!fieldName.isPresent()
          || !excludedColumns.contains(fieldName.get().toLowerCase(Locale.ENGLISH))) {
        Type columnType = allInputColumnsType.get(i);
        checkType(columnType, fieldName.orElse(""));
        inputColumnTypes.add(columnType);
        requiredIndexList.add(i);
      }
    }

    // Define output schema with classification result column
    DescribedSchema.Builder properColumnSchemaBuilder =
        new DescribedSchema.Builder().addField(DEFAULT_OUTPUT_COLUMN_NAME, Type.INT32);

    String options = (String) ((ScalarArgument) arguments.get(OPTIONS_PARAMETER_NAME)).getValue();
    ClassifyTableFunctionHandle functionHandle =
        new ClassifyTableFunctionHandle(
            modelId, MAX_INPUT_LENGTH, parseOptions(options), inputColumnTypes);

    // outputColumnSchema
    return TableFunctionAnalysis.builder()
        .properColumnSchema(properColumnSchemaBuilder.build())
        .handle(functionHandle)
        .requiredColumns(INPUT_PARAMETER_NAME, requiredIndexList)
        .build();
  }

  @Override
  public TableFunctionHandle createTableFunctionHandle() {
    return new ClassifyTableFunctionHandle();
  }

  @Override
  public TableFunctionProcessorProvider getProcessorProvider(
      TableFunctionHandle tableFunctionHandle) {
    return new TableFunctionProcessorProvider() {
      @Override
      public TableFunctionDataProcessor getDataProcessor() {
        return new ClassifyDataProcessor((ClassifyTableFunctionHandle) tableFunctionHandle);
      }
    };
  }

  private static class ClassifyDataProcessor implements TableFunctionDataProcessor {

    private static final TsBlockSerde SERDE = new TsBlockSerde();

    private final String modelId;
    private final int maxInputLength;
    private final LinkedList<Record> inputRecords;
    protected final Map<String, String> options;
    private final TsBlockBuilder inputTsBlockBuilder;
    private final List<ResultColumnAppender> inputColumnAppenderList;
    private final List<ResultColumnAppender> resultColumnAppenderList;

    public ClassifyDataProcessor(ClassifyTableFunctionHandle functionHandle) {
      this.modelId = functionHandle.modelId;
      this.maxInputLength = functionHandle.maxInputLength;
      this.options = functionHandle.options;
      this.inputRecords = new LinkedList<>();
      List<TSDataType> inputTsDataTypeList =
          new ArrayList<>(functionHandle.inputColumnTypes.size());
      this.inputColumnAppenderList = new ArrayList<>(functionHandle.inputColumnTypes.size());
      for (Type type : functionHandle.inputColumnTypes) {
        // AINode currently only accept double input
        inputTsDataTypeList.add(TSDataType.DOUBLE);
        inputColumnAppenderList.add(createResultColumnAppender(Type.DOUBLE));
      }
      this.inputTsBlockBuilder = new TsBlockBuilder(inputTsDataTypeList);
      this.resultColumnAppenderList = new ArrayList<>(1);
      this.resultColumnAppenderList.add(createResultColumnAppender(Type.INT32));
    }

    @Override
    public void process(
        Record input,
        List<ColumnBuilder> properColumnBuilders,
        ColumnBuilder passThroughIndexBuilder) {
      // only keep at most maxInputLength rows
      if (maxInputLength != 0 && inputRecords.size() == maxInputLength) {
        inputRecords.removeFirst();
      }
      inputRecords.add(input);
    }

    @Override
    public void finish(
        List<ColumnBuilder> properColumnBuilders, ColumnBuilder passThroughIndexBuilder) {

      // sort inputRecords in ascending order by timestamp
      inputRecords.sort(Comparator.comparingLong(r -> r.getLong(0)));

      // time column
      long inputStartTime = inputRecords.getFirst().getLong(0);
      long inputEndTime = inputRecords.getLast().getLong(0);
      if (inputEndTime < inputStartTime) {
        throw new SemanticException(
            String.format(
                QueryMessages.INPUT_END_TIME_LESS_THAN_START_TIME, inputStartTime, inputEndTime));
      }

      // predicated columns
      TsBlock predicatedResult = classify();

      // construct result column
      for (int columnIndex = 0, columnCount = predicatedResult.getValueColumnCount();
          columnIndex < columnCount;
          columnIndex++) {
        Column column = predicatedResult.getColumn(columnIndex);
        ColumnBuilder builder = properColumnBuilders.get(columnIndex);
        ResultColumnAppender appender = resultColumnAppenderList.get(columnIndex);
        for (int row = 0, rowCount = predicatedResult.getPositionCount(); row < rowCount; row++) {
          if (column.isNull(row)) {
            builder.appendNull();
          } else {
            // convert double to real type
            appender.writeDouble(column.getDouble(row), builder);
          }
        }
      }
    }

    private TsBlock classify() {
      // construct inputTSBlock for AINode
      while (!inputRecords.isEmpty()) {
        Record row = inputRecords.removeFirst();
        inputTsBlockBuilder.getTimeColumnBuilder().writeLong(row.getLong(0));
        for (int i = 1, size = row.size(); i < size; i++) {
          // we set null input to 0.0
          if (row.isNull(i)) {
            inputTsBlockBuilder.getColumnBuilder(i - 1).writeDouble(0.0);
          } else {
            // need to transform other types to DOUBLE
            inputTsBlockBuilder
                .getColumnBuilder(i - 1)
                .writeDouble(inputColumnAppenderList.get(i - 1).getDouble(row, i));
          }
        }
        inputTsBlockBuilder.declarePosition();
      }
      TsBlock inputData = inputTsBlockBuilder.build();

      TClassifyResp resp;
      try {
        resp =
            TableUDFUtils.getTableFunctionAINodeService()
                .classify(
                    new TClassifyReq(modelId, SERDE.serialize(inputData)).setOptions(options));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }

      if (resp.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        String message =
            String.format(QueryMessages.CLASSIFY_EXECUTION_ERROR, resp.getStatus().getMessage());
        throw new IoTDBRuntimeException(message, resp.getStatus().getCode());
      }

      // Only support one column output for now
      TsBlock res = SERDE.deserialize(resp.classifyResult.get(0));
      if (res.getValueColumnCount() != 1) {
        throw new IoTDBRuntimeException(
            String.format(
                "Model %s output %s columns, doesn't equal to specified %s",
                modelId, res.getValueColumnCount(), 1),
            TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode());
      }
      return res;
    }
  }
}
