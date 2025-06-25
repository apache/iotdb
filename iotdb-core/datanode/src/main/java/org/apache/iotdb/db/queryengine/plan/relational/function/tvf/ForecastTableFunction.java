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

package org.apache.iotdb.db.queryengine.plan.relational.function.tvf;

import org.apache.iotdb.ainode.rpc.thrift.TForecastResp;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.ainode.AINodeClient;
import org.apache.iotdb.commons.client.ainode.AINodeClientManager;
import org.apache.iotdb.commons.exception.IoTDBRuntimeException;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.queryengine.plan.analyze.IModelFetcher;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.model.ModelInferenceDescriptor;
import org.apache.iotdb.rpc.TSStatusCode;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.iotdb.commons.udf.builtin.relational.tvf.WindowTVFUtils.findColumnIndex;
import static org.apache.iotdb.rpc.TSStatusCode.CAN_NOT_CONNECT_AINODE;

public class ForecastTableFunction implements TableFunction {

  public static class ForecastTableFunctionHandle implements TableFunctionHandle {
    TEndPoint targetAINode;
    String modelId;
    int maxInputLength;
    int outputLength;
    long outputStartTime;
    long outputInterval;
    boolean keepInput;
    Map<String, String> options;
    List<Type> types;

    public ForecastTableFunctionHandle() {}

    public ForecastTableFunctionHandle(
        boolean keepInput,
        int maxInputLength,
        String modelId,
        Map<String, String> options,
        int outputLength,
        long outputStartTime,
        long outputInterval,
        TEndPoint targetAINode,
        List<Type> types) {
      this.keepInput = keepInput;
      this.maxInputLength = maxInputLength;
      this.modelId = modelId;
      this.options = options;
      this.outputLength = outputLength;
      this.outputStartTime = outputStartTime;
      this.outputInterval = outputInterval;
      this.targetAINode = targetAINode;
      this.types = types;
    }

    @Override
    public byte[] serialize() {
      try (PublicBAOS publicBAOS = new PublicBAOS();
          DataOutputStream outputStream = new DataOutputStream(publicBAOS)) {
        ReadWriteIOUtils.write(targetAINode.getIp(), outputStream);
        ReadWriteIOUtils.write(targetAINode.getPort(), outputStream);
        ReadWriteIOUtils.write(modelId, outputStream);
        ReadWriteIOUtils.write(maxInputLength, outputStream);
        ReadWriteIOUtils.write(outputLength, outputStream);
        ReadWriteIOUtils.write(outputStartTime, outputStream);
        ReadWriteIOUtils.write(outputInterval, outputStream);
        ReadWriteIOUtils.write(keepInput, outputStream);
        ReadWriteIOUtils.write(options, outputStream);
        ReadWriteIOUtils.write(types.size(), outputStream);
        for (Type type : types) {
          ReadWriteIOUtils.write(type.getType(), outputStream);
        }
        outputStream.flush();
        return publicBAOS.toByteArray();
      } catch (IOException e) {
        throw new IoTDBRuntimeException(
            String.format(
                "Error occurred while serializing ForecastTableFunctionHandle: %s", e.getMessage()),
            TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode());
      }
    }

    @Override
    public void deserialize(byte[] bytes) {
      ByteBuffer buffer = ByteBuffer.wrap(bytes);
      this.targetAINode =
          new TEndPoint(ReadWriteIOUtils.readString(buffer), ReadWriteIOUtils.readInt(buffer));
      this.modelId = ReadWriteIOUtils.readString(buffer);
      this.maxInputLength = ReadWriteIOUtils.readInt(buffer);
      this.outputLength = ReadWriteIOUtils.readInt(buffer);
      this.outputStartTime = ReadWriteIOUtils.readLong(buffer);
      this.outputInterval = ReadWriteIOUtils.readLong(buffer);
      this.keepInput = ReadWriteIOUtils.readBoolean(buffer);
      this.options = ReadWriteIOUtils.readMap(buffer);
      int size = ReadWriteIOUtils.readInt(buffer);
      this.types = new ArrayList<>(size);
      for (int i = 0; i < size; i++) {
        types.add(Type.valueOf(ReadWriteIOUtils.readByte(buffer)));
      }
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      ForecastTableFunctionHandle that = (ForecastTableFunctionHandle) o;
      return maxInputLength == that.maxInputLength
          && outputLength == that.outputLength
          && outputStartTime == that.outputStartTime
          && outputInterval == that.outputInterval
          && keepInput == that.keepInput
          && Objects.equals(targetAINode, that.targetAINode)
          && Objects.equals(modelId, that.modelId)
          && Objects.equals(options, that.options)
          && Objects.equals(types, that.types);
    }

    @Override
    public int hashCode() {
      return Objects.hash(
          targetAINode,
          modelId,
          maxInputLength,
          outputLength,
          outputStartTime,
          outputInterval,
          keepInput,
          options,
          types);
    }
  }

  private static final String INPUT_PARAMETER_NAME = "INPUT";
  private static final String MODEL_ID_PARAMETER_NAME = "MODEL_ID";
  private static final String OUTPUT_LENGTH_PARAMETER_NAME = "OUTPUT_LENGTH";
  private static final int DEFAULT_OUTPUT_LENGTH = 96;
  private static final String PREDICATED_COLUMNS_PARAMETER_NAME = "PREDICATED_COLUMNS";
  private static final String DEFAULT_PREDICATED_COLUMNS = "";
  private static final String OUTPUT_START_TIME = "OUTPUT_START_TIME";
  public static final long DEFAULT_OUTPUT_START_TIME = Long.MIN_VALUE;
  private static final String OUTPUT_INTERVAL = "OUTPUT_INTERVAL";
  public static final long DEFAULT_OUTPUT_INTERVAL = 0L;
  public static final String TIMECOL_PARAMETER_NAME = "TIMECOL";
  private static final String DEFAULT_TIME_COL = "time";
  private static final String KEEP_INPUT_PARAMETER_NAME = "PRESERVE_INPUT";
  private static final Boolean DEFAULT_KEEP_INPUT = Boolean.FALSE;
  private static final String IS_INPUT_COLUMN_NAME = "is_input";
  private static final String OPTIONS_PARAMETER_NAME = "MODEL_OPTIONS";
  private static final String DEFAULT_OPTIONS = "";

  private static final String INVALID_OPTIONS_FORMAT = "Invalid options: %s";

  private static final Set<Type> ALLOWED_INPUT_TYPES = new HashSet<>();

  static {
    ALLOWED_INPUT_TYPES.add(Type.INT32);
    ALLOWED_INPUT_TYPES.add(Type.INT64);
    ALLOWED_INPUT_TYPES.add(Type.FLOAT);
    ALLOWED_INPUT_TYPES.add(Type.DOUBLE);
  }

  // need to set before analyze method is called
  // should only be used in fe scope, never be used in TableFunctionProcessorProvider
  // The reason we don't directly set modelFetcher=ModelFetcher.getInstance() is that we need to
  // mock IModelFetcher in UT
  private IModelFetcher modelFetcher = null;

  public void setModelFetcher(IModelFetcher modelFetcher) {
    this.modelFetcher = modelFetcher;
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
            .name(OUTPUT_LENGTH_PARAMETER_NAME)
            .type(Type.INT32)
            .defaultValue(DEFAULT_OUTPUT_LENGTH)
            .build(),
        ScalarParameterSpecification.builder()
            .name(OUTPUT_START_TIME)
            .type(Type.TIMESTAMP)
            .defaultValue(DEFAULT_OUTPUT_START_TIME)
            .build(),
        ScalarParameterSpecification.builder()
            .name(OUTPUT_INTERVAL)
            .type(Type.INT64)
            .defaultValue(DEFAULT_OUTPUT_INTERVAL)
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
  public TableFunctionAnalysis analyze(Map<String, Argument> arguments) {
    TableArgument input = (TableArgument) arguments.get(INPUT_PARAMETER_NAME);
    String modelId = (String) ((ScalarArgument) arguments.get(MODEL_ID_PARAMETER_NAME)).getValue();
    // modelId should never be null or empty
    if (modelId == null || modelId.isEmpty()) {
      throw new SemanticException(
          String.format("%s should never be null or empty", MODEL_ID_PARAMETER_NAME));
    }

    // make sure modelId exists
    ModelInferenceDescriptor descriptor = getModelInfo(modelId);
    if (descriptor == null || !descriptor.getModelInformation().available()) {
      throw new IoTDBRuntimeException(
          String.format("model [%s] is not available", modelId),
          TSStatusCode.GET_MODEL_INFO_ERROR.getStatusCode());
    }

    int maxInputLength = descriptor.getModelInformation().getInputShape()[0];
    TEndPoint targetAINode = descriptor.getTargetAINode();

    int outputLength =
        (int) ((ScalarArgument) arguments.get(OUTPUT_LENGTH_PARAMETER_NAME)).getValue();
    if (outputLength <= 0) {
      throw new SemanticException(
          String.format("%s should be greater than 0", OUTPUT_LENGTH_PARAMETER_NAME));
    }

    String predicatedColumns =
        (String) ((ScalarArgument) arguments.get(PREDICATED_COLUMNS_PARAMETER_NAME)).getValue();

    String timeColumn =
        ((String) ((ScalarArgument) arguments.get(TIMECOL_PARAMETER_NAME)).getValue())
            .toLowerCase(Locale.ENGLISH);

    if (timeColumn.isEmpty()) {
      throw new SemanticException(
          String.format("%s should never be null or empty.", TIMECOL_PARAMETER_NAME));
    }

    // predicated columns should never contain partition by columns and time column
    Set<String> excludedColumns =
        input.getPartitionBy().stream()
            .map(s -> s.toLowerCase(Locale.ENGLISH))
            .collect(Collectors.toSet());
    excludedColumns.add(timeColumn);
    int timeColumnIndex = findColumnIndex(input, timeColumn, Collections.singleton(Type.TIMESTAMP));

    List<Integer> requiredIndexList = new ArrayList<>();
    requiredIndexList.add(timeColumnIndex);
    DescribedSchema.Builder properColumnSchemaBuilder =
        new DescribedSchema.Builder().addField(timeColumn, Type.TIMESTAMP);

    List<Type> predicatedColumnTypes = new ArrayList<>();
    List<Optional<String>> allInputColumnsName = input.getFieldNames();
    List<Type> allInputColumnsType = input.getFieldTypes();
    if (predicatedColumns.isEmpty()) {
      // predicated columns by default include all columns from input table except for timecol and
      // partition by columns
      for (int i = 0, size = allInputColumnsName.size(); i < size; i++) {
        Optional<String> fieldName = allInputColumnsName.get(i);
        if (!fieldName.isPresent()
            || !excludedColumns.contains(fieldName.get().toLowerCase(Locale.ENGLISH))) {
          Type columnType = allInputColumnsType.get(i);
          predicatedColumnTypes.add(columnType);
          checkType(columnType, fieldName.orElse(""));
          requiredIndexList.add(i);
          properColumnSchemaBuilder.addField(fieldName, columnType);
        }
      }
    } else {
      String[] predictedColumnsArray = predicatedColumns.split(";");
      Map<String, Integer> inputColumnIndexMap = new HashMap<>();
      for (int i = 0, size = allInputColumnsName.size(); i < size; i++) {
        Optional<String> fieldName = allInputColumnsName.get(i);
        if (!fieldName.isPresent()) {
          continue;
        }
        inputColumnIndexMap.put(fieldName.get().toLowerCase(Locale.ENGLISH), i);
      }

      Set<Integer> requiredIndexSet = new HashSet<>(predictedColumnsArray.length);
      // columns need to be predicated
      for (String outputColumn : predictedColumnsArray) {
        String lowerCaseOutputColumn = outputColumn.toLowerCase(Locale.ENGLISH);
        if (excludedColumns.contains(lowerCaseOutputColumn)) {
          throw new SemanticException(
              String.format("%s is in partition by clause or is time column", outputColumn));
        }
        Integer inputColumnIndex = inputColumnIndexMap.get(lowerCaseOutputColumn);
        if (inputColumnIndex == null) {
          throw new SemanticException(
              String.format("Column %s don't exist in input", outputColumn));
        }
        if (!requiredIndexSet.add(inputColumnIndex)) {
          throw new SemanticException(String.format("Duplicate column %s", outputColumn));
        }

        Type columnType = allInputColumnsType.get(inputColumnIndex);
        predicatedColumnTypes.add(columnType);
        checkType(columnType, outputColumn);
        requiredIndexList.add(inputColumnIndex);
        properColumnSchemaBuilder.addField(outputColumn, columnType);
      }
    }

    boolean keepInput =
        (boolean) ((ScalarArgument) arguments.get(KEEP_INPUT_PARAMETER_NAME)).getValue();
    if (keepInput) {
      properColumnSchemaBuilder.addField(IS_INPUT_COLUMN_NAME, Type.BOOLEAN);
    }

    long outputStartTime = (long) ((ScalarArgument) arguments.get(OUTPUT_START_TIME)).getValue();
    long outputInterval = (long) ((ScalarArgument) arguments.get(OUTPUT_INTERVAL)).getValue();
    String options = (String) ((ScalarArgument) arguments.get(OPTIONS_PARAMETER_NAME)).getValue();

    ForecastTableFunctionHandle functionHandle =
        new ForecastTableFunctionHandle(
            keepInput,
            maxInputLength,
            modelId,
            parseOptions(options),
            outputLength,
            outputStartTime,
            outputInterval,
            targetAINode,
            predicatedColumnTypes);

    // outputColumnSchema
    return TableFunctionAnalysis.builder()
        .properColumnSchema(properColumnSchemaBuilder.build())
        .handle(functionHandle)
        .requiredColumns(INPUT_PARAMETER_NAME, requiredIndexList)
        .build();
  }

  @Override
  public TableFunctionHandle createTableFunctionHandle() {
    return new ForecastTableFunctionHandle();
  }

  @Override
  public TableFunctionProcessorProvider getProcessorProvider(
      TableFunctionHandle tableFunctionHandle) {
    return new TableFunctionProcessorProvider() {
      @Override
      public TableFunctionDataProcessor getDataProcessor() {
        return new ForecastDataProcessor((ForecastTableFunctionHandle) tableFunctionHandle);
      }
    };
  }

  private ModelInferenceDescriptor getModelInfo(String modelId) {
    return modelFetcher.fetchModel(modelId);
  }

  // only allow for INT32, INT64, FLOAT, DOUBLE
  private void checkType(Type type, String columnName) {
    if (!ALLOWED_INPUT_TYPES.contains(type)) {
      throw new SemanticException(
          String.format(
              "The type of the column [%s] is [%s], only INT32, INT64, FLOAT, DOUBLE is allowed",
              columnName, type));
    }
  }

  private static Map<String, String> parseOptions(String options) {
    if (options.isEmpty()) {
      return Collections.emptyMap();
    }
    String[] optionArray = options.split(",");
    if (optionArray.length == 0) {
      throw new SemanticException(String.format(INVALID_OPTIONS_FORMAT, options));
    }

    Map<String, String> optionsMap = new HashMap<>(optionArray.length);
    for (String option : optionArray) {
      int index = option.indexOf('=');
      if (index == -1 || index == option.length() - 1) {
        throw new SemanticException(String.format(INVALID_OPTIONS_FORMAT, option));
      }
      String key = option.substring(0, index).trim();
      String value = option.substring(index + 1).trim();
      optionsMap.put(key, value);
    }
    return optionsMap;
  }

  private static class ForecastDataProcessor implements TableFunctionDataProcessor {

    private static final TsBlockSerde SERDE = new TsBlockSerde();
    private static final IClientManager<TEndPoint, AINodeClient> CLIENT_MANAGER =
        AINodeClientManager.getInstance();

    private final TEndPoint targetAINode;
    private final String modelId;
    private final int maxInputLength;
    private final int outputLength;
    private final long outputStartTime;
    private final long outputInterval;
    private final boolean keepInput;
    private final Map<String, String> options;
    private final LinkedList<Record> inputRecords;
    private final List<ResultColumnAppender> resultColumnAppenderList;
    private final TsBlockBuilder inputTsBlockBuilder;

    public ForecastDataProcessor(ForecastTableFunctionHandle functionHandle) {
      this.targetAINode = functionHandle.targetAINode;
      this.modelId = functionHandle.modelId;
      this.maxInputLength = functionHandle.maxInputLength;
      this.outputLength = functionHandle.outputLength;
      this.outputStartTime = functionHandle.outputStartTime;
      this.outputInterval = functionHandle.outputInterval;
      this.keepInput = functionHandle.keepInput;
      this.options = functionHandle.options;
      this.inputRecords = new LinkedList<>();
      this.resultColumnAppenderList = new ArrayList<>(functionHandle.types.size());
      List<TSDataType> tsDataTypeList = new ArrayList<>(functionHandle.types.size());
      for (Type type : functionHandle.types) {
        resultColumnAppenderList.add(createResultColumnAppender(type));
        // ainode currently only accept double input
        tsDataTypeList.add(TSDataType.DOUBLE);
      }
      this.inputTsBlockBuilder = new TsBlockBuilder(tsDataTypeList);
    }

    private static ResultColumnAppender createResultColumnAppender(Type type) {
      switch (type) {
        case INT32:
          return new Int32Appender();
        case INT64:
          return new Int64Appender();
        case FLOAT:
          return new FloatAppender();
        case DOUBLE:
          return new DoubleAppender();
        default:
          throw new IllegalArgumentException("Unsupported column type: " + type);
      }
    }

    @Override
    public void process(
        Record input,
        List<ColumnBuilder> properColumnBuilders,
        ColumnBuilder passThroughIndexBuilder) {

      if (keepInput) {
        int columnSize = properColumnBuilders.size();

        // time column, will never be null
        if (input.isNull(0)) {
          throw new IoTDBRuntimeException(
              "Time column should never be null", TSStatusCode.SEMANTIC_ERROR.getStatusCode());
        }
        properColumnBuilders.get(0).writeLong(input.getLong(0));

        // predicated columns
        for (int i = 1, size = columnSize - 1; i < size; i++) {
          resultColumnAppenderList.get(i - 1).append(input, i, properColumnBuilders.get(i));
        }

        // is_input column
        properColumnBuilders.get(columnSize - 1).writeBoolean(true);
      }

      // only keep at most maxInputLength rows
      if (maxInputLength != 0 && inputRecords.size() == maxInputLength) {
        inputRecords.removeFirst();
      }
      inputRecords.add(input);
    }

    @Override
    public void finish(
        List<ColumnBuilder> properColumnBuilders, ColumnBuilder passThroughIndexBuilder) {

      int columnSize = properColumnBuilders.size();

      // time column
      long inputStartTime = inputRecords.getFirst().getLong(0);
      long inputEndTime = inputRecords.getLast().getLong(0);
      if (inputEndTime < inputStartTime) {
        throw new SemanticException(
            String.format(
                "input end time should never less than start time, start time is %s, end time is %s",
                inputStartTime, inputEndTime));
      }
      long interval = outputInterval;
      if (outputInterval <= 0) {
        interval =
            inputRecords.size() == 1
                ? 0
                : (inputEndTime - inputStartTime) / (inputRecords.size() - 1);
      }
      long outputTime =
          (outputStartTime == Long.MIN_VALUE) ? (inputEndTime + interval) : outputStartTime;
      for (int i = 0; i < outputLength; i++) {
        properColumnBuilders.get(0).writeLong(outputTime + interval * i);
      }

      // predicated columns
      TsBlock predicatedResult = forecast();
      if (predicatedResult.getPositionCount() != outputLength) {
        throw new IoTDBRuntimeException(
            String.format(
                "Model %s output length is %s, doesn't equal to specified %s",
                modelId, predicatedResult.getPositionCount(), outputLength),
            TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode());
      }

      for (int columnIndex = 1, size = predicatedResult.getValueColumnCount();
          columnIndex <= size;
          columnIndex++) {
        Column column = predicatedResult.getColumn(columnIndex - 1);
        ColumnBuilder builder = properColumnBuilders.get(columnIndex);
        ResultColumnAppender appender = resultColumnAppenderList.get(columnIndex - 1);
        for (int row = 0; row < outputLength; row++) {
          if (column.isNull(row)) {
            builder.appendNull();
          } else {
            // convert double to real type
            appender.writeDouble(column.getDouble(row), builder);
          }
        }
      }

      // is_input column if keep_input is true
      if (keepInput) {
        for (int i = 0; i < outputLength; i++) {
          properColumnBuilders.get(columnSize - 1).writeBoolean(false);
        }
      }
    }

    private TsBlock forecast() {
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
                .writeDouble(resultColumnAppenderList.get(i - 1).getDouble(row, i));
          }
        }
        inputTsBlockBuilder.declarePosition();
      }
      TsBlock inputData = inputTsBlockBuilder.build();

      TForecastResp resp;
      try (AINodeClient client = CLIENT_MANAGER.borrowClient(targetAINode)) {
        resp = client.forecast(modelId, inputData, outputLength, options);
      } catch (Exception e) {
        throw new IoTDBRuntimeException(e.getMessage(), CAN_NOT_CONNECT_AINODE.getStatusCode());
      }

      if (resp.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        String message =
            String.format(
                "Error occurred while executing forecast:[%s]", resp.getStatus().getMessage());
        throw new IoTDBRuntimeException(message, resp.getStatus().getCode());
      }

      TsBlock res = SERDE.deserialize(ByteBuffer.wrap(resp.getForecastResult()));
      if (res.getValueColumnCount() != inputData.getValueColumnCount()) {
        throw new IoTDBRuntimeException(
            String.format(
                "Model %s output %s columns, doesn't equal to specified %s",
                modelId, res.getValueColumnCount(), inputData.getValueColumnCount()),
            TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode());
      }
      return res;
    }
  }

  private interface ResultColumnAppender {
    void append(Record row, int columnIndex, ColumnBuilder properColumnBuilder);

    double getDouble(Record row, int columnIndex);

    void writeDouble(double value, ColumnBuilder columnBuilder);
  }

  private static class Int32Appender implements ResultColumnAppender {

    @Override
    public void append(Record row, int columnIndex, ColumnBuilder properColumnBuilder) {
      if (row.isNull(columnIndex)) {
        properColumnBuilder.appendNull();
      } else {
        properColumnBuilder.writeInt(row.getInt(columnIndex));
      }
    }

    @Override
    public double getDouble(Record row, int columnIndex) {
      return row.getInt(columnIndex);
    }

    @Override
    public void writeDouble(double value, ColumnBuilder columnBuilder) {
      columnBuilder.writeInt((int) value);
    }
  }

  private static class Int64Appender implements ResultColumnAppender {

    @Override
    public void append(Record row, int columnIndex, ColumnBuilder properColumnBuilder) {
      if (row.isNull(columnIndex)) {
        properColumnBuilder.appendNull();
      } else {
        properColumnBuilder.writeLong(row.getLong(columnIndex));
      }
    }

    @Override
    public double getDouble(Record row, int columnIndex) {
      return row.getLong(columnIndex);
    }

    @Override
    public void writeDouble(double value, ColumnBuilder columnBuilder) {
      columnBuilder.writeLong((long) value);
    }
  }

  private static class FloatAppender implements ResultColumnAppender {

    @Override
    public void append(Record row, int columnIndex, ColumnBuilder properColumnBuilder) {
      if (row.isNull(columnIndex)) {
        properColumnBuilder.appendNull();
      } else {
        properColumnBuilder.writeFloat(row.getFloat(columnIndex));
      }
    }

    @Override
    public double getDouble(Record row, int columnIndex) {
      return row.getFloat(columnIndex);
    }

    @Override
    public void writeDouble(double value, ColumnBuilder columnBuilder) {
      columnBuilder.writeFloat((float) value);
    }
  }

  private static class DoubleAppender implements ResultColumnAppender {

    @Override
    public void append(Record row, int columnIndex, ColumnBuilder properColumnBuilder) {
      if (row.isNull(columnIndex)) {
        properColumnBuilder.appendNull();
      } else {
        properColumnBuilder.writeDouble(row.getDouble(columnIndex));
      }
    }

    @Override
    public double getDouble(Record row, int columnIndex) {
      return row.getDouble(columnIndex);
    }

    @Override
    public void writeDouble(double value, ColumnBuilder columnBuilder) {
      columnBuilder.writeDouble(value);
    }
  }
}
