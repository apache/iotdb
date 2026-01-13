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

import org.apache.iotdb.ainode.rpc.thrift.TForecastReq;
import org.apache.iotdb.ainode.rpc.thrift.TForecastResp;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.exception.IoTDBRuntimeException;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.protocol.client.an.AINodeClient;
import org.apache.iotdb.db.protocol.client.an.AINodeClientManager;
import org.apache.iotdb.db.queryengine.plan.relational.utils.ResultColumnAppender;
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
import java.util.Comparator;
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
import static org.apache.iotdb.db.queryengine.plan.relational.utils.ResultColumnAppender.createResultColumnAppender;
import static org.apache.iotdb.rpc.TSStatusCode.CAN_NOT_CONNECT_AINODE;

public class ForecastTableFunction implements TableFunction {

  public static class ForecastTableFunctionHandle implements TableFunctionHandle {
    protected String modelId;
    protected int maxInputLength;
    protected int outputLength;
    protected long outputStartTime;
    protected long outputInterval;
    protected boolean keepInput;
    protected Map<String, String> options;
    protected List<Type> targetColumntypes;

    public ForecastTableFunctionHandle() {}

    public ForecastTableFunctionHandle(
        boolean keepInput,
        int maxInputLength,
        String modelId,
        Map<String, String> options,
        int outputLength,
        long outputStartTime,
        long outputInterval,
        List<Type> targetColumntypes) {
      this.keepInput = keepInput;
      this.maxInputLength = maxInputLength;
      this.modelId = modelId;
      this.options = options;
      this.outputLength = outputLength;
      this.outputStartTime = outputStartTime;
      this.outputInterval = outputInterval;
      this.targetColumntypes = targetColumntypes;
    }

    @Override
    public byte[] serialize() {
      try (PublicBAOS publicBAOS = new PublicBAOS();
          DataOutputStream outputStream = new DataOutputStream(publicBAOS)) {
        ReadWriteIOUtils.write(modelId, outputStream);
        ReadWriteIOUtils.write(maxInputLength, outputStream);
        ReadWriteIOUtils.write(outputLength, outputStream);
        ReadWriteIOUtils.write(outputStartTime, outputStream);
        ReadWriteIOUtils.write(outputInterval, outputStream);
        ReadWriteIOUtils.write(keepInput, outputStream);
        ReadWriteIOUtils.write(options, outputStream);
        ReadWriteIOUtils.write(targetColumntypes.size(), outputStream);
        for (Type type : targetColumntypes) {
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
      this.modelId = ReadWriteIOUtils.readString(buffer);
      this.maxInputLength = ReadWriteIOUtils.readInt(buffer);
      this.outputLength = ReadWriteIOUtils.readInt(buffer);
      this.outputStartTime = ReadWriteIOUtils.readLong(buffer);
      this.outputInterval = ReadWriteIOUtils.readLong(buffer);
      this.keepInput = ReadWriteIOUtils.readBoolean(buffer);
      this.options = ReadWriteIOUtils.readMap(buffer);
      int size = ReadWriteIOUtils.readInt(buffer);
      this.targetColumntypes = new ArrayList<>(size);
      for (int i = 0; i < size; i++) {
        targetColumntypes.add(Type.valueOf(ReadWriteIOUtils.readByte(buffer)));
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
          && Objects.equals(modelId, that.modelId)
          && Objects.equals(options, that.options)
          && Objects.equals(targetColumntypes, that.targetColumntypes);
    }

    @Override
    public int hashCode() {
      return Objects.hash(
          modelId,
          maxInputLength,
          outputLength,
          outputStartTime,
          outputInterval,
          keepInput,
          options,
          targetColumntypes);
    }
  }

  protected static final String TARGETS_PARAMETER_NAME = "TARGETS";
  protected static final String MODEL_ID_PARAMETER_NAME = "MODEL_ID";
  protected static final String OUTPUT_LENGTH_PARAMETER_NAME = "OUTPUT_LENGTH";
  protected static final int DEFAULT_OUTPUT_LENGTH = 96;
  protected static final String OUTPUT_START_TIME = "OUTPUT_START_TIME";
  public static final long DEFAULT_OUTPUT_START_TIME = Long.MIN_VALUE;
  protected static final String OUTPUT_INTERVAL = "OUTPUT_INTERVAL";
  public static final long DEFAULT_OUTPUT_INTERVAL = 0L;
  public static final String TIMECOL_PARAMETER_NAME = "TIMECOL";
  protected static final String DEFAULT_TIME_COL = "time";
  protected static final String KEEP_INPUT_PARAMETER_NAME = "PRESERVE_INPUT";
  protected static final Boolean DEFAULT_KEEP_INPUT = Boolean.FALSE;
  protected static final String IS_INPUT_COLUMN_NAME = "is_input";
  protected static final String OPTIONS_PARAMETER_NAME = "MODEL_OPTIONS";
  protected static final String DEFAULT_OPTIONS = "";
  protected static final int MAX_INPUT_LENGTH = 2880;

  private static final String INVALID_OPTIONS_FORMAT = "Invalid options: %s";

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
        TableParameterSpecification.builder().name(TARGETS_PARAMETER_NAME).setSemantics().build(),
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
    TableArgument targets = (TableArgument) arguments.get(TARGETS_PARAMETER_NAME);
    String modelId = (String) ((ScalarArgument) arguments.get(MODEL_ID_PARAMETER_NAME)).getValue();
    // modelId should never be null or empty
    if (modelId == null || modelId.isEmpty()) {
      throw new SemanticException(
          String.format("%s should never be null or empty", MODEL_ID_PARAMETER_NAME));
    }

    int outputLength =
        (int) ((ScalarArgument) arguments.get(OUTPUT_LENGTH_PARAMETER_NAME)).getValue();
    if (outputLength <= 0) {
      throw new SemanticException(
          String.format("%s should be greater than 0", OUTPUT_LENGTH_PARAMETER_NAME));
    }

    String timeColumn =
        ((String) ((ScalarArgument) arguments.get(TIMECOL_PARAMETER_NAME)).getValue())
            .toLowerCase(Locale.ENGLISH);
    if (timeColumn.isEmpty()) {
      throw new SemanticException(
          String.format("%s should never be null or empty.", TIMECOL_PARAMETER_NAME));
    }

    long outputInterval = (long) ((ScalarArgument) arguments.get(OUTPUT_INTERVAL)).getValue();
    if (outputInterval < 0) {
      throw new SemanticException(String.format("%s should be greater than 0", OUTPUT_INTERVAL));
    }

    // predicated columns should never contain partition by columns and time column
    Set<String> excludedColumns =
        targets.getPartitionBy().stream()
            .map(s -> s.toLowerCase(Locale.ENGLISH))
            .collect(Collectors.toSet());
    excludedColumns.add(timeColumn);
    int timeColumnIndex =
        findColumnIndex(targets, timeColumn, Collections.singleton(Type.TIMESTAMP));

    // List of required column indexes
    List<Integer> requiredIndexList = new ArrayList<>();
    requiredIndexList.add(timeColumnIndex);
    DescribedSchema.Builder properColumnSchemaBuilder =
        new DescribedSchema.Builder().addField(timeColumn, Type.TIMESTAMP);

    List<Type> targetColumnTypes = new ArrayList<>();
    List<Optional<String>> allInputColumnsName = targets.getFieldNames();
    List<Type> allInputColumnsType = targets.getFieldTypes();

    // predicated columns = all input columns except timecol / partition by columns
    for (int i = 0, size = allInputColumnsName.size(); i < size; i++) {
      Optional<String> fieldName = allInputColumnsName.get(i);
      if (!fieldName.isPresent()
          || excludedColumns.contains(fieldName.get().toLowerCase(Locale.ENGLISH))) {
        continue;
      }

      Type columnType = allInputColumnsType.get(i);
      targetColumnTypes.add(columnType);
      checkType(columnType, fieldName.get());
      requiredIndexList.add(i);
      properColumnSchemaBuilder.addField(fieldName.get(), columnType);
    }

    if (targetColumnTypes.size() > 1) {
      throw new SemanticException(
          String.format(
              "%s should not contain more than one target column, found [%s] target columns.",
              TARGETS_PARAMETER_NAME, targetColumnTypes.size()));
    }

    boolean keepInput =
        (boolean) ((ScalarArgument) arguments.get(KEEP_INPUT_PARAMETER_NAME)).getValue();
    if (keepInput) {
      properColumnSchemaBuilder.addField(IS_INPUT_COLUMN_NAME, Type.BOOLEAN);
    }

    long outputStartTime = (long) ((ScalarArgument) arguments.get(OUTPUT_START_TIME)).getValue();
    String options = (String) ((ScalarArgument) arguments.get(OPTIONS_PARAMETER_NAME)).getValue();

    ForecastTableFunctionHandle functionHandle =
        new ForecastTableFunctionHandle(
            keepInput,
            MAX_INPUT_LENGTH,
            modelId,
            parseOptions(options),
            outputLength,
            outputStartTime,
            outputInterval,
            targetColumnTypes);

    // outputColumnSchema
    return TableFunctionAnalysis.builder()
        .properColumnSchema(properColumnSchemaBuilder.build())
        .handle(functionHandle)
        .requiredColumns(TARGETS_PARAMETER_NAME, requiredIndexList)
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

  // only allow for INT32, INT64, FLOAT, DOUBLE
  public void checkType(Type type, String columnName) {
    if (!ALLOWED_INPUT_TYPES.contains(type)) {
      throw new SemanticException(
          String.format(
              "The type of the column [%s] is [%s], only INT32, INT64, FLOAT, DOUBLE is allowed",
              columnName, type));
    }
  }

  public static Map<String, String> parseOptions(String options) {
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

  protected static class ForecastDataProcessor implements TableFunctionDataProcessor {

    protected static final TsBlockSerde SERDE = new TsBlockSerde();
    protected static final IClientManager<Integer, AINodeClient> CLIENT_MANAGER =
        AINodeClientManager.getInstance();

    protected final String modelId;
    private final int maxInputLength;
    protected final int outputLength;
    private final long outputStartTime;
    private final long outputInterval;
    private final boolean keepInput;
    protected final Map<String, String> options;
    protected final LinkedList<Record> inputRecords;
    protected final List<ResultColumnAppender> resultColumnAppenderList;
    protected final TsBlockBuilder inputTsBlockBuilder;

    public ForecastDataProcessor(ForecastTableFunctionHandle functionHandle) {
      this.modelId = functionHandle.modelId;
      this.maxInputLength = functionHandle.maxInputLength;
      this.outputLength = functionHandle.outputLength;
      this.outputStartTime = functionHandle.outputStartTime;
      this.outputInterval = functionHandle.outputInterval;
      this.keepInput = functionHandle.keepInput;
      this.options = functionHandle.options;
      this.inputRecords = new LinkedList<>();
      this.resultColumnAppenderList = new ArrayList<>(functionHandle.targetColumntypes.size());
      List<TSDataType> tsDataTypeList = new ArrayList<>(functionHandle.targetColumntypes.size());
      for (Type type : functionHandle.targetColumntypes) {
        resultColumnAppenderList.add(createResultColumnAppender(type));
        // ainode currently only accept double input
        tsDataTypeList.add(TSDataType.DOUBLE);
      }
      this.inputTsBlockBuilder = new TsBlockBuilder(tsDataTypeList);
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

      // sort inputRecords in ascending order by timestamp
      inputRecords.sort(Comparator.comparingLong(record -> record.getLong(0)));

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
      if (outputTime <= inputEndTime) {
        throw new SemanticException(
            String.format(
                "The %s should be greater than the maximum timestamp of target time series. Expected greater than [%s] but found [%s].",
                OUTPUT_START_TIME, inputEndTime, outputTime));
      }
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

    protected TsBlock forecast() {
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
                .writeDouble(resultColumnAppenderList.get(i - 1).getDouble(row, i));
          }
        }
        inputTsBlockBuilder.declarePosition();
      }
      TsBlock inputData = inputTsBlockBuilder.build();

      TForecastResp resp;
      try (AINodeClient client =
          CLIENT_MANAGER.borrowClient(AINodeClientManager.AINODE_ID_PLACEHOLDER)) {
        resp =
            client.forecast(
                new TForecastReq(modelId, SERDE.serialize(inputData), outputLength)
                    .setOptions(options));
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
}
