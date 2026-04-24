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

package com.timecho.iotdb.commons.queryengine.plan.relational.function.tvf;

import org.apache.iotdb.ainode.rpc.thrift.TForecastReq;
import org.apache.iotdb.ainode.rpc.thrift.TForecastResp;
import org.apache.iotdb.commons.exception.IoTDBRuntimeException;
import org.apache.iotdb.commons.exception.SemanticException;
import org.apache.iotdb.commons.queryengine.plan.relational.function.tvf.ForecastTableFunction;
import org.apache.iotdb.commons.queryengine.plan.udf.TableUDFUtils;
import org.apache.iotdb.rpc.TSStatusCode;
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

import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.utils.PublicBAOS;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.iotdb.commons.queryengine.plan.relational.function.tvf.TableFunctionUtils.checkType;
import static org.apache.iotdb.commons.queryengine.plan.relational.function.tvf.TableFunctionUtils.parseOptions;
import static org.apache.iotdb.commons.udf.builtin.relational.tvf.WindowTVFUtils.findColumnIndex;

public class TimechoForecastTableFunction extends ForecastTableFunction {

  public static class TimechoForecastTableFunctionHandle
      extends ForecastTableFunction.ForecastTableFunctionHandle {
    protected String historyCovs;
    protected String futureCovs;
    protected boolean autoAdapt;

    public TimechoForecastTableFunctionHandle() {}

    public TimechoForecastTableFunctionHandle(
        boolean autoAdapt,
        boolean keepInput,
        int maxInputLength,
        String modelId,
        Map<String, String> options,
        String historyCovs,
        String futureCovs,
        int outputLength,
        long outputStartTime,
        long outputInterval,
        List<Type> targetColumntypes) {
      super(
          keepInput,
          maxInputLength,
          modelId,
          options,
          outputLength,
          outputStartTime,
          outputInterval,
          targetColumntypes);
      this.autoAdapt = autoAdapt;
      this.historyCovs = historyCovs;
      this.futureCovs = futureCovs;
    }

    @Override
    public byte[] serialize() {
      try (PublicBAOS publicBAOS = new PublicBAOS();
          DataOutputStream outputStream = new DataOutputStream(publicBAOS)) {
        ReadWriteIOUtils.write(modelId, outputStream);
        ReadWriteIOUtils.write(maxInputLength, outputStream);
        ReadWriteIOUtils.write(outputLength, outputStream);
        ReadWriteIOUtils.write(historyCovs, outputStream);
        ReadWriteIOUtils.write(futureCovs, outputStream);
        ReadWriteIOUtils.write(outputStartTime, outputStream);
        ReadWriteIOUtils.write(outputInterval, outputStream);
        ReadWriteIOUtils.write(keepInput, outputStream);
        ReadWriteIOUtils.write(autoAdapt, outputStream);
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
      this.historyCovs = ReadWriteIOUtils.readString(buffer);
      this.futureCovs = ReadWriteIOUtils.readString(buffer);
      this.outputStartTime = ReadWriteIOUtils.readLong(buffer);
      this.outputInterval = ReadWriteIOUtils.readLong(buffer);
      this.keepInput = ReadWriteIOUtils.readBoolean(buffer);
      this.autoAdapt = ReadWriteIOUtils.readBoolean(buffer);
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
      TimechoForecastTableFunctionHandle that = (TimechoForecastTableFunctionHandle) o;
      return maxInputLength == that.maxInputLength
          && outputLength == that.outputLength
          && outputStartTime == that.outputStartTime
          && outputInterval == that.outputInterval
          && keepInput == that.keepInput
          && autoAdapt == that.autoAdapt
          && Objects.equals(modelId, that.modelId)
          && Objects.equals(historyCovs, that.historyCovs)
          && Objects.equals(futureCovs, that.futureCovs)
          && Objects.equals(options, that.options)
          && Objects.equals(targetColumntypes, that.targetColumntypes);
    }

    @Override
    public int hashCode() {
      return Objects.hash(
          modelId,
          maxInputLength,
          outputLength,
          historyCovs,
          futureCovs,
          outputStartTime,
          outputInterval,
          keepInput,
          autoAdapt,
          options,
          targetColumntypes);
    }
  }

  private static final String HISTORY_COVS_PARAMETER_NAME = "HISTORY_COVS";
  private static final String DEFAULT_HISTORY_COVS = "";
  private static final String FUTURE_COVS_PARAMETER_NAME = "FUTURE_COVS";
  private static final String DEFAULT_FUTURE_COVS = "";
  private static final String AUTO_ADAPT_PARAMETER_NAME = "AUTO_ADAPT";
  private static final Boolean DEFAULT_AUTO_ADAPT = Boolean.TRUE;

  @Override
  public List<ParameterSpecification> getArgumentsSpecifications() {
    return Arrays.asList(
        TableParameterSpecification.builder().name(TARGETS_PARAMETER_NAME).setSemantics().build(),
        ScalarParameterSpecification.builder()
            .name(MODEL_ID_PARAMETER_NAME)
            .type(Type.STRING)
            .build(),
        ScalarParameterSpecification.builder()
            .name(HISTORY_COVS_PARAMETER_NAME)
            .type(Type.STRING)
            .defaultValue(DEFAULT_HISTORY_COVS)
            .build(),
        ScalarParameterSpecification.builder()
            .name(FUTURE_COVS_PARAMETER_NAME)
            .type(Type.STRING)
            .defaultValue(DEFAULT_FUTURE_COVS)
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
            .name(AUTO_ADAPT_PARAMETER_NAME)
            .type(Type.BOOLEAN)
            .defaultValue(DEFAULT_AUTO_ADAPT)
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

    boolean keepInput =
        (boolean) ((ScalarArgument) arguments.get(KEEP_INPUT_PARAMETER_NAME)).getValue();
    if (keepInput) {
      properColumnSchemaBuilder.addField(IS_INPUT_COLUMN_NAME, Type.BOOLEAN);
    }

    boolean autoAdapt =
        (boolean) ((ScalarArgument) arguments.get(AUTO_ADAPT_PARAMETER_NAME)).getValue();
    long outputStartTime = (long) ((ScalarArgument) arguments.get(OUTPUT_START_TIME)).getValue();
    String historyCovs =
        (String) ((ScalarArgument) arguments.get(HISTORY_COVS_PARAMETER_NAME)).getValue();
    String futureCovs =
        (String) ((ScalarArgument) arguments.get(FUTURE_COVS_PARAMETER_NAME)).getValue();
    String options = (String) ((ScalarArgument) arguments.get(OPTIONS_PARAMETER_NAME)).getValue();

    ForecastTableFunctionHandle functionHandle =
        new TimechoForecastTableFunctionHandle(
            autoAdapt,
            keepInput,
            MAX_INPUT_LENGTH,
            modelId,
            parseOptions(options),
            historyCovs,
            futureCovs,
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
    return new TimechoForecastTableFunctionHandle();
  }

  @Override
  public TableFunctionProcessorProvider getProcessorProvider(
      TableFunctionHandle tableFunctionHandle) {
    return new TableFunctionProcessorProvider() {
      @Override
      public TableFunctionDataProcessor getDataProcessor() {
        return new TimechoForecastDataProcessor(
            (TimechoForecastTableFunctionHandle) tableFunctionHandle);
      }
    };
  }

  private static class TimechoForecastDataProcessor extends ForecastDataProcessor {
    private final String historyCovs;
    private final String futureCovs;
    private final boolean autoAdapt;

    public TimechoForecastDataProcessor(TimechoForecastTableFunctionHandle functionHandle) {
      super(functionHandle);
      this.historyCovs = functionHandle.historyCovs;
      this.futureCovs = functionHandle.futureCovs;
      this.autoAdapt = functionHandle.autoAdapt;
    }

    @Override
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
      try {
        resp =
            TableUDFUtils.getTableFunctionAINodeService()
                .forecast(
                    new TForecastReq(modelId, SERDE.serialize(inputData), outputLength)
                        .setHistoryCovs(historyCovs)
                        .setFutureCovs(futureCovs)
                        .setAutoAdapt(autoAdapt)
                        .setOptions(options));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }

      if (resp.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        String message =
            String.format(
                "Error occurred while executing forecast:[%s]", resp.getStatus().getMessage());
        throw new IoTDBRuntimeException(message, resp.getStatus().getCode());
      }

      TsBlock res = SERDE.deserialize(resp.forecastResult.get(0));
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
