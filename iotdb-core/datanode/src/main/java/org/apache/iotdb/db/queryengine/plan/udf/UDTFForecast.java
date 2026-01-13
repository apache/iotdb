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

package org.apache.iotdb.db.queryengine.plan.udf;

import org.apache.iotdb.ainode.rpc.thrift.TForecastReq;
import org.apache.iotdb.ainode.rpc.thrift.TForecastResp;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.exception.IoTDBRuntimeException;
import org.apache.iotdb.db.protocol.client.an.AINodeClient;
import org.apache.iotdb.db.protocol.client.an.AINodeClientManager;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.udf.api.UDTF;
import org.apache.iotdb.udf.api.access.Row;
import org.apache.iotdb.udf.api.collector.PointCollector;
import org.apache.iotdb.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.udf.api.customizer.strategy.RowByRowAccessStrategy;
import org.apache.iotdb.udf.api.type.Type;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.block.column.TsBlockSerde;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class UDTFForecast implements UDTF {
  private static final TsBlockSerde serde = new TsBlockSerde();
  private static final IClientManager<Integer, AINodeClient> CLIENT_MANAGER =
      AINodeClientManager.getInstance();
  private String model_id;
  private int maxInputLength;
  private int outputLength;
  private long outputStartTime;
  private long outputInterval;
  private boolean keepInput;
  Map<String, String> options;
  List<Type> types;
  private LinkedList<Row> inputRows;
  private TsBlockBuilder inputTsBlockBuilder;

  private static final Set<Type> ALLOWED_INPUT_TYPES = new HashSet<>();

  static {
    ALLOWED_INPUT_TYPES.add(Type.INT32);
    ALLOWED_INPUT_TYPES.add(Type.INT64);
    ALLOWED_INPUT_TYPES.add(Type.FLOAT);
    ALLOWED_INPUT_TYPES.add(Type.DOUBLE);
  }

  private static final String MODEL_ID_PARAMETER_NAME = "MODEL_ID";
  private static final String OUTPUT_LENGTH_PARAMETER_NAME = "OUTPUT_LENGTH";
  private static final int DEFAULT_OUTPUT_LENGTH = 96;
  private static final String OUTPUT_START_TIME = "OUTPUT_START_TIME";
  public static final long DEFAULT_OUTPUT_START_TIME = Long.MIN_VALUE;
  private static final String OUTPUT_INTERVAL = "OUTPUT_INTERVAL";
  public static final long DEFAULT_OUTPUT_INTERVAL = 0L;
  private static final String KEEP_INPUT_PARAMETER_NAME = "PRESERVE_INPUT";
  private static final Boolean DEFAULT_KEEP_INPUT = Boolean.FALSE;
  private static final String OPTIONS_PARAMETER_NAME = "MODEL_OPTIONS";
  private static final String DEFAULT_OPTIONS = "";
  private static final int MAX_INPUT_LENGTH = 2880;

  private void checkType() {
    for (Type type : this.types) {
      if (!ALLOWED_INPUT_TYPES.contains(type)) {
        throw new IllegalArgumentException(
            String.format(
                "Input data type %s is not supported, only %s are allowed.",
                type, ALLOWED_INPUT_TYPES));
      }
    }
  }

  @Override
  public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations)
      throws Exception {
    this.types = parameters.getDataTypes();
    checkType();
    configurations.setAccessStrategy(new RowByRowAccessStrategy()).setOutputDataType(Type.DOUBLE);

    this.model_id = parameters.getString(MODEL_ID_PARAMETER_NAME);
    if (this.model_id == null || this.model_id.isEmpty()) {
      throw new IllegalArgumentException(
          "MODEL_ID parameter must be provided and cannot be empty.");
    }
    this.maxInputLength = MAX_INPUT_LENGTH;

    this.outputInterval = parameters.getLongOrDefault(OUTPUT_INTERVAL, DEFAULT_OUTPUT_INTERVAL);
    this.outputLength =
        parameters.getIntOrDefault(OUTPUT_LENGTH_PARAMETER_NAME, DEFAULT_OUTPUT_LENGTH);
    this.outputStartTime =
        parameters.getLongOrDefault(OUTPUT_START_TIME, DEFAULT_OUTPUT_START_TIME);
    this.keepInput = parameters.getBooleanOrDefault(KEEP_INPUT_PARAMETER_NAME, DEFAULT_KEEP_INPUT);
    this.options =
        Arrays.stream(
                parameters.getStringOrDefault(OPTIONS_PARAMETER_NAME, DEFAULT_OPTIONS).split(","))
            .map(s -> s.split("="))
            .filter(arr -> arr.length == 2 && !arr[0].isEmpty()) // 防御性检查
            .collect(
                Collectors.toMap(
                    arr -> arr[0].trim(), arr -> arr[1].trim(), (v1, v2) -> v2 // 如果 key 重复，保留后一个
                    ));
    this.inputRows = new LinkedList<>();
    List<TSDataType> tsDataTypeList = new ArrayList<>(this.types.size() - 1);
    for (int i = 0; i < this.types.size(); i++) {
      tsDataTypeList.add(TSDataType.DOUBLE);
    }
    this.inputTsBlockBuilder = new TsBlockBuilder(tsDataTypeList);
  }

  private void setByType(Row row, PointCollector collector) throws IOException {
    for (int i = 0; i < row.size(); i++) {
      switch (this.types.get(i)) {
        case INT32:
          collector.putInt(row.getTime(), row.getInt(i));
          break;
        case INT64:
          collector.putLong(row.getTime(), row.getLong(i));
          break;
        case FLOAT:
          collector.putFloat(row.getTime(), row.getFloat(i));
          break;
        case DOUBLE:
          collector.putDouble(row.getTime(), row.getDouble(i));
          break;
        default:
          throw new IllegalArgumentException(
              String.format("Unsupported data type %s", this.types.get(i + 1)));
      }
    }
  }

  private void setByType(Row row, TsBlockBuilder tsBlockBuilder) throws IOException {
    for (int i = 0; i < row.size(); i++) {
      if (row.isNull(i)) {
        tsBlockBuilder.getColumnBuilder(i).appendNull();
        continue;
      }
      switch (this.types.get(i)) {
        case INT32:
          tsBlockBuilder.getColumnBuilder(i).writeInt(row.getInt(i));
          break;
        case INT64:
          tsBlockBuilder.getColumnBuilder(i).writeLong(row.getLong(i));
          break;
        case FLOAT:
          tsBlockBuilder.getColumnBuilder(i).writeFloat(row.getFloat(i));
          break;
        case DOUBLE:
          tsBlockBuilder.getColumnBuilder(i).writeDouble(row.getDouble(i));
          break;
        default:
          throw new IllegalArgumentException(
              String.format("Unsupported data type %s", this.types.get(i + 1)));
      }
    }
  }

  @Override
  public void transform(Row row, PointCollector collector) throws Exception {
    if (this.keepInput) {
      setByType(row, collector);
    }

    if (maxInputLength != 0 && inputRows.size() >= maxInputLength) {
      // If the input rows exceed the maximum length, remove the oldest row
      inputRows.removeFirst();
    }
    inputRows.add(row);
  }

  private TsBlock forecast() throws Exception {
    // Build the input data which will be sent to AINode
    while (!inputRows.isEmpty()) {
      Row row = inputRows.removeFirst();
      inputTsBlockBuilder.getTimeColumnBuilder().writeLong(row.getTime());
      setByType(row, inputTsBlockBuilder);
      inputTsBlockBuilder.declarePosition();
    }

    TsBlock inputData = inputTsBlockBuilder.build();

    TForecastResp resp;
    try (AINodeClient client =
        CLIENT_MANAGER.borrowClient(AINodeClientManager.AINODE_ID_PLACEHOLDER)) {
      resp =
          client.forecast(
              new TForecastReq(model_id, serde.serialize(inputData), outputLength)
                  .setOptions(options));
    } catch (Exception e) {
      throw new IoTDBRuntimeException(
          e.getMessage(), TSStatusCode.CAN_NOT_CONNECT_AINODE.getStatusCode());
    }

    if (resp.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new IoTDBRuntimeException(
          String.format(
              "Forecast failed due to %d %s",
              resp.getStatus().getCode(), resp.getStatus().getMessage()),
          resp.getStatus().getCode());
    }
    return serde.deserialize(ByteBuffer.wrap(resp.getForecastResult()));
  }

  @Override
  public void terminate(PointCollector collector) throws Exception {
    long inputStartTime = inputRows.get(0).getTime();
    long inputEndTime = inputRows.get(inputRows.size() - 1).getTime();
    if (inputStartTime > inputEndTime) {
      throw new IllegalArgumentException(
          String.format(
              "input end time should never less than start time, start time is %s, end time is %s",
              inputStartTime, inputEndTime));
    }
    long interval = this.outputInterval;
    if (outputInterval <= 0) {
      interval = (inputEndTime - inputStartTime) / (inputRows.size() - 1);
    }
    long outputTime =
        (this.outputStartTime == Long.MIN_VALUE) ? inputEndTime + interval : this.outputStartTime;
    long[] outputTimes = new long[this.outputLength];
    for (int i = 0; i < this.outputLength; i++) {
      outputTimes[i] = outputTime + interval * i;
    }

    TsBlock forecastResult = forecast();
    if (forecastResult.getPositionCount() != this.outputLength) {
      throw new IllegalArgumentException(
          String.format(
              "The forecast result length %d does not match the expected output length %d",
              forecastResult.getPositionCount(), this.outputLength));
    }
    if (forecastResult.getValueColumnCount() != 1) {
      throw new IllegalArgumentException(
          String.format(
              "The forecast result should have only one value column, but got %d",
              forecastResult.getValueColumnCount()));
    }

    for (int i = 0; i < forecastResult.getPositionCount(); i++) {
      collector.putDouble(outputTimes[i], forecastResult.getValueColumns()[0].getDouble(i));
    }
  }
}
