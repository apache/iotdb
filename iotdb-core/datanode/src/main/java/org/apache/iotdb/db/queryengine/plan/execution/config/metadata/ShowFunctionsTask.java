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

package org.apache.iotdb.db.queryengine.plan.execution.config.metadata;

import org.apache.iotdb.common.rpc.thrift.Model;
import org.apache.iotdb.commons.udf.UDFInformation;
import org.apache.iotdb.commons.udf.UDFType;
import org.apache.iotdb.commons.udf.builtin.BuiltinAggregationFunction;
import org.apache.iotdb.commons.udf.builtin.BuiltinScalarFunction;
import org.apache.iotdb.commons.udf.builtin.BuiltinTimeSeriesGeneratingFunction;
import org.apache.iotdb.commons.udf.utils.TreeUDFUtils;
import org.apache.iotdb.db.queryengine.common.header.ColumnHeader;
import org.apache.iotdb.db.queryengine.common.header.ColumnHeaderConstant;
import org.apache.iotdb.db.queryengine.common.header.DatasetHeader;
import org.apache.iotdb.db.queryengine.common.header.DatasetHeaderFactory;
import org.apache.iotdb.db.queryengine.plan.execution.config.ConfigTaskResult;
import org.apache.iotdb.db.queryengine.plan.execution.config.IConfigTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.executor.IConfigTaskExecutor;
import org.apache.iotdb.rpc.TSStatusCode;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BytesUtils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.iotdb.commons.conf.IoTDBConstant.FUNCTION_STATE_AVAILABLE;
import static org.apache.iotdb.commons.conf.IoTDBConstant.FUNCTION_STATE_UNAVAILABLE;
import static org.apache.iotdb.commons.conf.IoTDBConstant.FUNCTION_TYPE_BUILTIN_SCALAR;
import static org.apache.iotdb.commons.conf.IoTDBConstant.FUNCTION_TYPE_BUILTIN_UDTF;
import static org.apache.iotdb.commons.conf.IoTDBConstant.FUNCTION_TYPE_EXTERNAL_UDAF;
import static org.apache.iotdb.commons.conf.IoTDBConstant.FUNCTION_TYPE_EXTERNAL_UDTF;
import static org.apache.iotdb.commons.conf.IoTDBConstant.FUNCTION_TYPE_NATIVE;
import static org.apache.iotdb.commons.conf.IoTDBConstant.FUNCTION_TYPE_UNKNOWN;

public class ShowFunctionsTask implements IConfigTask {

  private static final Map<String, Binary> BINARY_MAP = new HashMap<>();

  static {
    BINARY_MAP.put(FUNCTION_TYPE_NATIVE, BytesUtils.valueOf(FUNCTION_TYPE_NATIVE));
    BINARY_MAP.put(FUNCTION_TYPE_BUILTIN_UDTF, BytesUtils.valueOf(FUNCTION_TYPE_BUILTIN_UDTF));
    BINARY_MAP.put(FUNCTION_TYPE_EXTERNAL_UDTF, BytesUtils.valueOf(FUNCTION_TYPE_EXTERNAL_UDTF));
    BINARY_MAP.put(FUNCTION_TYPE_EXTERNAL_UDAF, BytesUtils.valueOf(FUNCTION_TYPE_EXTERNAL_UDAF));
    BINARY_MAP.put(FUNCTION_TYPE_BUILTIN_SCALAR, BytesUtils.valueOf(FUNCTION_TYPE_BUILTIN_SCALAR));
    BINARY_MAP.put(FUNCTION_TYPE_UNKNOWN, BytesUtils.valueOf(FUNCTION_TYPE_UNKNOWN));
    BINARY_MAP.put(FUNCTION_STATE_AVAILABLE, BytesUtils.valueOf(FUNCTION_STATE_AVAILABLE));
    BINARY_MAP.put(FUNCTION_STATE_UNAVAILABLE, BytesUtils.valueOf(FUNCTION_STATE_UNAVAILABLE));
    BINARY_MAP.put("", BytesUtils.valueOf(""));
  }

  private final Model model;

  public ShowFunctionsTask(Model model) {
    this.model = model;
  }

  @Override
  public ListenableFuture<ConfigTaskResult> execute(IConfigTaskExecutor configTaskExecutor) {
    return configTaskExecutor.showFunctions(model);
  }

  public static void buildTsBlock(
      List<ByteBuffer> allUDFInformation, SettableFuture<ConfigTaskResult> future) {
    List<TSDataType> outputDataTypes =
        ColumnHeaderConstant.showFunctionsColumnHeaders.stream()
            .map(ColumnHeader::getColumnType)
            .collect(Collectors.toList());
    TsBlockBuilder builder = new TsBlockBuilder(outputDataTypes);
    List<UDFInformation> udfInformations = new ArrayList<>();
    if (allUDFInformation != null && !allUDFInformation.isEmpty()) {
      for (ByteBuffer udfInformationByteBuffer : allUDFInformation) {
        UDFInformation udfInformation = UDFInformation.deserialize(udfInformationByteBuffer);
        udfInformations.add(udfInformation);
      }
    }

    udfInformations.sort(Comparator.comparing(UDFInformation::getFunctionName));
    appendBuiltInTimeSeriesGeneratingFunctions(builder);
    for (UDFInformation udfInformation : udfInformations) {
      appendUDFInformation(builder, udfInformation);
    }
    appendBuiltInAggregationFunctions(builder);
    appendBuiltInScalarFunctions(builder);
    DatasetHeader datasetHeader = DatasetHeaderFactory.getShowFunctionsHeader();
    future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS, builder.build(), datasetHeader));
  }

  private static void appendUDFInformation(TsBlockBuilder builder, UDFInformation udfInformation) {
    builder.getTimeColumnBuilder().writeLong(0L);
    builder.getColumnBuilder(0).writeBinary(BytesUtils.valueOf(udfInformation.getFunctionName()));
    builder.getColumnBuilder(1).writeBinary(getFunctionType(udfInformation));
    builder.getColumnBuilder(2).writeBinary(BytesUtils.valueOf(udfInformation.getClassName()));
    builder.getColumnBuilder(3).writeBinary(getFunctionState(udfInformation));

    builder.declarePosition();
  }

  private static void appendBuiltInTimeSeriesGeneratingFunctions(TsBlockBuilder builder) {
    final Binary functionType = BINARY_MAP.get(FUNCTION_TYPE_BUILTIN_UDTF);
    final Binary functionState = BINARY_MAP.get(FUNCTION_STATE_AVAILABLE);
    for (BuiltinTimeSeriesGeneratingFunction function :
        BuiltinTimeSeriesGeneratingFunction.values()) {
      builder.getTimeColumnBuilder().writeLong(0L);
      builder
          .getColumnBuilder(0)
          .writeBinary(BytesUtils.valueOf(function.getFunctionName().toUpperCase()));
      builder.getColumnBuilder(1).writeBinary(functionType);
      builder.getColumnBuilder(2).writeBinary(BytesUtils.valueOf(function.getClassName()));
      builder.getColumnBuilder(3).writeBinary(functionState);
      builder.declarePosition();
    }
  }

  private static void appendBuiltInAggregationFunctions(TsBlockBuilder builder) {
    final Binary functionType = BINARY_MAP.get(FUNCTION_TYPE_NATIVE);
    final Binary functionState = BINARY_MAP.get(FUNCTION_STATE_AVAILABLE);
    final Binary className = BINARY_MAP.get("");
    for (String functionName : BuiltinAggregationFunction.getNativeFunctionNames()) {
      builder.getTimeColumnBuilder().writeLong(0L);
      builder.getColumnBuilder(0).writeBinary(BytesUtils.valueOf(functionName.toUpperCase()));
      builder.getColumnBuilder(1).writeBinary(functionType);
      builder.getColumnBuilder(2).writeBinary(className);
      builder.getColumnBuilder(3).writeBinary(functionState);
      builder.declarePosition();
    }
  }

  private static void appendBuiltInScalarFunctions(TsBlockBuilder builder) {
    final Binary functionType = BINARY_MAP.get(FUNCTION_TYPE_BUILTIN_SCALAR);
    final Binary functionState = BINARY_MAP.get(FUNCTION_STATE_AVAILABLE);
    final Binary className = BINARY_MAP.get("");
    for (String functionName : BuiltinScalarFunction.getNativeFunctionNames()) {
      builder.getTimeColumnBuilder().writeLong(0L);
      builder.getColumnBuilder(0).writeBinary(BytesUtils.valueOf(functionName.toUpperCase()));
      builder.getColumnBuilder(1).writeBinary(functionType);
      builder.getColumnBuilder(2).writeBinary(className);
      builder.getColumnBuilder(3).writeBinary(functionState);
      builder.declarePosition();
    }
  }

  private static Binary getFunctionType(UDFInformation udfInformation) {
    UDFType type = udfInformation.getUdfType();
    if (udfInformation.isAvailable()) {
      if (type.isTreeModel()) {
        if (TreeUDFUtils.isUDTF(udfInformation.getFunctionName())) {
          return BINARY_MAP.get(FUNCTION_TYPE_EXTERNAL_UDTF);
        } else if (TreeUDFUtils.isUDAF(udfInformation.getFunctionName())) {
          return BINARY_MAP.get(FUNCTION_TYPE_EXTERNAL_UDAF);
        }
      }
    }
    return BINARY_MAP.get(FUNCTION_TYPE_UNKNOWN);
  }

  private static Binary getFunctionState(UDFInformation udfInformation) {
    if (udfInformation.isAvailable()) {
      return BINARY_MAP.get(FUNCTION_STATE_AVAILABLE);
    } else {
      return BINARY_MAP.get(FUNCTION_STATE_UNAVAILABLE);
    }
  }
}
