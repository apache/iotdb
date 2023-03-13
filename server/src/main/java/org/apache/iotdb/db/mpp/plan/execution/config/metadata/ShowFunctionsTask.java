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

package org.apache.iotdb.db.mpp.plan.execution.config.metadata;

import org.apache.iotdb.commons.udf.UDFInformation;
import org.apache.iotdb.commons.udf.builtin.BuiltinAggregationFunction;
import org.apache.iotdb.commons.udf.builtin.BuiltinScalarFunction;
import org.apache.iotdb.commons.udf.service.UDFManagementService;
import org.apache.iotdb.db.mpp.common.header.ColumnHeader;
import org.apache.iotdb.db.mpp.common.header.ColumnHeaderConstant;
import org.apache.iotdb.db.mpp.common.header.DatasetHeader;
import org.apache.iotdb.db.mpp.common.header.DatasetHeaderFactory;
import org.apache.iotdb.db.mpp.plan.execution.config.ConfigTaskResult;
import org.apache.iotdb.db.mpp.plan.execution.config.IConfigTask;
import org.apache.iotdb.db.mpp.plan.execution.config.executor.IConfigTaskExecutor;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.utils.Binary;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.iotdb.commons.conf.IoTDBConstant.FUNCTION_TYPE_BUILTIN_SCALAR;
import static org.apache.iotdb.commons.conf.IoTDBConstant.FUNCTION_TYPE_BUILTIN_UDAF;
import static org.apache.iotdb.commons.conf.IoTDBConstant.FUNCTION_TYPE_BUILTIN_UDTF;
import static org.apache.iotdb.commons.conf.IoTDBConstant.FUNCTION_TYPE_EXTERNAL_UDAF;
import static org.apache.iotdb.commons.conf.IoTDBConstant.FUNCTION_TYPE_EXTERNAL_UDTF;
import static org.apache.iotdb.commons.conf.IoTDBConstant.FUNCTION_TYPE_NATIVE;

public class ShowFunctionsTask implements IConfigTask {

  @Override
  public ListenableFuture<ConfigTaskResult> execute(IConfigTaskExecutor configTaskExecutor) {
    return configTaskExecutor.showFunctions();
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
    // native and built-in functions
    udfInformations.addAll(
        UDFManagementService.getInstance().getAllBuiltInTimeSeriesGeneratingInformation());

    udfInformations.sort(Comparator.comparing(UDFInformation::getFunctionName));
    for (UDFInformation udfInformation : udfInformations) {
      appendUDFInformation(builder, udfInformation);
    }
    appendNativeFunctions(builder);
    appendBuiltInScalarFunctions(builder);
    DatasetHeader datasetHeader = DatasetHeaderFactory.getShowFunctionsHeader();
    future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS, builder.build(), datasetHeader));
  }

  private static void appendUDFInformation(TsBlockBuilder builder, UDFInformation udfInformation) {
    builder.getTimeColumnBuilder().writeLong(0L);
    builder.getColumnBuilder(0).writeBinary(Binary.valueOf(udfInformation.getFunctionName()));
    builder.getColumnBuilder(1).writeBinary(Binary.valueOf(getFunctionType(udfInformation)));
    builder.getColumnBuilder(2).writeBinary(Binary.valueOf(udfInformation.getClassName()));
    builder.declarePosition();
  }

  private static void appendNativeFunctions(TsBlockBuilder builder) {
    final Binary functionType = Binary.valueOf(FUNCTION_TYPE_NATIVE);
    final Binary className = Binary.valueOf("");
    for (String functionName : BuiltinAggregationFunction.getNativeFunctionNames()) {
      builder.getTimeColumnBuilder().writeLong(0L);
      builder.getColumnBuilder(0).writeBinary(Binary.valueOf(functionName.toUpperCase()));
      builder.getColumnBuilder(1).writeBinary(functionType);
      builder.getColumnBuilder(2).writeBinary(className);
      builder.declarePosition();
    }
  }

  private static void appendBuiltInScalarFunctions(TsBlockBuilder builder) {
    final Binary functionType = Binary.valueOf(FUNCTION_TYPE_BUILTIN_SCALAR);
    final Binary className = Binary.valueOf("");
    for (String functionName : BuiltinScalarFunction.getNativeFunctionNames()) {
      builder.getTimeColumnBuilder().writeLong(0L);
      builder.getColumnBuilder(0).writeBinary(Binary.valueOf(functionName.toUpperCase()));
      builder.getColumnBuilder(1).writeBinary(functionType);
      builder.getColumnBuilder(2).writeBinary(className);
      builder.declarePosition();
    }
  }

  private static String getFunctionType(UDFInformation udfInformation) {
    String functionType = FUNCTION_TYPE_EXTERNAL_UDTF;
    try {
      if (udfInformation.isBuiltin()) {
        if (UDFManagementService.getInstance().isUDTF(udfInformation.getFunctionName())) {
          functionType = FUNCTION_TYPE_BUILTIN_UDTF;
        } else if (UDFManagementService.getInstance().isUDAF(udfInformation.getFunctionName())) {
          functionType = FUNCTION_TYPE_BUILTIN_UDAF;
        }
      } else {
        if (UDFManagementService.getInstance().isUDTF(udfInformation.getFunctionName())) {
          functionType = FUNCTION_TYPE_EXTERNAL_UDTF;
        } else if (UDFManagementService.getInstance().isUDAF(udfInformation.getFunctionName())) {
          functionType = FUNCTION_TYPE_EXTERNAL_UDAF;
        }
      }
    } catch (InstantiationException
        | InvocationTargetException
        | NoSuchMethodException
        | IllegalAccessException ignore) {
    }
    return functionType;
  }
}
