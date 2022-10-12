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

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.udf.builtin.BuiltinAggregationFunction;
import org.apache.iotdb.commons.udf.service.UDFRegistrationInformation;
import org.apache.iotdb.commons.udf.service.UDFRegistrationService;
import org.apache.iotdb.db.mpp.common.header.ColumnHeader;
import org.apache.iotdb.db.mpp.common.header.ColumnHeaderConstant;
import org.apache.iotdb.db.mpp.common.header.DatasetHeaderFactory;
import org.apache.iotdb.db.mpp.plan.execution.config.ConfigTaskResult;
import org.apache.iotdb.db.mpp.plan.execution.config.IConfigTask;
import org.apache.iotdb.db.mpp.plan.execution.config.executor.IConfigTaskExecutor;
import org.apache.iotdb.db.query.dataset.ListDataSet;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.utils.Binary;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.iotdb.commons.conf.IoTDBConstant.COLUMN_FUNCTION_CLASS;
import static org.apache.iotdb.commons.conf.IoTDBConstant.COLUMN_FUNCTION_NAME;
import static org.apache.iotdb.commons.conf.IoTDBConstant.COLUMN_FUNCTION_TYPE;
import static org.apache.iotdb.commons.conf.IoTDBConstant.FUNCTION_TYPE_BUILTIN_UDAF;
import static org.apache.iotdb.commons.conf.IoTDBConstant.FUNCTION_TYPE_BUILTIN_UDTF;
import static org.apache.iotdb.commons.conf.IoTDBConstant.FUNCTION_TYPE_EXTERNAL_UDAF;
import static org.apache.iotdb.commons.conf.IoTDBConstant.FUNCTION_TYPE_EXTERNAL_UDTF;
import static org.apache.iotdb.commons.conf.IoTDBConstant.FUNCTION_TYPE_NATIVE;

public class ShowFunctionsTask implements IConfigTask {

  private static final Logger LOGGER = LoggerFactory.getLogger(ShowFunctionsTask.class);

  @Override
  public ListenableFuture<ConfigTaskResult> execute(IConfigTaskExecutor configTaskExecutor)
      throws InterruptedException {
    final SettableFuture<ConfigTaskResult> future = SettableFuture.create();

    try {
      List<TSDataType> outputDataTypes =
          ColumnHeaderConstant.showFunctionsColumnHeaders.stream()
              .map(ColumnHeader::getColumnType)
              .collect(Collectors.toList());
      TsBlockBuilder builder = new TsBlockBuilder(outputDataTypes);

      final QueryDataSet showDataSet = generateShowFunctionsListDataSet();
      while (showDataSet.hasNextWithoutConstraint()) {
        final RowRecord record = showDataSet.nextWithoutConstraint();

        builder.getTimeColumnBuilder().writeLong(0);
        for (int i = 0; i < 3; ++i) {
          builder.getColumnBuilder(i).writeBinary(record.getFields().get(i).getBinaryV());
        }
        builder.declarePosition();
      }

      future.set(
          new ConfigTaskResult(
              TSStatusCode.SUCCESS_STATUS,
              builder.build(),
              DatasetHeaderFactory.getShowFunctionsHeader()));
    } catch (Exception e) {
      LOGGER.error("Failed to get functions.", e);
      future.setException(e);
    }

    return future;
  }

  private QueryDataSet generateShowFunctionsListDataSet() {
    ListDataSet listDataSet =
        new ListDataSet(
            Arrays.asList(
                new PartialPath(COLUMN_FUNCTION_NAME, false),
                new PartialPath(COLUMN_FUNCTION_TYPE, false),
                new PartialPath(COLUMN_FUNCTION_CLASS, false)),
            Arrays.asList(TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT));

    appendUDFs(listDataSet);
    appendNativeFunctions(listDataSet);

    listDataSet.sort(
        (r1, r2) ->
            String.CASE_INSENSITIVE_ORDER.compare(
                r1.getFields().get(0).getStringValue(), r2.getFields().get(0).getStringValue()));
    return listDataSet;
  }

  private void appendUDFs(ListDataSet listDataSet) {
    for (UDFRegistrationInformation info :
        UDFRegistrationService.getInstance().getRegistrationInformation()) {
      RowRecord rowRecord = new RowRecord(0); // ignore timestamp
      rowRecord.addField(Binary.valueOf(info.getFunctionName()), TSDataType.TEXT);
      String functionType = "";
      try {
        if (info.isBuiltin()) {
          if (info.isUDTF()) {
            functionType = FUNCTION_TYPE_BUILTIN_UDTF;
          } else if (info.isUDAF()) {
            functionType = FUNCTION_TYPE_BUILTIN_UDAF;
          }
        } else {
          if (info.isUDTF()) {
            functionType = FUNCTION_TYPE_EXTERNAL_UDTF;
          } else if (info.isUDAF()) {
            functionType = FUNCTION_TYPE_EXTERNAL_UDAF;
          }
        }
      } catch (InstantiationException
          | InvocationTargetException
          | NoSuchMethodException
          | IllegalAccessException e) {
        throw new RuntimeException(e.toString());
      }
      rowRecord.addField(Binary.valueOf(functionType), TSDataType.TEXT);
      rowRecord.addField(Binary.valueOf(info.getClassName()), TSDataType.TEXT);
      listDataSet.putRecord(rowRecord);
    }
  }

  private void appendNativeFunctions(ListDataSet listDataSet) {
    final Binary functionType = Binary.valueOf(FUNCTION_TYPE_NATIVE);
    final Binary className = Binary.valueOf("");
    for (String functionName : BuiltinAggregationFunction.getNativeFunctionNames()) {
      RowRecord rowRecord = new RowRecord(0); // ignore timestamp
      rowRecord.addField(Binary.valueOf(functionName.toUpperCase()), TSDataType.TEXT);
      rowRecord.addField(functionType, TSDataType.TEXT);
      rowRecord.addField(className, TSDataType.TEXT);
      listDataSet.putRecord(rowRecord);
    }
  }
}
