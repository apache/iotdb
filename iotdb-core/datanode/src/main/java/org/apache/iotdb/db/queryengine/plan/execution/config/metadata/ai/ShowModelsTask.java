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

package org.apache.iotdb.db.queryengine.plan.execution.config.metadata.ai;

import org.apache.iotdb.commons.schema.column.ColumnHeader;
import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.confignode.rpc.thrift.TShowModelResp;
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
import org.apache.tsfile.utils.BytesUtils;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ShowModelsTask implements IConfigTask {

  private final String modelName;

  public ShowModelsTask(String modelName) {
    this.modelName = modelName;
  }

  public static final String INPUT_SHAPE = "inputShape:";
  public static final String OUTPUT_SHAPE = "outputShape:";
  public static final String INPUT_DATA_TYPE = "inputDataType:";
  public static final String OUTPUT_DATA_TYPE = "outputDataType:";
  private static final String EMPTY_STRING = "";

  @Override
  public ListenableFuture<ConfigTaskResult> execute(IConfigTaskExecutor configTaskExecutor)
      throws InterruptedException {
    return configTaskExecutor.showModels(modelName);
  }

  public static void buildTsBlock(TShowModelResp resp, SettableFuture<ConfigTaskResult> future) {
    List<String> modelIdList = resp.getModelIdList();
    Map<String, String> modelTypeMap = resp.getModelTypeMap();
    Map<String, String> categoryMap = resp.getCategoryMap();
    Map<String, String> stateMap = resp.getStateMap();
    List<TSDataType> outputDataTypes =
        ColumnHeaderConstant.showModelsColumnHeaders.stream()
            .map(ColumnHeader::getColumnType)
            .collect(Collectors.toList());
    TsBlockBuilder builder = new TsBlockBuilder(outputDataTypes);
    for (String modelId : modelIdList) {
      //      String note;
      //      String config;
      //      if (Objects.equals(modelType, ModelType.USER_DEFINED.toString())) {
      //        String inputShape = ReadWriteIOUtils.readString(modelInfo);
      //        String outputShape = ReadWriteIOUtils.readString(modelInfo);
      //        String inputTypes = ReadWriteIOUtils.readString(modelInfo);
      //        String outputTypes = ReadWriteIOUtils.readString(modelInfo);
      //        note = ReadWriteIOUtils.readString(modelInfo);
      //        config =
      //            INPUT_SHAPE
      //                + inputShape
      //                + OUTPUT_SHAPE
      //                + outputShape
      //                + INPUT_DATA_TYPE
      //                + inputTypes
      //                + OUTPUT_DATA_TYPE
      //                + outputTypes;
      //      } else {
      //        config = EMPTY_STRING;
      //        note = "Built-in model in IoTDB";
      //      }
      builder.getTimeColumnBuilder().writeLong(0L);
      builder.getColumnBuilder(0).writeBinary(BytesUtils.valueOf(modelId));
      builder.getColumnBuilder(1).writeBinary(BytesUtils.valueOf(modelTypeMap.get(modelId)));
      builder.getColumnBuilder(2).writeBinary(BytesUtils.valueOf(categoryMap.get(modelId)));
      builder.getColumnBuilder(3).writeBinary(BytesUtils.valueOf(stateMap.get(modelId)));
      builder.declarePosition();
    }
    DatasetHeader datasetHeader = DatasetHeaderFactory.getShowModelsHeader();
    future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS, builder.build(), datasetHeader));
  }
}
