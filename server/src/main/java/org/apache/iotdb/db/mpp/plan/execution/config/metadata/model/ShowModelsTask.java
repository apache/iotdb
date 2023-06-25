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

package org.apache.iotdb.db.mpp.plan.execution.config.metadata.model;

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
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class ShowModelsTask implements IConfigTask {

  public ShowModelsTask() {
    // do nothing
  }

  @Override
  public ListenableFuture<ConfigTaskResult> execute(IConfigTaskExecutor configTaskExecutor)
      throws InterruptedException {
    return configTaskExecutor.showModels();
  }

  public static void buildTsBlock(
      List<ByteBuffer> modelInfoList, SettableFuture<ConfigTaskResult> future) {
    List<TSDataType> outputDataTypes =
        ColumnHeaderConstant.showModelsColumnHeaders.stream()
            .map(ColumnHeader::getColumnType)
            .collect(Collectors.toList());
    TsBlockBuilder builder = new TsBlockBuilder(outputDataTypes);
    for (ByteBuffer modelInfo : modelInfoList) {
      String modelId = ReadWriteIOUtils.readString(modelInfo);
      String modelTask = ReadWriteIOUtils.readString(modelInfo);
      String modelType = ReadWriteIOUtils.readString(modelInfo);
      String queryBody = ReadWriteIOUtils.readString(modelInfo);
      String trainingState = ReadWriteIOUtils.readString(modelInfo);
      String modelPath = ReadWriteIOUtils.readString(modelInfo);

      int listSize = ReadWriteIOUtils.readInt(modelInfo);
      List<String> modelHyperparameter = new ArrayList<>();
      for (int i = 0; i < listSize; i++) {
        modelHyperparameter.add(ReadWriteIOUtils.readString(modelInfo));
      }

      builder.getTimeColumnBuilder().writeLong(0L);
      builder.getColumnBuilder(0).writeBinary(Binary.valueOf(modelId));
      builder.getColumnBuilder(1).writeBinary(Binary.valueOf(modelTask));
      builder.getColumnBuilder(2).writeBinary(Binary.valueOf(modelType));
      builder.getColumnBuilder(3).writeBinary(Binary.valueOf(queryBody));
      builder.getColumnBuilder(4).writeBinary(Binary.valueOf(trainingState));
      builder.getColumnBuilder(5).writeBinary(Binary.valueOf(modelPath));
      builder.getColumnBuilder(6).writeBinary(Binary.valueOf(modelHyperparameter.get(0)));
      builder.declarePosition();

      for (int i = 1; i < listSize; i++) {
        builder.getTimeColumnBuilder().writeLong(0L);
        for (int columnIndex = 0; columnIndex <= 5; columnIndex++) {
          builder.getColumnBuilder(columnIndex).writeBinary(Binary.valueOf(""));
        }
        builder.getColumnBuilder(6).writeBinary(Binary.valueOf(modelHyperparameter.get(i)));
        builder.declarePosition();
      }
    }
    DatasetHeader datasetHeader = DatasetHeaderFactory.getShowModelsHeader();
    future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS, builder.build(), datasetHeader));
  }
}
