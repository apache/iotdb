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

import org.apache.iotdb.commons.trigger.TriggerInformation;
import org.apache.iotdb.db.mpp.common.header.ColumnHeader;
import org.apache.iotdb.db.mpp.common.header.ColumnHeaderConstant;
import org.apache.iotdb.db.mpp.common.header.DatasetHeader;
import org.apache.iotdb.db.mpp.common.header.DatasetHeaderFactory;
import org.apache.iotdb.db.mpp.plan.execution.config.ConfigTaskResult;
import org.apache.iotdb.db.mpp.plan.execution.config.IConfigTask;
import org.apache.iotdb.db.mpp.plan.execution.config.executor.IConfigTaskExecutor;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.trigger.api.enums.TriggerType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.utils.Binary;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.stream.Collectors;

public class ShowTriggersTask implements IConfigTask {

  @Override
  public ListenableFuture<ConfigTaskResult> execute(IConfigTaskExecutor configTaskExecutor)
      throws InterruptedException {
    return configTaskExecutor.showTriggers();
  }

  public static void buildTsBlock(
      List<ByteBuffer> allTriggerInformation, SettableFuture<ConfigTaskResult> future) {
    List<TSDataType> outputDataTypes =
        ColumnHeaderConstant.showTriggersColumnHeaders.stream()
            .map(ColumnHeader::getColumnType)
            .collect(Collectors.toList());
    TsBlockBuilder builder = new TsBlockBuilder(outputDataTypes);
    if (allTriggerInformation != null && !allTriggerInformation.isEmpty()) {
      for (ByteBuffer triggerInformationByteBuffer : allTriggerInformation) {
        TriggerInformation triggerInformation =
            TriggerInformation.deserialize(triggerInformationByteBuffer);
        builder.getTimeColumnBuilder().writeLong(0L);
        builder
            .getColumnBuilder(0)
            .writeBinary(Binary.valueOf(triggerInformation.getTriggerName()));
        builder
            .getColumnBuilder(1)
            .writeBinary(Binary.valueOf(triggerInformation.getEvent().toString()));
        builder
            .getColumnBuilder(2)
            .writeBinary(
                Binary.valueOf(
                    triggerInformation.isStateful()
                        ? TriggerType.STATEFUL.toString()
                        : TriggerType.STATELESS.toString()));
        builder
            .getColumnBuilder(3)
            .writeBinary(Binary.valueOf(triggerInformation.getTriggerState().toString()));
        builder
            .getColumnBuilder(4)
            .writeBinary(Binary.valueOf(triggerInformation.getPathPattern().toString()));
        builder.getColumnBuilder(5).writeBinary(Binary.valueOf(triggerInformation.getClassName()));
        builder
            .getColumnBuilder(6)
            .writeBinary(
                Binary.valueOf(
                    !triggerInformation.isStateful()
                        ? "ALL"
                        : String.valueOf(
                            triggerInformation.getDataNodeLocation().getDataNodeId())));
        builder.declarePosition();
      }
    }
    DatasetHeader datasetHeader = DatasetHeaderFactory.getShowTriggersHeader();
    future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS, builder.build(), datasetHeader));
  }
}
