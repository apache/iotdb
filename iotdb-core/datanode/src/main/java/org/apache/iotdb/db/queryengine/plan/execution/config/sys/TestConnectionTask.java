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

package org.apache.iotdb.db.queryengine.plan.execution.config.sys;

import com.google.common.util.concurrent.SettableFuture;
import org.apache.iotdb.common.rpc.thrift.TTestConnectionResp;
import org.apache.iotdb.common.rpc.thrift.TTestConnectionResult;
import org.apache.iotdb.db.queryengine.common.header.ColumnHeader;
import org.apache.iotdb.db.queryengine.common.header.ColumnHeaderConstant;
import org.apache.iotdb.db.queryengine.plan.execution.config.ConfigTaskResult;
import org.apache.iotdb.db.queryengine.plan.execution.config.IConfigTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.executor.IConfigTaskExecutor;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.utils.Binary;

import java.util.List;
import java.util.stream.Collectors;

public class TestConnectionTask implements IConfigTask {

  @Override
  public ListenableFuture<ConfigTaskResult> execute(IConfigTaskExecutor configTaskExecutor)
      throws InterruptedException {
    return configTaskExecutor.testConnection();
  }

  public static void buildTSBlock(TTestConnectionResp resp, SettableFuture<ConfigTaskResult> future) {
    List<TSDataType> outputDataTypes =
            ColumnHeaderConstant.testConnectionColumnHeaders.stream()
                    .map(ColumnHeader::getColumnType)
                    .collect(Collectors.toList());
    TsBlockBuilder builder = new TsBlockBuilder(outputDataTypes);

    for (TTestConnectionResult result : resp.getResultList()) {
      builder.getTimeColumnBuilder().writeLong(0);
      builder.getColumnBuilder(0).writeBinary(new Binary(result.getServiceProvider().toString(), TSFileConfig.STRING_CHARSET));
      builder.getColumnBuilder(1).writeBinary(new Binary(result.getSender().toString(), TSFileConfig.STRING_CHARSET));
      String connectionStatus;
      if (result.isSuccess()) {
        connectionStatus = "up";
      } else {
        connectionStatus = "down" + " (" + result.getReason() + ")";
      }
      builder.getColumnBuilder(2).writeBinary(new Binary(connectionStatus, TSFileConfig.STRING_CHARSET));
      builder.declarePosition();
    }
  }
}
