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
package org.apache.iotdb.db.mpp.plan.execution.config;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.consensus.PartitionRegionId;
import org.apache.iotdb.confignode.rpc.thrift.TOperateReceiverPipeReq;
import org.apache.iotdb.db.client.ConfigNodeClient;
import org.apache.iotdb.db.client.ConfigNodeInfo;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.mpp.plan.statement.sync.OperateReceiverPipeStatement;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.rpc.TSStatusCode;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class OperateReceiverPipeTask implements IConfigTask {
  private static final Logger LOGGER = LoggerFactory.getLogger(OperateReceiverPipeTask.class);
  private static IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private OperateReceiverPipeStatement operateReceiverPipeStatement;

  public OperateReceiverPipeTask(OperateReceiverPipeStatement operateReceiverPipeStatement) {
    this.operateReceiverPipeStatement = operateReceiverPipeStatement;
  }

  @Override
  public ListenableFuture<ConfigTaskResult> execute(
      IClientManager<PartitionRegionId, ConfigNodeClient> clientManager)
      throws InterruptedException {
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    if (config.isClusterMode()) {
      //      // Construct request using statement
      TOperateReceiverPipeReq req = new TOperateReceiverPipeReq();
      req.setType(operateReceiverPipeStatement.getOperateType());
      req.setRemoteIp(operateReceiverPipeStatement.getRemoteIp());
      req.setPipeName(operateReceiverPipeStatement.getPipeName());
      req.setCreateTime(operateReceiverPipeStatement.getCreateTime());
      try (ConfigNodeClient client = clientManager.borrowClient(ConfigNodeInfo.partitionRegionId)) {
        // Send request to some API server
        TSStatus status = client.operateReceiverPipe(req);
        //    Get response or throw exception
        if (TSStatusCode.SUCCESS_STATUS.getStatusCode() != status.getCode()) {
          LOGGER.error(
              "Failed to execute operate pipe {}-{}-{} in config node, status is {}.",
              operateReceiverPipeStatement.getRemoteIp(),
              operateReceiverPipeStatement.getPipeName(),
              operateReceiverPipeStatement.getCreateTime(),
              status);
          future.setException(new StatementExecutionException(status));
        } else {
          future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS));
        }
      } catch (TException | IOException e) {
        LOGGER.error("Failed to connect to config node.");
        future.setException(e);
      }
    } else {
      // TODO(sync): use local config node api
    }
    // If the action is executed successfully, return the Future.
    // If your operation is async, you can return the corresponding future directly.
    return future;
  }
}
