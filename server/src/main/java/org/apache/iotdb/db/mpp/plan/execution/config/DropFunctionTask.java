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
import org.apache.iotdb.commons.udf.service.UDFRegistrationService;
import org.apache.iotdb.confignode.rpc.thrift.TDropFunctionReq;
import org.apache.iotdb.db.client.ConfigNodeClient;
import org.apache.iotdb.db.client.ConfigNodeInfo;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.mpp.plan.statement.metadata.DropFunctionStatement;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.rpc.TSStatusCode;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class DropFunctionTask implements IConfigTask {

  private static final Logger LOGGER = LoggerFactory.getLogger(DropFunctionTask.class);
  private static final IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();

  private final String udfName;

  public DropFunctionTask(DropFunctionStatement dropFunctionStatement) {
    udfName = dropFunctionStatement.getUdfName();
  }

  @Override
  public ListenableFuture<ConfigTaskResult> execute(
      IClientManager<PartitionRegionId, ConfigNodeClient> clientManager)
      throws InterruptedException {
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    if (CONFIG.isClusterMode()) {
      executeCluster(clientManager, future);
    } else {
      executeStandalone(future);
    }
    return future;
  }

  private void executeCluster(
      IClientManager<PartitionRegionId, ConfigNodeClient> clientManager,
      SettableFuture<ConfigTaskResult> future) {
    try (ConfigNodeClient client = clientManager.borrowClient(ConfigNodeInfo.partitionRegionId)) {
      final TSStatus executionStatus = client.dropFunction(new TDropFunctionReq(udfName));

      if (TSStatusCode.SUCCESS_STATUS.getStatusCode() != executionStatus.getCode()) {
        LOGGER.error("[{}] Failed to drop function {} in config node.", executionStatus, udfName);
        future.setException(new StatementExecutionException(executionStatus));
      } else {
        future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS));
      }
    } catch (TException | IOException e) {
      LOGGER.error("Failed to connect to config node.");
      future.setException(e);
    }
  }

  private void executeStandalone(SettableFuture<ConfigTaskResult> future) {
    try {
      UDFRegistrationService.getInstance().deregister(udfName);
      future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS));
    } catch (Exception e) {
      final String message =
          String.format("Failed to drop function %s, because %s.", udfName, e.getMessage());
      LOGGER.error(message, e);
      future.setException(
          new StatementExecutionException(
              new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
                  .setMessage(message)));
    }
  }
}
