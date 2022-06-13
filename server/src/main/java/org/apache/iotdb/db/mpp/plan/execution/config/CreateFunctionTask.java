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
import org.apache.iotdb.commons.udf.service.UDFExecutableManager;
import org.apache.iotdb.commons.udf.service.UDFRegistrationService;
import org.apache.iotdb.confignode.rpc.thrift.TCreateFunctionReq;
import org.apache.iotdb.db.client.ConfigNodeInfo;
import org.apache.iotdb.db.client.DataNodeToConfigNodeClient;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.mpp.plan.statement.metadata.CreateFunctionStatement;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.rpc.TSStatusCode;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.stream.Collectors;

public class CreateFunctionTask implements IConfigTask {

  private static final Logger LOGGER = LoggerFactory.getLogger(CreateFunctionTask.class);
  private static final IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();

  private final String udfName;
  private final String className;
  private final List<String> uris;

  public CreateFunctionTask(CreateFunctionStatement createFunctionStatement) {
    udfName = createFunctionStatement.getUdfName();
    className = createFunctionStatement.getClassName();
    uris =
        createFunctionStatement.getUris().stream().map(URI::toString).collect(Collectors.toList());
  }

  @Override
  public ListenableFuture<ConfigTaskResult> execute(
      IClientManager<PartitionRegionId, DataNodeToConfigNodeClient> clientManager)
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
      IClientManager<PartitionRegionId, DataNodeToConfigNodeClient> clientManager,
      SettableFuture<ConfigTaskResult> future) {
    try (DataNodeToConfigNodeClient client =
        clientManager.borrowClient(ConfigNodeInfo.partitionRegionId)) {
      final TSStatus executionStatus =
          client.createFunction(new TCreateFunctionReq(udfName, className, uris));

      if (TSStatusCode.SUCCESS_STATUS.getStatusCode() != executionStatus.getCode()) {
        LOGGER.error(
            "[{}] Failed to create function {}({}) in config node, URI: {}.",
            executionStatus,
            udfName,
            className,
            uris);
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
      UDFRegistrationService.getInstance()
          .register(udfName, className, uris, UDFExecutableManager.getInstance(), true);
      future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS));
    } catch (Exception e) {
      final String message =
          String.format(
              "Failed to create function %s(%s), URI: %s, because %s.",
              udfName, className, uris, e.getMessage());
      LOGGER.error(message, e);
      future.setException(
          new StatementExecutionException(
              new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
                  .setMessage(message)));
    }
  }
}
