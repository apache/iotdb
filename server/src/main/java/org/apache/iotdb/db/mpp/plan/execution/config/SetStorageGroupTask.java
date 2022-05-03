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
import org.apache.iotdb.commons.exception.BadNodeUrlException;
import org.apache.iotdb.confignode.rpc.thrift.TSetStorageGroupReq;
import org.apache.iotdb.confignode.rpc.thrift.TStorageGroupSchema;
import org.apache.iotdb.db.client.ConfigNodeClient;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.localconfignode.LocalConfigNode;
import org.apache.iotdb.db.mpp.plan.statement.metadata.SetStorageGroupStatement;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.rpc.TSStatusCode;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SetStorageGroupTask implements IConfigTask {
  private static final Logger LOGGER = LoggerFactory.getLogger(SetStorageGroupTask.class);

  private static IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private final SetStorageGroupStatement setStorageGroupStatement;

  public SetStorageGroupTask(SetStorageGroupStatement setStorageGroupStatement) {
    this.setStorageGroupStatement = setStorageGroupStatement;
  }

  @Override
  public ListenableFuture<ConfigTaskResult> execute() {
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    // TODO:(this judgement needs to be integrated in a high level framework)
    if (config.isClusterMode()) {
      // Construct request using statement
      TStorageGroupSchema storageGroupSchema = new TStorageGroupSchema();
      storageGroupSchema.setName(setStorageGroupStatement.getStorageGroupPath().getFullPath());
      TSetStorageGroupReq req = new TSetStorageGroupReq(storageGroupSchema);
      ConfigNodeClient configNodeClient = null;
      try {
        configNodeClient = new ConfigNodeClient();
        // Send request to some API server
        TSStatus tsStatus = configNodeClient.setStorageGroup(req);
        // Get response or throw exception
        if (TSStatusCode.SUCCESS_STATUS.getStatusCode() != tsStatus.getCode()) {
          LOGGER.error(
              "Failed to execute set storage group {} in config node, status is {}.",
              setStorageGroupStatement.getStorageGroupPath(),
              tsStatus);
          future.setException(new StatementExecutionException(tsStatus));
        } else {
          future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS));
        }
      } catch (IoTDBConnectionException | BadNodeUrlException e) {
        LOGGER.error("Failed to connect to config node.");
        future.setException(e);
      } finally {
        if (configNodeClient != null) {
          configNodeClient.close();
        }
      }
    } else {
      try {
        LocalConfigNode.getInstance()
            .setStorageGroup(setStorageGroupStatement.getStorageGroupPath());
      } catch (MetadataException e) {
        future.setException(e);
      }
      future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS));
    }
    // If the action is executed successfully, return the Future.
    // If your operation is async, you can return the corresponding future directly.
    return future;
  }
}
