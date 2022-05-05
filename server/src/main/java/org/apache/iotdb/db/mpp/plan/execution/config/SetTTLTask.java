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
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.confignode.rpc.thrift.TSetTTLReq;
import org.apache.iotdb.db.client.ConfigNodeClient;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.localconfignode.LocalConfigNode;
import org.apache.iotdb.db.mpp.plan.statement.metadata.SetTTLStatement;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.rpc.TSStatusCode;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class SetTTLTask implements IConfigTask {
  private static final Logger LOGGER = LoggerFactory.getLogger(SetTTLTask.class);

  private static IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  protected final SetTTLStatement statement;
  protected String taskName;

  public SetTTLTask(SetTTLStatement statement) {
    this.statement = statement;
    this.taskName = "set ttl";
  }

  @Override
  public ListenableFuture<ConfigTaskResult> execute() throws InterruptedException {
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    if (config.isClusterMode()) {
      TSetTTLReq setTTLReq =
          new TSetTTLReq(statement.getStorageGroupPath().getFullPath(), statement.getTTL());
      ConfigNodeClient configNodeClient = null;
      try {
        configNodeClient = new ConfigNodeClient();
        // Send request to some API server
        TSStatus tsStatus = configNodeClient.setTTL(setTTLReq);
        // Get response or throw exception
        if (TSStatusCode.SUCCESS_STATUS.getStatusCode() != tsStatus.getCode()) {
          LOGGER.error(
              "Failed to execute {} {} in config node, status is {}.",
              taskName,
              statement.getStorageGroupPath(),
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
        LocalConfigNode.getInstance().setTTL(statement.getStorageGroupPath(), statement.getTTL());
      } catch (MetadataException | IOException e) {
        future.setException(e);
      }
      future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS));
    }
    return future;
  }
}
