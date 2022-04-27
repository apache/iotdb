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

package org.apache.iotdb.db.auth.authorizer;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.exception.BadNodeUrlException;
import org.apache.iotdb.confignode.rpc.thrift.TAuthorizerReq;
import org.apache.iotdb.confignode.rpc.thrift.TAuthorizerResp;
import org.apache.iotdb.db.client.ConfigNodeClient;
import org.apache.iotdb.db.mpp.execution.config.ConfigTaskResult;
import org.apache.iotdb.db.qp.logical.sys.AuthorOperator;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;

import com.google.common.util.concurrent.SettableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Locale;

public class ClusterAuthorizer {

  private static final Logger LOGGER = LoggerFactory.getLogger(ClusterAuthorizer.class);

  public SettableFuture<ConfigTaskResult> operatePermission(TAuthorizerReq authorizerReq) {
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    ConfigNodeClient configNodeClient = null;
    try {
      configNodeClient = new ConfigNodeClient();
      // Send request to some API server
      TSStatus tsStatus = configNodeClient.operatePermission(authorizerReq);
      // Get response or throw exception
      if (TSStatusCode.SUCCESS_STATUS.getStatusCode() != tsStatus.getCode()) {
        LOGGER.error(
            "Failed to execute {} in config node, status is {}.",
            AuthorOperator.AuthorType.values()[authorizerReq.getAuthorType()]
                .toString()
                .toLowerCase(Locale.ROOT),
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
    // If the action is executed successfully, return the Future.
    // If your operation is async, you can return the corresponding future directly.
    return future;
  }

  public SettableFuture<ConfigTaskResult> queryPermission(TAuthorizerReq authorizerReq) {
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    ConfigNodeClient configNodeClient = null;
    TsBlock tsBlock = null;
    TAuthorizerResp authorizerResp;
    try {
      configNodeClient = new ConfigNodeClient();
      // Send request to some API server
      authorizerResp = configNodeClient.queryPermission(authorizerReq);
      // Get response or throw exception
      if (TSStatusCode.SUCCESS_STATUS.getStatusCode() != authorizerResp.getStatus().getCode()) {
        LOGGER.error(
            "Failed to execute {} in config node, status is {}.",
            AuthorOperator.AuthorType.values()[authorizerReq.getAuthorType()]
                .toString()
                .toLowerCase(Locale.ROOT),
            authorizerResp.getStatus());
        future.setException(new StatementExecutionException(authorizerResp.getStatus()));
      } else {
        // TODO: Construct result
        future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS, tsBlock));
      }
    } catch (IoTDBConnectionException | BadNodeUrlException e) {
      LOGGER.error("Failed to connect to config node.");
      future.setException(e);
    } finally {
      if (configNodeClient != null) {
        configNodeClient.close();
      }
    }
    // If the action is executed successfully, return the Future.
    // If your operation is async, you can return the corresponding future directly.
    return future;
  }
}
