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
import org.apache.iotdb.commons.auth.AuthException;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.consensus.PartitionRegionId;
import org.apache.iotdb.confignode.rpc.thrift.TAuthorizerReq;
import org.apache.iotdb.confignode.rpc.thrift.TAuthorizerResp;
import org.apache.iotdb.db.client.ConfigNodeClient;
import org.apache.iotdb.db.client.ConfigNodeInfo;
import org.apache.iotdb.db.mpp.common.header.ColumnHeader;
import org.apache.iotdb.db.mpp.common.header.DatasetHeader;
import org.apache.iotdb.db.mpp.plan.analyze.QueryType;
import org.apache.iotdb.db.mpp.plan.statement.sys.AuthorStatement;
import org.apache.iotdb.db.qp.logical.sys.AuthorOperator;
import org.apache.iotdb.db.qp.physical.sys.AuthorPlan;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.utils.Binary;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class AuthorizerConfigTask implements IConfigTask {

  private static final Logger LOGGER = LoggerFactory.getLogger(AuthorizerConfigTask.class);

  private AuthorStatement authorStatement;

  public AuthorizerConfigTask(AuthorStatement authorStatement) {
    this.authorStatement = authorStatement;
  }

  @Override
  public ListenableFuture<ConfigTaskResult> execute(
      IClientManager<PartitionRegionId, ConfigNodeClient> clientManager) {
    SettableFuture<ConfigTaskResult> future = null;
    try (ConfigNodeClient configNodeClient =
        clientManager.borrowClient(ConfigNodeInfo.partitionRegionId)) {
      // Construct request using statement
      TAuthorizerReq req =
          new TAuthorizerReq(
              authorStatement.getAuthorType().ordinal(),
              authorStatement.getUserName() == null ? "" : authorStatement.getUserName(),
              authorStatement.getRoleName() == null ? "" : authorStatement.getRoleName(),
              authorStatement.getPassWord() == null ? "" : authorStatement.getPassWord(),
              authorStatement.getNewPassword() == null ? "" : authorStatement.getNewPassword(),
              AuthorPlan.strToPermissions(authorStatement.getPrivilegeList()),
              authorStatement.getNodeName() == null
                  ? ""
                  : authorStatement.getNodeName().getFullPath());

      // Send request to some API server
      if (authorStatement.getQueryType() == QueryType.WRITE) {
        future = operatePermission(req, configNodeClient);
      } else {
        future = queryPermission(req, configNodeClient);
      }
    } catch (AuthException e) {
      LOGGER.error("No such privilege {}.", authorStatement.getAuthorType());
      future.setException(e);
    } catch (IOException e) {
      LOGGER.error("can't connect to all config nodes", e);
      future.setException(e);
    }
    // If the action is executed successfully, return the Future.
    // If your operation is async, you can return the corresponding future directly.
    return future;
  }

  private SettableFuture<ConfigTaskResult> operatePermission(
      TAuthorizerReq authorizerReq, ConfigNodeClient configNodeClient) {
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    try {
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
    } catch (TException e) {
      LOGGER.error("Failed to connect to config node.");
      future.setException(e);
    }
    // If the action is executed successfully, return the Future.
    // If your operation is async, you can return the corresponding future directly.
    return future;
  }

  private SettableFuture<ConfigTaskResult> queryPermission(
      TAuthorizerReq authorizerReq, ConfigNodeClient configNodeClient) {
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    TAuthorizerResp authorizerResp;
    try {
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
        // build TSBlock
        List<TSDataType> types = new ArrayList<>();
        Map<String, List<String>> authorizerInfo = authorizerResp.getAuthorizerInfo();
        for (int i = 0; i < authorizerInfo.size(); i++) {
          types.add(TSDataType.TEXT);
        }
        TsBlockBuilder builder = new TsBlockBuilder(types);
        List<ColumnHeader> headerList = new ArrayList<>();

        for (String header : authorizerInfo.keySet()) {
          headerList.add(new ColumnHeader(header, TSDataType.TEXT));
        }
        // The Time column will be ignored by the setting of ColumnHeader.
        // So we can put a meaningless value here
        for (String value : authorizerInfo.get(headerList.get(0).getColumnName())) {
          builder.getTimeColumnBuilder().writeLong(0L);
          builder.getColumnBuilder(0).writeBinary(new Binary(value));
          builder.declarePosition();
        }
        for (int i = 1; i < headerList.size(); i++) {
          for (String value : authorizerInfo.get(headerList.get(i).getColumnName())) {
            builder.getColumnBuilder(i).writeBinary(new Binary(value));
          }
        }

        DatasetHeader datasetHeader = new DatasetHeader(headerList, true);
        future.set(
            new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS, builder.build(), datasetHeader));
      }
    } catch (TException e) {
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
