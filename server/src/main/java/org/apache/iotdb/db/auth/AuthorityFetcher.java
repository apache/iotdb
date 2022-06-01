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

package org.apache.iotdb.db.auth;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.consensus.PartitionRegionId;
import org.apache.iotdb.confignode.rpc.thrift.TAuthorizerReq;
import org.apache.iotdb.confignode.rpc.thrift.TAuthorizerResp;
import org.apache.iotdb.confignode.rpc.thrift.TCheckUserPrivilegesReq;
import org.apache.iotdb.confignode.rpc.thrift.TLoginReq;
import org.apache.iotdb.confignode.rpc.thrift.TPermissionInfoResp;
import org.apache.iotdb.db.client.ConfigNodeClient;
import org.apache.iotdb.db.client.ConfigNodeInfo;
import org.apache.iotdb.db.client.DataNodeClientPoolFactory;
import org.apache.iotdb.db.mpp.common.header.ColumnHeader;
import org.apache.iotdb.db.mpp.common.header.DatasetHeader;
import org.apache.iotdb.db.mpp.plan.execution.config.AuthorizerConfigTask;
import org.apache.iotdb.db.mpp.plan.execution.config.ConfigTaskResult;
import org.apache.iotdb.db.qp.logical.sys.AuthorOperator;
import org.apache.iotdb.rpc.ConfigNodeConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.utils.Binary;

import com.google.common.util.concurrent.SettableFuture;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class AuthorityFetcher {

  private static final Logger logger = LoggerFactory.getLogger(AuthorizerConfigTask.class);

  private static final IClientManager<PartitionRegionId, ConfigNodeClient> configNodeClientManager =
      new IClientManager.Factory<PartitionRegionId, ConfigNodeClient>()
          .createClientManager(new DataNodeClientPoolFactory.ConfigNodeClientPoolFactory());

  public static TPermissionInfoResp checkPath(String username, List<String> allPath, int permission)
      throws ConfigNodeConnectionException {
    TCheckUserPrivilegesReq req = new TCheckUserPrivilegesReq(username, allPath, permission);
    TPermissionInfoResp status = null;
    try (ConfigNodeClient configNodeClient =
        configNodeClientManager.borrowClient(ConfigNodeInfo.partitionRegionId)) {
      // Send request to some API server
      status = configNodeClient.checkUserPrivileges(req);
    } catch (TException | IOException e) {
      throw new ConfigNodeConnectionException("Couldn't connect config node");
    } finally {
      if (status == null) {
        status = new TPermissionInfoResp();
      }
    }
    return status;
  }

  /** Check the user */
  public static TPermissionInfoResp checkUser(String username, String password)
      throws ConfigNodeConnectionException {
    TLoginReq req = new TLoginReq(username, password);
    TPermissionInfoResp status = null;
    try (ConfigNodeClient configNodeClient =
        configNodeClientManager.borrowClient(ConfigNodeInfo.partitionRegionId)) {
      // Send request to some API server
      status = configNodeClient.login(req);
    } catch (TException | IOException e) {
      throw new ConfigNodeConnectionException("Couldn't connect config node");
    } finally {
      if (status == null) {
        status = new TPermissionInfoResp();
      }
    }
    return status;
  }

  public static SettableFuture<ConfigTaskResult> operatePermission(
      TAuthorizerReq authorizerReq, ConfigNodeClient configNodeClient) {
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    try {
      // Send request to some API server
      TSStatus tsStatus = configNodeClient.operatePermission(authorizerReq);
      // Get response or throw exception
      if (TSStatusCode.SUCCESS_STATUS.getStatusCode() != tsStatus.getCode()) {
        logger.error(
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
      logger.error("Failed to connect to config node.");
      future.setException(e);
    }
    // If the action is executed successfully, return the Future.
    // If your operation is async, you can return the corresponding future directly.
    return future;
  }

  public static SettableFuture<ConfigTaskResult> queryPermission(
      TAuthorizerReq authorizerReq, ConfigNodeClient configNodeClient) {
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    TAuthorizerResp authorizerResp;
    try {
      // Send request to some API server
      authorizerResp = configNodeClient.queryPermission(authorizerReq);
      // Get response or throw exception
      if (TSStatusCode.SUCCESS_STATUS.getStatusCode() != authorizerResp.getStatus().getCode()) {
        logger.error(
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
        for (String value : authorizerInfo.get(headerList.get(0).getColumnNameWithAlias())) {
          builder.getTimeColumnBuilder().writeLong(0L);
          builder.getColumnBuilder(0).writeBinary(new Binary(value));
          builder.declarePosition();
        }
        for (int i = 1; i < headerList.size(); i++) {
          for (String value : authorizerInfo.get(headerList.get(i).getColumnNameWithAlias())) {
            builder.getColumnBuilder(i).writeBinary(new Binary(value));
          }
        }

        DatasetHeader datasetHeader = new DatasetHeader(headerList, true);
        future.set(
            new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS, builder.build(), datasetHeader));
      }
    } catch (TException e) {
      logger.error("Failed to connect to config node.");
      future.setException(e);
    }
    // If the action is executed successfully, return the Future.
    // If your operation is async, you can return the corresponding future directly.
    return future;
  }
}
