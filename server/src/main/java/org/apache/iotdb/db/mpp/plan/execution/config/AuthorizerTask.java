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

import org.apache.iotdb.commons.auth.AuthException;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.consensus.PartitionRegionId;
import org.apache.iotdb.confignode.rpc.thrift.TAuthorizerReq;
import org.apache.iotdb.db.auth.AuthorizerManager;
import org.apache.iotdb.db.client.ConfigNodeClient;
import org.apache.iotdb.db.client.ConfigNodeInfo;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.mpp.plan.analyze.QueryType;
import org.apache.iotdb.db.mpp.plan.statement.sys.AuthorStatement;
import org.apache.iotdb.db.qp.physical.sys.AuthorPlan;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class AuthorizerTask implements IConfigTask {

  private static final Logger LOGGER = LoggerFactory.getLogger(AuthorizerTask.class);

  private static IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private AuthorStatement authorStatement;
  private AuthorizerManager authorizerManager = AuthorizerManager.getInstance();

  public AuthorizerTask(AuthorStatement authorStatement) {
    this.authorStatement = authorStatement;
  }

  @Override
  public ListenableFuture<ConfigTaskResult> execute(
      IClientManager<PartitionRegionId, ConfigNodeClient> clientManager) {
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();

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
        future = authorizerManager.operatePermission(req, configNodeClient);
      } else {
        future = authorizerManager.queryPermission(req, configNodeClient);
      }
    } catch (IOException | TException e) {
      LOGGER.error("can't connect to all config nodes", e);
      future.setException(e);
    } catch (AuthException e) {
      future.setException(e);
    }
    // If the action is executed successfully, return the Future.
    // If your operation is async, you can return the corresponding future directly.
    return future;
  }
}
