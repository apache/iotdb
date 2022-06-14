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
import org.apache.iotdb.commons.auth.AuthException;
import org.apache.iotdb.commons.utils.AuthUtils;
import org.apache.iotdb.db.localconfignode.LocalConfigNode;
import org.apache.iotdb.db.mpp.plan.execution.config.ConfigTaskResult;
import org.apache.iotdb.db.mpp.plan.statement.sys.AuthorStatement;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import com.google.common.util.concurrent.SettableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class StandaloneAuthorityFetcher implements IAuthorityFetcher {

  private static final Logger logger = LoggerFactory.getLogger(StandaloneAuthorityFetcher.class);

  private LocalConfigNode localConfigNode = LocalConfigNode.getInstance();

  private static final class StandaloneAuthorityFetcherHolder {
    private static final StandaloneAuthorityFetcher INSTANCE = new StandaloneAuthorityFetcher();

    private StandaloneAuthorityFetcherHolder() {}
  }

  public static StandaloneAuthorityFetcher getInstance() {
    return StandaloneAuthorityFetcher.StandaloneAuthorityFetcherHolder.INSTANCE;
  }

  @Override
  public TSStatus checkUser(String username, String password) {
    try {
      if (localConfigNode.login(username, password)) {
        return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
      } else {
        return RpcUtils.getStatus(
            TSStatusCode.WRONG_LOGIN_PASSWORD_ERROR, "Authentication failed.");
      }
    } catch (AuthException e) {
      return RpcUtils.getStatus(TSStatusCode.AUTHENTICATION_ERROR, e.getMessage());
    }
  }

  @Override
  public TSStatus checkUserPrivileges(String username, List<String> allPath, int permission) {
    boolean checkStatus = true;
    String checkMessage = null;
    for (String path : allPath) {
      try {
        if (!checkOnePath(username, path, permission)) {
          checkStatus = false;
          break;
        }
      } catch (AuthException e) {
        checkStatus = false;
      }
    }
    if (checkStatus) {
      return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
    } else {
      return RpcUtils.getStatus(TSStatusCode.NO_PERMISSION_ERROR, checkMessage);
    }
  }

  private boolean checkOnePath(String username, String path, int permission) throws AuthException {
    try {
      String fullPath = path == null ? AuthUtils.ROOT_PATH_PRIVILEGE : path;
      if (localConfigNode.checkUserPrivileges(username, fullPath, permission)) {
        return true;
      }
    } catch (AuthException e) {
      logger.error("Error occurs when checking the seriesPath {} for user {}", path, username, e);
      throw new AuthException(e);
    }
    return false;
  }

  @Override
  public SettableFuture<ConfigTaskResult> operatePermission(AuthorStatement authorStatement) {
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    boolean status = true;
    try {
      LocalConfigNode.getInstance().operatorPermission(authorStatement);
    } catch (AuthException e) {
      future.setException(e);
      status = false;
    }
    if (status) {
      future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS));
    }
    return future;
  }

  @Override
  public SettableFuture<ConfigTaskResult> queryPermission(AuthorStatement authorStatement) {
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    Map<String, List<String>> authorizerResp;
    try {
      authorizerResp = LocalConfigNode.getInstance().queryPermission(authorStatement);
      // build TSBlock
      AuthorizerManager.getInstance().buildTSBlock(authorizerResp, future);
    } catch (AuthException e) {
      future.setException(e);
    }
    return future;
  }
}
