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

package org.apache.iotdb.confignode.manager;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.confignode.consensus.request.auth.AuthorReq;
import org.apache.iotdb.confignode.consensus.response.PermissionInfoResp;
import org.apache.iotdb.confignode.persistence.AuthorInfo;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.rpc.thrift.TPermissionInfoResp;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/** manager permission query and operation */
public class PermissionManager {

  private static final Logger logger = LoggerFactory.getLogger(PermissionManager.class);

  private final ConfigManager configManager;
  private final AuthorInfo authorInfo;
  private ConfigNodeProcedureEnv env;

  public PermissionManager(ConfigManager configManager, AuthorInfo authorInfo) {
    this.configManager = configManager;
    this.authorInfo = authorInfo;
    this.env = new ConfigNodeProcedureEnv(configManager);
  }

  /**
   * write permission
   *
   * @param authorReq AuthorReq
   * @return TSStatus
   */
  public TSStatus operatePermission(AuthorReq authorReq) {
    TSStatus status = getConsensusManager().write(authorReq).getStatus();
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      try {
        authorInfo.invalidateCache(env, authorReq.getUserName(), authorReq.getRoleName());
      } catch (IOException | TException e) {
        logger.error(
            "Failed to initialize cache,the initialization operation is {}",
            authorReq.getAuthorType());
      }
    }
    return status;
  }

  /**
   * Query for permissions
   *
   * @param authorReq AuthorReq
   * @return PermissionInfoResp
   */
  public PermissionInfoResp queryPermission(AuthorReq authorReq) {
    return (PermissionInfoResp) getConsensusManager().read(authorReq).getDataset();
  }

  private ConsensusManager getConsensusManager() {
    return configManager.getConsensusManager();
  }

  public TPermissionInfoResp login(String username, String password) {
    return authorInfo.login(username, password);
  }

  public TPermissionInfoResp checkUserPrivileges(
      String username, List<String> paths, int permission) {
    return authorInfo.checkUserPrivileges(username, paths, permission);
  }
}
