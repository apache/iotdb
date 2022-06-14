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

import org.apache.iotdb.common.rpc.thrift.TDataNodeInfo;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.confignode.client.SyncConfigNodeToDataNodeClientPool;
import org.apache.iotdb.confignode.consensus.request.ConfigRequestType;
import org.apache.iotdb.confignode.consensus.request.auth.AuthorReq;
import org.apache.iotdb.confignode.consensus.response.PermissionInfoResp;
import org.apache.iotdb.confignode.persistence.AuthorInfo;
import org.apache.iotdb.confignode.rpc.thrift.TPermissionInfoResp;
import org.apache.iotdb.mpp.rpc.thrift.TInvalidatePermissionCacheReq;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/** manager permission query and operation */
public class PermissionManager {

  private static final Logger logger = LoggerFactory.getLogger(PermissionManager.class);

  private final ConfigManager configManager;
  private final AuthorInfo authorInfo;

  public PermissionManager(ConfigManager configManager, AuthorInfo authorInfo) {
    this.configManager = configManager;
    this.authorInfo = authorInfo;
  }

  /**
   * write permission
   *
   * @param authorReq AuthorReq
   * @return TSStatus
   */
  public TSStatus operatePermission(AuthorReq authorReq) {
    TSStatus tsStatus;
    // If the permissions change, clear the cache content affected by the operation
    if (authorReq.getAuthorType() == ConfigRequestType.CreateUser
        || authorReq.getAuthorType() == ConfigRequestType.CreateRole) {
      tsStatus = getConsensusManager().write(authorReq).getStatus();
    } else {
      tsStatus = invalidateCache(authorReq.getUserName(), authorReq.getRoleName());
      if (tsStatus.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        tsStatus = getConsensusManager().write(authorReq).getStatus();
      }
    }
    return tsStatus;
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

  /**
   * When the permission information of a user or role is changed will clear all datanode
   * permissions related to the user or role
   */
  public TSStatus invalidateCache(String username, String roleName) {
    List<TDataNodeInfo> allDataNodes = configManager.getNodeManager().getOnlineDataNodes(-1);
    TInvalidatePermissionCacheReq req = new TInvalidatePermissionCacheReq();
    TSStatus status;
    req.setUsername(username);
    req.setRoleName(roleName);
    for (TDataNodeInfo dataNodeInfo : allDataNodes) {
      status =
          SyncConfigNodeToDataNodeClientPool.getInstance()
              .invalidatePermissionCache(dataNodeInfo.getLocation().getInternalEndPoint(), req);
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return status;
      }
    }
    return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
  }
}
