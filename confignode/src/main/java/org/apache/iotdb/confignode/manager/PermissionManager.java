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

import org.apache.iotdb.common.rpc.thrift.TDataNodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.confignode.client.DataNodeRequestType;
import org.apache.iotdb.confignode.client.sync.SyncDataNodeClientPool;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;
import org.apache.iotdb.confignode.consensus.request.auth.AuthorPlan;
import org.apache.iotdb.confignode.consensus.response.auth.PermissionInfoResp;
import org.apache.iotdb.confignode.manager.consensus.ConsensusManager;
import org.apache.iotdb.confignode.persistence.AuthorInfo;
import org.apache.iotdb.confignode.rpc.thrift.TPermissionInfoResp;
import org.apache.iotdb.mpp.rpc.thrift.TInvalidatePermissionCacheReq;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import java.util.List;

/** manager permission query and operation */
public class PermissionManager {

  private final ConfigManager configManager;
  private final AuthorInfo authorInfo;

  public PermissionManager(ConfigManager configManager, AuthorInfo authorInfo) {
    this.configManager = configManager;
    this.authorInfo = authorInfo;
  }

  /**
   * write permission
   *
   * @param authorPlan AuthorReq
   * @return TSStatus
   */
  public TSStatus operatePermission(AuthorPlan authorPlan) {
    TSStatus tsStatus;
    // If the permissions change, clear the cache content affected by the operation
    if (authorPlan.getAuthorType() == ConfigPhysicalPlanType.CreateUser
        || authorPlan.getAuthorType() == ConfigPhysicalPlanType.CreateRole) {
      tsStatus = getConsensusManager().write(authorPlan).getStatus();
    } else {
      tsStatus = invalidateCache(authorPlan.getUserName(), authorPlan.getRoleName());
      if (tsStatus.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        tsStatus = getConsensusManager().write(authorPlan).getStatus();
      }
    }
    return tsStatus;
  }

  /**
   * Query for permissions
   *
   * @param authorPlan AuthorReq
   * @return PermissionInfoResp
   */
  public PermissionInfoResp queryPermission(AuthorPlan authorPlan) {
    return (PermissionInfoResp) getConsensusManager().read(authorPlan).getDataset();
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
    List<TDataNodeConfiguration> allDataNodes =
        configManager.getNodeManager().getRegisteredDataNodes();
    TInvalidatePermissionCacheReq req = new TInvalidatePermissionCacheReq();
    TSStatus status;
    req.setUsername(username);
    req.setRoleName(roleName);
    for (TDataNodeConfiguration dataNodeInfo : allDataNodes) {
      status =
          SyncDataNodeClientPool.getInstance()
              .sendSyncRequestToDataNodeWithRetry(
                  dataNodeInfo.getLocation().getInternalEndPoint(),
                  req,
                  DataNodeRequestType.INVALIDATE_PERMISSION_CACHE);
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return status;
      }
    }
    return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
  }
}
