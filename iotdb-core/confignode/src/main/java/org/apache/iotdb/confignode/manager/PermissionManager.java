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
import org.apache.iotdb.commons.auth.AuthException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;
import org.apache.iotdb.confignode.consensus.request.read.auth.AuthorReadPlan;
import org.apache.iotdb.confignode.consensus.request.write.auth.AuthorPlan;
import org.apache.iotdb.confignode.consensus.response.auth.PermissionInfoResp;
import org.apache.iotdb.confignode.manager.consensus.ConsensusManager;
import org.apache.iotdb.confignode.persistence.AuthorInfo;
import org.apache.iotdb.confignode.rpc.thrift.TAuthizedPatternTreeResp;
import org.apache.iotdb.confignode.rpc.thrift.TPermissionInfoResp;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/** Manager permission read and operation. */
public class PermissionManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(PermissionManager.class);

  private final ConfigManager configManager;
  private final AuthorInfo authorInfo;

  public PermissionManager(ConfigManager configManager, AuthorInfo authorInfo) {
    this.configManager = configManager;
    this.authorInfo = authorInfo;
  }

  /**
   * Write permission.
   *
   * @param authorPlan AuthorReq
   * @param isGeneratedByPipe whether the plan is operated by pipe receiver
   * @return TSStatus
   */
  public TSStatus operatePermission(AuthorPlan authorPlan, boolean isGeneratedByPipe) {
    TSStatus tsStatus;
    // If the permissions change, clear the cache content affected by the operation
    LOGGER.info("Auth: run auth plan: {}", authorPlan.toString());
    try {
      if (authorPlan.getAuthorType() == ConfigPhysicalPlanType.CreateUser
          || authorPlan.getAuthorType() == ConfigPhysicalPlanType.CreateRole
          || authorPlan.getAuthorType() == ConfigPhysicalPlanType.CreateUserWithRawPassword) {
        tsStatus = getConsensusManager().write(authorPlan);
      } else {
        List<TDataNodeConfiguration> allDataNodes =
            configManager.getNodeManager().getRegisteredDataNodes();
        tsStatus =
            configManager
                .getProcedureManager()
                .operateAuthPlan(authorPlan, allDataNodes, isGeneratedByPipe);
      }
      return tsStatus;
    } catch (ConsensusException e) {
      LOGGER.warn("Failed in the write API executing the consensus layer due to: ", e);
      TSStatus res = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
      res.setMessage(e.getMessage());
      return res;
    }
  }

  /**
   * Query for permissions.
   *
   * @param authorPlan AuthorReq
   * @return PermissionInfoResp
   */
  public PermissionInfoResp queryPermission(final AuthorReadPlan authorPlan) {
    try {
      return (PermissionInfoResp) getConsensusManager().read(authorPlan);
    } catch (final ConsensusException e) {
      LOGGER.warn("Failed in the read API executing the consensus layer due to: ", e);
      final TSStatus res = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
      res.setMessage(e.getMessage());
      return new PermissionInfoResp(res);
    }
  }

  private ConsensusManager getConsensusManager() {
    return configManager.getConsensusManager();
  }

  public TPermissionInfoResp login(String username, String password) {
    return authorInfo.login(username, password);
  }

  public TPermissionInfoResp checkUserPrivileges(
      String username, List<PartialPath> paths, int permission) {
    return authorInfo.checkUserPrivileges(username, paths, permission);
  }

  public TAuthizedPatternTreeResp fetchAuthizedPTree(String username, int permission)
      throws AuthException {
    return authorInfo.generateAuthizedPTree(username, permission);
  }

  public TPermissionInfoResp checkUserPrivilegeGrantOpt(
      String username, List<PartialPath> paths, int permission) throws AuthException {
    return authorInfo.checkUserPrivilegeGrantOpt(username, paths, permission);
  }

  public TPermissionInfoResp checkRoleOfUser(String username, String rolename)
      throws AuthException {
    return authorInfo.checkRoleOfUser(username, rolename);
  }

  public void checkUserPathPrivilege() {
    authorInfo.checkUserPathPrivilege();
  }
}
