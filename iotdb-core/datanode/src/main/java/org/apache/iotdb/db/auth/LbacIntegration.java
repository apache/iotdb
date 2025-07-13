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
import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.auth.entity.User;
import org.apache.iotdb.commons.client.exception.ClientManagerException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.confignode.rpc.thrift.TCheckUserPrivilegesReq;
import org.apache.iotdb.confignode.rpc.thrift.TPermissionInfoResp;
import org.apache.iotdb.db.protocol.client.ConfigNodeClient;
import org.apache.iotdb.db.protocol.client.ConfigNodeClientManager;
import org.apache.iotdb.db.protocol.client.ConfigNodeInfo;
import org.apache.iotdb.db.queryengine.plan.execution.config.session.ShowUserLabelPolicyTask;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.ShowUserLabelPolicyStatement;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Integration point for LBAC (Label-Based Access Control) with existing IoTDB authorization system.
 * This class provides methods to add LBAC checks to the existing RBAC flow.
 */
public class LbacIntegration {

  private static final Logger LOGGER = LoggerFactory.getLogger(LbacIntegration.class);

  /**
   * Main entry point for LBAC permission checking. This method should be called AFTER successful
   * RBAC permission check to add an additional layer of label-based access control.
   *
   * @param statement The statement being executed
   * @param userName The user requesting access
   * @param paths The paths involved in the operation
   * @return TSStatus indicating success or failure with reason
   */
  public static TSStatus checkLbacAfterRbac(
      Statement statement, String userName, List<PartialPath> paths) {

    LOGGER.warn(
        "=== LBAC INTEGRATION START === Starting LBAC check for user: {} on statement: {}",
        userName,
        statement.getClass().getSimpleName());

    try {
      // Step 1: Get user information
      LOGGER.warn("Step 1: Getting user by name: {}", userName);
      User user = getUserByName(userName);
      if (user == null) {
        LOGGER.warn("User not found for LBAC check: {}", userName);
        return new TSStatus(TSStatusCode.USER_NOT_EXIST.getStatusCode())
            .setMessage("User not found for LBAC check");
      }

      LOGGER.warn(
          "Step 2: Found user {} for LBAC check with roles: {}", userName, user.getRoleSet());

      // Step 2: Convert paths to device paths (strings) for LBAC processing
      List<String> devicePaths = new ArrayList<>();
      for (PartialPath path : paths) {
        devicePaths.add(path.getFullPath());
      }
      LOGGER.warn("Step 3: Converted {} paths to device paths: {}", paths.size(), devicePaths);

      // Step 3: Perform LBAC permission check
      LOGGER.warn("Step 4: About to call LbacPermissionChecker.checkLbacPermission");
      LbacPermissionChecker.LbacCheckResult result =
          LbacPermissionChecker.checkLbacPermission(statement, user, devicePaths);
      LOGGER.warn(
          "Step 5: LbacPermissionChecker returned result: allowed={}, reason={}",
          result.isAllowed(),
          result.getReason());

      // Step 4: Return result
      if (result.isAllowed()) {
        LOGGER.warn("LBAC permission check PASSED for user: {}", userName);
        return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
      } else {
        LOGGER.warn("LBAC permission DENIED for user {}: {}", userName, result.getReason());
        return new TSStatus(TSStatusCode.NO_PERMISSION.getStatusCode())
            .setMessage("LBAC access denied: " + result.getReason());
      }

    } catch (Exception e) {
      LOGGER.error("Error during LBAC permission check for user: {}", userName, e);
      return new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
          .setMessage("Internal error during LBAC check: " + e.getMessage());
    }
  }

  /** Get user object by username - fetch from ConfigNode if not cached */
  private static User getUserByName(String userName) {
    try {
      LOGGER.debug("Getting user: {}", userName);

      // Step 1: Try to get user from cache first
      IAuthorityFetcher authorityFetcher = AuthorityChecker.getAuthorityFetcher();
      User user = authorityFetcher.getAuthorCache().getUserCache(userName);

      if (user != null) {
        LOGGER.debug("Found user {} in cache", userName);
        return user;
      }

      LOGGER.debug("User {} not in cache, fetching from ConfigNode", userName);

      // Step 2: Fetch user from ConfigNode using same approach as
      // ClusterAuthorityFetcher
      TCheckUserPrivilegesReq req =
          new TCheckUserPrivilegesReq(
              userName,
              0, // PrivilegeModelType.TREE.ordinal()
              PrivilegeType.READ_DATA.ordinal(),
              false);

      TPermissionInfoResp permissionInfoResp = null;
      try (ConfigNodeClient configNodeClient =
          ConfigNodeClientManager.getInstance().borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {

        permissionInfoResp = configNodeClient.checkUserPrivileges(req);

      } catch (ClientManagerException | TException e) {
        LOGGER.error("Failed to connect to ConfigNode to fetch user: {}", userName, e);
        return null;
      }

      // Step 3: Process response and cache user if successful
      if (permissionInfoResp != null
          && permissionInfoResp.getStatus().getCode()
              == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {

        // Use ClusterAuthorityFetcher's cacheUser method to properly construct User
        // object
        ClusterAuthorityFetcher clusterFetcher = (ClusterAuthorityFetcher) authorityFetcher;
        User fetchedUser = clusterFetcher.cacheUser(permissionInfoResp);

        // Cache the user for future use
        authorityFetcher.getAuthorCache().putUserCache(userName, fetchedUser);

        LOGGER.debug("Successfully fetched and cached user: {}", userName);
        return fetchedUser;

      } else {
        LOGGER.warn(
            "Failed to fetch user {} from ConfigNode: {}",
            userName,
            permissionInfoResp != null
                ? permissionInfoResp.getStatus().getMessage()
                : "null response");
        return null;
      }

    } catch (Exception e) {
      LOGGER.error("Error fetching user: {}", userName, e);
      return null;
    }
  }

  /** Perform the actual LBAC check */
  private static LbacPermissionChecker.LbacCheckResult performLbacCheck(
      Statement statement, User user, List<String> devicePaths) {
    try {
      // Use the LbacPermissionChecker to perform the actual check
      return LbacPermissionChecker.checkLbacPermission(statement, user, devicePaths);
    } catch (Exception e) {
      LOGGER.error("Error performing LBAC check", e);
      return LbacPermissionChecker.LbacCheckResult.deny(
          "Internal error during LBAC policy evaluation: " + e.getMessage());
    }
  }

  /**
   * Get user label policies for the specified user and scope.
   *
   * @param username The username to get policies for
   * @param scope The scope (READ, WRITE, or READ_WRITE)
   * @return List of user label policy information
   */
  public static List<ShowUserLabelPolicyTask.UserLabelPolicyInfo> getUserLabelPolicies(
      String username, ShowUserLabelPolicyStatement.LabelPolicyScope scope) {

    LOGGER.debug("Getting user label policies for user: {} with scope: {}", username, scope);

    List<ShowUserLabelPolicyTask.UserLabelPolicyInfo> policies = new ArrayList<>();

    try {
      // Try to get label policies from ConfigNode
      try (ConfigNodeClient configNodeClient =
          ConfigNodeClientManager.getInstance().borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {

        // Build request
        org.apache.iotdb.confignode.rpc.thrift.TShowUserLabelPolicyReq req =
            new org.apache.iotdb.confignode.rpc.thrift.TShowUserLabelPolicyReq();
        if (username != null) {
          req.setUsername(username);
        }
        req.setScope(scope.name());

        // Send request to ConfigNode
        org.apache.iotdb.confignode.rpc.thrift.TShowUserLabelPolicyResp resp =
            configNodeClient.showUserLabelPolicy(req);

        // Process response
        if (resp.getStatus().getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
          // If there are policy data, add them to the result list
          if (resp.isSetUserLabelPolicyList() && resp.getUserLabelPolicyListSize() > 0) {
            for (int i = 0; i < resp.getUserLabelPolicyListSize(); i++) {
              String user = resp.getUserLabelPolicyList().get(i).getUsername();
              String policyScope = resp.getUserLabelPolicyList().get(i).getScope();
              String policyExpression = resp.getUserLabelPolicyList().get(i).getPolicyExpression();

              ShowUserLabelPolicyStatement.LabelPolicyScope labelScope =
                  ShowUserLabelPolicyStatement.LabelPolicyScope.valueOf(policyScope);

              ShowUserLabelPolicyTask.UserLabelPolicyInfo policyInfo =
                  new ShowUserLabelPolicyTask.UserLabelPolicyInfo(
                      user, labelScope, policyExpression);
              policies.add(policyInfo);
            }
          }
          LOGGER.debug("Retrieved {} label policies for user: {}", policies.size(), username);
        } else {
          LOGGER.warn(
              "Failed to get label policies from ConfigNode: {}", resp.getStatus().getMessage());
        }
      } catch (ClientManagerException | TException e) {
        LOGGER.error("Error connecting to ConfigNode for label policies: {}", e.getMessage());
      }
    } catch (Exception e) {
      LOGGER.error("Error retrieving user label policies for user: {}", username, e);
    }

    return policies;
  }

  /**
   * Drop user label policy for the specified user and scope.
   *
   * @param username The username to drop policy for
   * @param scope The scope (READ, WRITE, or READ_WRITE)
   * @throws Exception If there is an error dropping the policy
   */
  public static void dropUserLabelPolicy(
      String username, ShowUserLabelPolicyStatement.LabelPolicyScope scope) throws Exception {

    LOGGER.debug("Dropping user label policy for user: {} with scope: {}", username, scope);

    try {
      // Try to drop label policy via ConfigNode
      try (ConfigNodeClient configNodeClient =
          ConfigNodeClientManager.getInstance().borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {

        // Build request
        org.apache.iotdb.confignode.rpc.thrift.TDropUserLabelPolicyReq req =
            new org.apache.iotdb.confignode.rpc.thrift.TDropUserLabelPolicyReq();
        req.setUsername(username);
        req.setScope(scope.name());

        // Send request to ConfigNode
        TSStatus status = configNodeClient.dropUserLabelPolicy(req);

        // Process response
        if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
          LOGGER.debug(
              "Successfully dropped label policy for user: {} with scope: {}", username, scope);
        } else {
          String errorMsg = "Failed to drop label policy from ConfigNode: " + status.getMessage();
          LOGGER.error(errorMsg);
          throw new Exception(errorMsg);
        }
      } catch (ClientManagerException | TException e) {
        String errorMsg =
            "Error connecting to ConfigNode for dropping label policy: " + e.getMessage();
        LOGGER.error(errorMsg);
        throw new Exception(errorMsg);
      }
    } catch (Exception e) {
      LOGGER.error("Error dropping user label policy for user: {}", username, e);
      throw e;
    }
  }
}
