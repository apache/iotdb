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
import org.apache.iotdb.commons.auth.entity.User;
import org.apache.iotdb.commons.client.exception.ClientManagerException;
import org.apache.iotdb.db.protocol.client.ConfigNodeClient;
import org.apache.iotdb.db.protocol.client.ConfigNodeClientManager;
import org.apache.iotdb.db.protocol.client.ConfigNodeInfo;
import org.apache.iotdb.db.queryengine.plan.execution.config.session.ShowUserLabelPolicyTask;
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
   * Gets a ConfigNodeClient instance.
   *
   * @return A ConfigNodeClient instance
   * @throws Exception If an error occurs during client acquisition
   */
  private static ConfigNodeClient getConfigNodeClient() throws Exception {
    try {
      return ConfigNodeClientManager.getInstance().borrowClient(ConfigNodeInfo.CONFIG_REGION_ID);
    } catch (ClientManagerException e) {
      String errorMsg = "Error connecting to ConfigNode: " + e.getMessage();
      LOGGER.error(errorMsg);
      throw new Exception(errorMsg);
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
   * Drops a user's label policy.
   *
   * @param username The username of the user
   * @param scope The scope of the label policy to drop
   * @throws Exception If an error occurs during the operation
   */
  public static void dropUserLabelPolicy(
      String username, ShowUserLabelPolicyStatement.LabelPolicyScope scope) throws Exception {
    org.apache.iotdb.confignode.rpc.thrift.TDropUserLabelPolicyReq req =
        new org.apache.iotdb.confignode.rpc.thrift.TDropUserLabelPolicyReq();
    req.setUsername(username);
    req.setScope(scope.toString());

    TSStatus status = getConfigNodeClient().dropUserLabelPolicy(req);
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new Exception(
          String.format(
              "Failed to drop user label policy for user %s, scope %s: %s",
              username, scope, status));
    }
  }

  /**
   * Sets or updates a user's label policy.
   *
   * @param username The username of the user
   * @param policyExpression The label policy expression
   * @param scope The scope of the label policy
   * @throws Exception If an error occurs during the operation
   */
  public static void setUserLabelPolicy(
      String username, String policyExpression, ShowUserLabelPolicyStatement.LabelPolicyScope scope)
      throws Exception {
    org.apache.iotdb.confignode.rpc.thrift.TSetUserLabelPolicyReq req =
        new org.apache.iotdb.confignode.rpc.thrift.TSetUserLabelPolicyReq();
    req.setUsername(username);
    req.setPolicyExpression(policyExpression);
    req.setScope(scope.toString());

    TSStatus status = getConfigNodeClient().setUserLabelPolicy(req);
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new Exception(
          String.format(
              "Failed to set user label policy for user %s, scope %s: %s",
              username, scope, status));
    }
  }

  /**
   * Get user information from ConfigNode This method fetches user information including label
   * policies from ConfigNode
   *
   * @param userName The username to fetch
   * @return User object or null if not found
   */
  public static User getUserFromConfigNode(String userName) {
    try {
      LOGGER.debug("Getting user {} from ConfigNode", userName);

      // Use the existing ConfigNode client to fetch user information
      // This should use the same RPC mechanism as other operations
      org.apache.iotdb.confignode.rpc.thrift.TShowUserLabelPolicyReq req =
          new org.apache.iotdb.confignode.rpc.thrift.TShowUserLabelPolicyReq();
      req.setUsername(userName);
      req.setScope("READ_WRITE"); // Get both read and write policies

      org.apache.iotdb.confignode.rpc.thrift.TShowUserLabelPolicyResp resp =
          getConfigNodeClient().showUserLabelPolicy(req);

      if (resp.getStatus().getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        // Create a User object with the fetched information
        User user = new User(userName, ""); // Password not needed for LBAC

        // Set label policies from response
        if (resp.getUserLabelPolicyList() != null) {
          for (org.apache.iotdb.confignode.rpc.thrift.TUserLabelPolicyInfo policyInfo :
              resp.getUserLabelPolicyList()) {
            String scope = policyInfo.getScope();
            String expression = policyInfo.getPolicyExpression();

            if ("READ".equals(scope)) {
              user.setReadLabelPolicyExpression(expression);
            } else if ("WRITE".equals(scope)) {
              user.setWriteLabelPolicyExpression(expression);
            } else if ("READ_WRITE".equals(scope)) {
              user.setReadLabelPolicyExpression(expression);
              user.setWriteLabelPolicyExpression(expression);
            }
          }
        }

        LOGGER.debug("Successfully fetched user {} with policies from ConfigNode", userName);
        return user;
      } else {
        LOGGER.warn("Failed to fetch user {} from ConfigNode: {}", userName, resp.getStatus());
        return null;
      }
    } catch (Exception e) {
      LOGGER.error("Error getting user {} from ConfigNode: {}", userName, e.getMessage(), e);
      return null;
    }
  }
}
