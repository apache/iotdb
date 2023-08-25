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
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.commons.service.metric.PerformanceOverviewMetrics;
import org.apache.iotdb.db.protocol.session.IClientSession;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class AuthorityChecker {

  public static final String SUPER_USER = CommonDescriptor.getInstance().getConfig().getAdminName();
  private static final Logger logger = LoggerFactory.getLogger(AuthorityChecker.class);

  private static final AuthorizerManager authorizerManager = AuthorizerManager.getInstance();

  private static final PerformanceOverviewMetrics PERFORMANCE_OVERVIEW_METRICS =
      PerformanceOverviewMetrics.getInstance();

  private AuthorityChecker() {
    // Empty constructor
  }

  /** Check whether specific Session has the authorization to given plan. */
  public static TSStatus checkAuthority(Statement statement, IClientSession session) {
    long startTime = System.nanoTime();
    try {
      return statement.checkPermissionBeforeProcess(session.getUsername());
    } finally {
      PERFORMANCE_OVERVIEW_METRICS.recordAuthCost(System.nanoTime() - startTime);
    }
  }

  public static TSStatus getTSStatus(boolean hasPermission, String errMsg) {
    return hasPermission
        ? new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode())
        : new TSStatus(TSStatusCode.NO_PERMISSION.getStatusCode()).setMessage(errMsg);
  }

  public static TSStatus getTSStatus(boolean hasPermission, PrivilegeType[] neededPrivileges) {
    StringBuilder prompt =
        new StringBuilder("No permissions for this operation, please add privilege ");
    prompt.append(neededPrivileges[0]);
    for (int i = 1; i < neededPrivileges.length; i++) {
      prompt.append(" and ");
      prompt.append(neededPrivileges[i]);
    }
    return hasPermission
        ? new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode())
        : new TSStatus(TSStatusCode.NO_PERMISSION.getStatusCode()).setMessage(prompt.toString());
  }

  public static boolean checkFullPathPermission(
      String username, PartialPath fullPath, int permission) {
    // TODO
    return true;
  }

  public static boolean checkFullPathListPermission(
      String username, List<PartialPath> fullPaths, int permission) {
    // TODO
    return true;
  }

  public static boolean checkPatternPermission(
      String username, List<PartialPath> pathPatterns, int permission) {
    // TODO
    return true;
  }

  public static PathPatternTree getAuthorizedPathTree(String username, int permission) {
    // TODO
    return new PathPatternTree();
  }

  public static boolean checkSystemPermission(String username, int permission) {
    // TODO
    return true;
  }
}
