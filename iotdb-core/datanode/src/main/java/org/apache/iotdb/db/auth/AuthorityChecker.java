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
import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.commons.service.metric.PerformanceOverviewMetrics;
import org.apache.iotdb.db.protocol.session.IClientSession;
import org.apache.iotdb.db.queryengine.common.header.ColumnHeader;
import org.apache.iotdb.db.queryengine.common.header.DatasetHeader;
import org.apache.iotdb.db.queryengine.plan.execution.config.ConfigTaskResult;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.AuthorStatement;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.utils.Binary;

import com.google.common.util.concurrent.SettableFuture;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

// Authority checker is SingleTon working at datanode.
// It checks permission in local. DCL statement will send to confignode.
public class AuthorityChecker {

  public static final String SUPER_USER = CommonDescriptor.getInstance().getConfig().getAdminName();

  private static final String NO_PERMISSION_PROMOTION =
      "No permissions for this operation, please add privilege ";

  private static final IAuthorityFetcher authorityFetcher =
      new ClusterAuthorityFetcher(new BasicAuthorityCache());

  private static final PerformanceOverviewMetrics PERFORMANCE_OVERVIEW_METRICS =
      PerformanceOverviewMetrics.getInstance();

  private AuthorityChecker() {
    // empty constructor
  }

  public static IAuthorityFetcher getAuthorityFetcher() {
    return authorityFetcher;
  }

  public static boolean invalidateCache(String username, String rolename) {
    return authorityFetcher.getAuthorCache().invalidateCache(username, rolename);
  }

  public static TSStatus checkUser(String userName, String password) {
    return authorityFetcher.checkUser(userName, password);
  }

  public static SettableFuture<ConfigTaskResult> queryPermission(AuthorStatement authorStatement) {
    return authorityFetcher.queryPermission(authorStatement);
  }

  public static SettableFuture<ConfigTaskResult> operatePermission(
      AuthorStatement authorStatement) {
    return authorityFetcher.operatePermission(authorStatement);
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

  public static TSStatus getTSStatus(boolean hasPermission, PrivilegeType neededPrivilege) {
    return hasPermission
        ? new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode())
        : new TSStatus(TSStatusCode.NO_PERMISSION.getStatusCode())
            .setMessage(NO_PERMISSION_PROMOTION + neededPrivilege);
  }

  public static TSStatus getTSStatus(
      boolean hasPermission, PartialPath path, PrivilegeType neededPrivilege) {
    return hasPermission
        ? new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode())
        : new TSStatus(TSStatusCode.NO_PERMISSION.getStatusCode())
            .setMessage(NO_PERMISSION_PROMOTION + neededPrivilege + " on " + path);
  }

  public static TSStatus getTSStatus(
      List<Integer> noPermissionIndexList,
      List<PartialPath> pathList,
      PrivilegeType neededPrivilege) {
    if (noPermissionIndexList.isEmpty()) {
      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    }

    StringBuilder prompt = new StringBuilder(NO_PERMISSION_PROMOTION);
    prompt.append(neededPrivilege);
    prompt.append(" on [");
    prompt.append(pathList.get(noPermissionIndexList.get(0)));
    for (int i = 1; i < noPermissionIndexList.size(); i++) {
      prompt.append(", ");
      prompt.append(pathList.get(noPermissionIndexList.get(i)));
    }
    prompt.append("]");
    return new TSStatus(TSStatusCode.NO_PERMISSION.getStatusCode()).setMessage(prompt.toString());
  }

  public static boolean checkFullPathPermission(
      String userName, PartialPath fullPath, int permission) {
    long startTime = System.nanoTime();
    List<PartialPath> path = new ArrayList<>();
    path.add(fullPath);
    List<Integer> failIndex = authorityFetcher.checkUserPathPrivileges(userName, path, permission);
    PERFORMANCE_OVERVIEW_METRICS.recordAuthCost(System.nanoTime() - startTime);
    return failIndex.isEmpty();
  }

  public static List<Integer> checkFullPathListPermission(
      String userName, List<PartialPath> fullPaths, int permission) {
    long startTime = System.nanoTime();
    List<Integer> failIndex =
        authorityFetcher.checkUserPathPrivileges(userName, fullPaths, permission);
    PERFORMANCE_OVERVIEW_METRICS.recordAuthCost(System.nanoTime() - startTime);
    return failIndex;
  }

  public static List<Integer> checkPatternPermission(
      String userName, List<PartialPath> pathPatterns, int permission) {
    long startTime = System.nanoTime();
    List<Integer> failIndex =
        authorityFetcher.checkUserPathPrivileges(userName, pathPatterns, permission);
    PERFORMANCE_OVERVIEW_METRICS.recordAuthCost(System.nanoTime() - startTime);
    return failIndex;
  }

  public static PathPatternTree getAuthorizedPathTree(String userName, int permission)
      throws AuthException {
    return authorityFetcher.getAuthorizedPatternTree(userName, permission);
  }

  public static boolean checkSystemPermission(String userName, int permission) {
    long startTime = System.nanoTime();
    TSStatus status = authorityFetcher.checkUserSysPrivileges(userName, permission);
    PERFORMANCE_OVERVIEW_METRICS.recordAuthCost(System.nanoTime() - startTime);
    return status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode();
  }

  public static boolean checkGrantOption(
      String userName, String[] privilegeList, List<PartialPath> nodeNameList) {
    long startTime = System.nanoTime();
    for (String s : privilegeList) {
      if (!authorityFetcher.checkUserPrivilegeGrantOpt(
          userName, nodeNameList, PrivilegeType.valueOf(s).ordinal())) {
        return false;
      }
    }
    PERFORMANCE_OVERVIEW_METRICS.recordAuthCost(System.nanoTime() - startTime);
    return true;
  }

  public static void buildTSBlock(
      Map<String, List<String>> authorizerInfo, SettableFuture<ConfigTaskResult> future) {
    List<TSDataType> types = new ArrayList<>();
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
    for (String value : authorizerInfo.get(headerList.get(0).getColumnName())) {
      builder.getTimeColumnBuilder().writeLong(0L);
      builder.getColumnBuilder(0).writeBinary(new Binary(value));
      builder.declarePosition();
    }
    for (int i = 1; i < headerList.size(); i++) {
      for (String value : authorizerInfo.get(headerList.get(i).getColumnName())) {
        builder.getColumnBuilder(i).writeBinary(new Binary(value));
      }
    }

    DatasetHeader datasetHeader = new DatasetHeader(headerList, true);
    future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS, builder.build(), datasetHeader));
  }
}
