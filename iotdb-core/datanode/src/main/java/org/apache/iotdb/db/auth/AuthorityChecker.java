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
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.commons.service.metric.PerformanceOverviewMetrics;
import org.apache.iotdb.confignode.rpc.thrift.TAuthorizerResp;
import org.apache.iotdb.confignode.rpc.thrift.TPathPrivilege;
import org.apache.iotdb.confignode.rpc.thrift.TRoleResp;
import org.apache.iotdb.confignode.rpc.thrift.TUserResp;
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
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

  public static TSStatus checkAuthority(Statement statement, String userName) {
    long startTime = System.nanoTime();
    try {
      return statement.checkPermissionBeforeProcess(userName);
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
    if (noPermissionIndexList == null || noPermissionIndexList.isEmpty()) {
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
    return authorityFetcher
        .checkUserPathPrivileges(userName, Collections.singletonList(fullPath), permission)
        .isEmpty();
  }

  public static List<Integer> checkFullPathListPermission(
      String userName, List<PartialPath> fullPaths, int permission) {
    return authorityFetcher.checkUserPathPrivileges(userName, fullPaths, permission);
  }

  public static List<Integer> checkPatternPermission(
      String userName, List<PartialPath> pathPatterns, int permission) {
    return authorityFetcher.checkUserPathPrivileges(userName, pathPatterns, permission);
  }

  public static PathPatternTree getAuthorizedPathTree(String userName, int permission)
      throws AuthException {
    return authorityFetcher.getAuthorizedPatternTree(userName, permission);
  }

  public static boolean checkSystemPermission(String userName, int permission) {
    return authorityFetcher.checkUserSysPrivileges(userName, permission).getCode()
        == TSStatusCode.SUCCESS_STATUS.getStatusCode();
  }

  public static boolean checkGrantOption(
      String userName, String[] privilegeList, List<PartialPath> nodeNameList) {
    for (String s : privilegeList) {
      if (!authorityFetcher.checkUserPrivilegeGrantOpt(
          userName, nodeNameList, PrivilegeType.valueOf(s).ordinal())) {
        return false;
      }
    }
    return true;
  }

  public static boolean checkRole(String username, String rolename) {
    return authorityFetcher.checkRole(username, rolename);
  }

  public static void buildTSBlock(
      TAuthorizerResp authResp, SettableFuture<ConfigTaskResult> future) {
    List<TSDataType> types = new ArrayList<>();
    boolean listRoleUser =
        authResp.tag.equals(IoTDBConstant.COLUMN_ROLE)
            || authResp.tag.equals(IoTDBConstant.COLUMN_USER);

    List<ColumnHeader> headerList = new ArrayList<>();
    TsBlockBuilder builder;
    if (listRoleUser) {
      headerList.add(new ColumnHeader(authResp.getTag(), TSDataType.TEXT));
      types.add(TSDataType.TEXT);
      builder = new TsBlockBuilder(types);
      for (String name : authResp.getMemberInfo()) {
        builder.getTimeColumnBuilder().writeLong(0L);
        builder.getColumnBuilder(0).writeBinary(new Binary(name));
        builder.declarePosition();
      }
    } else {
      headerList.add(new ColumnHeader("ROLE", TSDataType.TEXT));
      types.add(TSDataType.TEXT);
      headerList.add(new ColumnHeader("PATH", TSDataType.TEXT));
      types.add(TSDataType.TEXT);
      headerList.add(new ColumnHeader("PRIVILEGES", TSDataType.TEXT));
      types.add(TSDataType.TEXT);
      headerList.add(new ColumnHeader("GRANT OPTION", TSDataType.BOOLEAN));
      types.add(TSDataType.BOOLEAN);
      builder = new TsBlockBuilder(types);
      TUserResp user = authResp.getPermissionInfo().getUserInfo();
      if (user != null) {
        appendPriBuilder("", "", user.getSysPriSet(), user.getSysPriSetGrantOpt(), builder);
        for (TPathPrivilege path : user.getPrivilegeList()) {
          appendPriBuilder("", path.getPath(), path.getPriSet(), path.getPriGrantOpt(), builder);
        }
      }
      Iterator<Map.Entry<String, TRoleResp>> it =
          authResp.getPermissionInfo().getRoleInfo().entrySet().iterator();
      while (it.hasNext()) {
        TRoleResp role = it.next().getValue();
        appendPriBuilder(
            role.getRoleName(), "", role.getSysPriSet(), role.getSysPriSetGrantOpt(), builder);
        for (TPathPrivilege path : role.getPrivilegeList()) {
          appendPriBuilder(
              role.getRoleName(), path.getPath(), path.getPriSet(), path.getPriGrantOpt(), builder);
        }
      }
    }
    DatasetHeader datasetHeader = new DatasetHeader(headerList, true);
    future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS, builder.build(), datasetHeader));
  }

  private static void appendPriBuilder(
      String name, String path, Set<Integer> priv, Set<Integer> grantOpt, TsBlockBuilder builder) {
    for (int i : priv) {
      builder.getColumnBuilder(0).writeBinary(new Binary(name));
      builder.getColumnBuilder(1).writeBinary(new Binary(path));
      builder.getColumnBuilder(2).writeBinary(new Binary(PrivilegeType.values()[i].toString()));
      builder.getColumnBuilder(3).writeBoolean(grantOpt.contains(i));
      builder.getTimeColumnBuilder().writeLong(0L);
      builder.declarePosition();
    }
  }
}
