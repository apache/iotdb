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
import org.apache.iotdb.commons.auth.entity.User;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.commons.path.PathPatternTreeUtils;
import org.apache.iotdb.commons.schema.SchemaConstant;
import org.apache.iotdb.commons.schema.column.ColumnHeader;
import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.commons.service.metric.PerformanceOverviewMetrics;
import org.apache.iotdb.confignode.rpc.thrift.TAuthorizerResp;
import org.apache.iotdb.confignode.rpc.thrift.TDBPrivilege;
import org.apache.iotdb.confignode.rpc.thrift.TGetDatabaseReq;
import org.apache.iotdb.confignode.rpc.thrift.TPathPrivilege;
import org.apache.iotdb.confignode.rpc.thrift.TRoleResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowDatabaseResp;
import org.apache.iotdb.confignode.rpc.thrift.TTablePrivilege;
import org.apache.iotdb.confignode.rpc.thrift.TUserResp;
import org.apache.iotdb.db.pipe.source.dataregion.realtime.listener.PipeInsertionDataNodeListener;
import org.apache.iotdb.db.protocol.client.ConfigNodeClient;
import org.apache.iotdb.db.protocol.client.ConfigNodeClientManager;
import org.apache.iotdb.db.protocol.client.ConfigNodeInfo;
import org.apache.iotdb.db.protocol.session.IClientSession;
import org.apache.iotdb.db.queryengine.common.header.DatasetHeader;
import org.apache.iotdb.db.queryengine.plan.execution.config.ConfigTaskResult;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.RelationalAuthorStatement;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.AuthorStatement;
import org.apache.iotdb.rpc.TSStatusCode;

import com.google.common.util.concurrent.SettableFuture;
import org.apache.ratis.util.MemoizedSupplier;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.utils.Binary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.iotdb.commons.schema.column.ColumnHeaderConstant.LIST_USER_OR_ROLE_PRIVILEGES_COLUMN_HEADERS;

// Authority checker is SingleTon working at datanode.
// It checks permission in local. DCL statement will send to configNode.
public class AuthorityChecker {

  private static final Logger LOGGER = LoggerFactory.getLogger(AuthorityChecker.class);

  public static final String SUPER_USER = CommonDescriptor.getInstance().getConfig().getAdminName();

  public static final TSStatus SUCCEED = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());

  public static final String ONLY_ADMIN_ALLOWED =
      "No permissions for this operation, only root user is allowed";

  private static final String NO_PERMISSION_PROMOTION =
      "No permissions for this operation, please add privilege ";

  private static final String NO_GRANT_OPT_PERMISSION_PROMOTION =
      "No permissions for this operation, please add grant option to privilege ";

  private static final String ROOT_PATH_PATTERN = "root.**";

  private static final int LBAC_DENIAL_OFFSET = -1;

  private static final String LBAC_POLICY_VIOLATION_PREFIX =
      "LBAC policy violation, please check label policies on [";

  private static final String INVALID_STATEMENT_MESSAGE = "Invalid statement";

  private static final String INVALID_USERNAME_MESSAGE = "Invalid username";

  private static final MemoizedSupplier<IAuthorityFetcher> authorityFetcher =
      MemoizedSupplier.valueOf(() -> new ClusterAuthorityFetcher(new BasicAuthorityCache()));

  private static final PerformanceOverviewMetrics PERFORMANCE_OVERVIEW_METRICS =
      PerformanceOverviewMetrics.getInstance();

  private AuthorityChecker() {
    // empty constructor
  }

  private static boolean isSuperUser(String userName) {
    return userName != null && SUPER_USER.equals(userName);
  }

  private static boolean isValidPath(PartialPath path) {
    return path != null && path.getFullPath() != null;
  }

  private static boolean isValidPermissionParams(String userName, PrivilegeType permission) {
    return userName != null && userName.trim().length() > 0 && permission != null;
  }

  private static PathPatternTree createAllPathsTree() {
    PathPatternTree allPathsTree = new PathPatternTree();
    try {
      allPathsTree.appendPathPattern(new PartialPath(ROOT_PATH_PATTERN));
    } catch (IllegalPathException e) {
      throw new RuntimeException(e);
    }
    return allPathsTree;
  }

  private static LbacOperationClassifier.OperationType determineOperationType(
      PrivilegeType permission) {
    if (permission == null) {
      return LbacOperationClassifier.OperationType.BOTH;
    }

    String permissionName = permission.name();
    if (permissionName.contains("READ")) {
      return LbacOperationClassifier.OperationType.READ;
    } else if (permissionName.contains("WRITE")) {
      return LbacPermissionChecker.convertPrivilegeTypeToLbacOperation(permission);
    } else {
      return LbacOperationClassifier.OperationType.BOTH;
    }
  }

  private static TSStatus handleAuthException(String operation, String userName, Exception e) {
    return new TSStatus(TSStatusCode.NO_PERMISSION.getStatusCode())
        .setMessage(operation + " failed: " + e.getMessage());
  }

  private static List<String> getAllDatabasePaths() {
    try {

      try (ConfigNodeClient configNodeClient =
          ConfigNodeClientManager.getInstance().borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {

        TGetDatabaseReq req =
            new TGetDatabaseReq(
                    Arrays.asList("root", "**"), SchemaConstant.ALL_MATCH_SCOPE.serialize())
                .setIsTableModel(false);

        TShowDatabaseResp resp = configNodeClient.showDatabase(req);

        if (resp.getStatus().getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
          return new ArrayList<>(resp.getDatabaseInfoMap().keySet());
        } else {

          return new ArrayList<>();
        }
      }
    } catch (Exception e) {
      return new ArrayList<>();
    }
  }

  // ==================== Core Authority Check Methods ====================

  public static IAuthorityFetcher getAuthorityFetcher() {
    return authorityFetcher.get();
  }

  public static boolean invalidateCache(String username, String roleName) {
    PipeInsertionDataNodeListener.getInstance().invalidateAllCache();
    return authorityFetcher.get().getAuthorCache().invalidateCache(username, roleName);
  }

  public static TSStatus checkUser(String userName, String password) {
    return authorityFetcher.get().checkUser(userName, password);
  }

  public static SettableFuture<ConfigTaskResult> queryPermission(AuthorStatement authorStatement) {
    return authorityFetcher.get().queryPermission(authorStatement);
  }

  public static SettableFuture<ConfigTaskResult> operatePermission(
      AuthorStatement authorStatement) {
    return authorityFetcher.get().operatePermission(authorStatement);
  }

  public static SettableFuture<ConfigTaskResult> queryPermission(
      RelationalAuthorStatement authorStatement) {
    return authorityFetcher.get().queryPermission(authorStatement);
  }

  public static SettableFuture<ConfigTaskResult> operatePermission(
      RelationalAuthorStatement authorStatement) {
    return authorityFetcher.get().operatePermission(authorStatement);
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
    if (statement == null) {
      return new TSStatus(TSStatusCode.NO_PERMISSION.getStatusCode())
          .setMessage(INVALID_STATEMENT_MESSAGE);
    }

    if (userName == null || userName.trim().isEmpty()) {
      return new TSStatus(TSStatusCode.NO_PERMISSION.getStatusCode())
          .setMessage(INVALID_USERNAME_MESSAGE);
    }

    // First check RBAC permissions
    TSStatus rbacStatus = RbacChecker.checkAuthority(statement, userName);
    if (rbacStatus.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return rbacStatus;
    }

    // Then check LBAC permissions if enabled
    if (LbacPermissionChecker.isLbacEnabled()) {
      TSStatus lbacStatus = LbacChecker.checkAuthority(statement, userName);
      if (lbacStatus.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return lbacStatus;
      }
    }
    return SUCCEED;
  }

  // ==================== Status Generation Methods ====================

  public static TSStatus getGrantOptTSStatus(boolean hasGrantOpt, PrivilegeType privilegeType) {
    return hasGrantOpt
        ? SUCCEED
        : new TSStatus(TSStatusCode.NOT_HAS_PRIVILEGE_GRANTOPT.getStatusCode())
            .setMessage(NO_GRANT_OPT_PERMISSION_PROMOTION + privilegeType);
  }

  public static TSStatus getTSStatus(boolean hasPermission, String errMsg) {
    return hasPermission
        ? SUCCEED
        : new TSStatus(TSStatusCode.NO_PERMISSION.getStatusCode()).setMessage(errMsg);
  }

  public static TSStatus getTSStatus(boolean hasPermission, PrivilegeType neededPrivilege) {
    return hasPermission
        ? SUCCEED
        : new TSStatus(TSStatusCode.NO_PERMISSION.getStatusCode())
            .setMessage(NO_PERMISSION_PROMOTION + neededPrivilege);
  }

  public static TSStatus getGrantOptTSStatus(
      boolean hasPermission, PrivilegeType neededPrivilege, String database) {
    return hasPermission
        ? SUCCEED
        : new TSStatus(TSStatusCode.NOT_HAS_PRIVILEGE_GRANTOPT.getStatusCode())
            .setMessage(NO_GRANT_OPT_PERMISSION_PROMOTION + neededPrivilege + " ON DB:" + database);
  }

  public static TSStatus getGrantOptTSStatus(
      boolean hasPermission, PrivilegeType neededPrivilege, String database, String table) {
    return hasPermission
        ? SUCCEED
        : new TSStatus(TSStatusCode.NOT_HAS_PRIVILEGE_GRANTOPT.getStatusCode())
            .setMessage(
                NO_GRANT_OPT_PERMISSION_PROMOTION
                    + neededPrivilege
                    + " ON "
                    + database
                    + "."
                    + table);
  }

  public static TSStatus getTSStatus(
      boolean hasPermission, PrivilegeType neededPrivilege, String database) {
    return hasPermission
        ? SUCCEED
        : new TSStatus(TSStatusCode.NO_PERMISSION.getStatusCode())
            .setMessage(NO_PERMISSION_PROMOTION + neededPrivilege + " ON DB:" + database);
  }

  public static TSStatus getTSStatus(
      boolean hasPermission, PrivilegeType neededPrivilege, String database, String table) {
    return hasPermission
        ? SUCCEED
        : new TSStatus(TSStatusCode.NO_PERMISSION.getStatusCode())
            .setMessage(
                NO_PERMISSION_PROMOTION + neededPrivilege + " ON " + database + "." + table);
  }

  public static TSStatus getTSStatus(
      boolean hasPermission, PartialPath path, PrivilegeType neededPrivilege) {
    return hasPermission
        ? SUCCEED
        : new TSStatus(TSStatusCode.NO_PERMISSION.getStatusCode())
            .setMessage(NO_PERMISSION_PROMOTION + neededPrivilege + " on " + path);
  }

  public static TSStatus getTSStatus(
      List<Integer> noPermissionIndexList,
      List<? extends PartialPath> pathList,
      PrivilegeType neededPrivilege) {
    if (noPermissionIndexList == null || noPermissionIndexList.isEmpty()) {
      return SUCCEED;
    }

    if (pathList == null) {
      return new TSStatus(TSStatusCode.NO_PERMISSION.getStatusCode())
          .setMessage("Invalid path list");
    }

    // Check if this is LBAC permission denial (negative indices)
    boolean isLbacDenial = noPermissionIndexList.stream().anyMatch(index -> index < 0);

    if (isLbacDenial) {
      // Handle LBAC permission denial
      StringBuilder prompt = new StringBuilder(LBAC_POLICY_VIOLATION_PREFIX);
      // Convert negative indices back to positive for path lookup
      int firstIndex = Math.abs(noPermissionIndexList.get(0) + 1);
      if (firstIndex < pathList.size()) {
        prompt.append(pathList.get(firstIndex));
        for (int i = 1; i < noPermissionIndexList.size(); i++) {
          prompt.append(", ");
          int index = Math.abs(noPermissionIndexList.get(i) + 1);
          if (index < pathList.size()) {
            prompt.append(pathList.get(index));
          }
        }
      }
      prompt.append("]");
      return new TSStatus(TSStatusCode.NO_PERMISSION.getStatusCode()).setMessage(prompt.toString());
    } else {
      // Handle RBAC permission denial (original logic)
      StringBuilder prompt = new StringBuilder(NO_PERMISSION_PROMOTION);
      prompt.append(neededPrivilege);
      prompt.append(" on [");
      int firstIndex = noPermissionIndexList.get(0);
      if (firstIndex < pathList.size()) {
        prompt.append(pathList.get(firstIndex));
        for (int i = 1; i < noPermissionIndexList.size(); i++) {
          prompt.append(", ");
          int index = noPermissionIndexList.get(i);
          if (index < pathList.size()) {
            prompt.append(pathList.get(index));
          }
        }
      }
      prompt.append("]");
      return new TSStatus(TSStatusCode.NO_PERMISSION.getStatusCode()).setMessage(prompt.toString());
    }
  }

  // ==================== Path Permission Check Methods ====================

  public static boolean checkFullPathOrPatternPermission(
      String userName, PartialPath fullPath, PrivilegeType permission) {
    if (!isValidPermissionParams(userName, permission) || !isValidPath(fullPath)) {
      return false;
    }

    // Check RBAC permissions first
    boolean rbacResult =
        RbacChecker.checkFullPathOrPatternPermission(userName, fullPath, permission);
    if (!rbacResult) {
      return false;
    }

    // Check LBAC permissions if enabled
    if (LbacPermissionChecker.isLbacEnabled()) {
      return LbacChecker.checkFullPathOrPatternPermission(userName, fullPath, permission);
    }

    return true;
  }

  public static List<Integer> checkFullPathOrPatternListPermission(
      String userName, List<? extends PartialPath> pathPatterns, PrivilegeType permission) {
    if (!isValidPermissionParams(userName, permission) || pathPatterns == null) {
      return new ArrayList<>();
    }

    // Check RBAC permissions first
    List<Integer> rbacResult =
        RbacChecker.checkFullPathOrPatternListPermission(userName, pathPatterns, permission);
    if (!rbacResult.isEmpty()) {
      return rbacResult;
    }

    // Check LBAC permissions if enabled
    if (LbacPermissionChecker.isLbacEnabled()) {
      List<Integer> lbacResult =
          LbacChecker.checkFullPathOrPatternListPermission(userName, pathPatterns, permission);
      if (!lbacResult.isEmpty()) {
        // Mark LBAC denied paths with negative indices to distinguish from RBAC
        List<Integer> lbacDeniedPaths = new ArrayList<>();
        for (Integer index : lbacResult) {
          lbacDeniedPaths.add(index + LBAC_DENIAL_OFFSET); // 使用常量
        }
        return lbacDeniedPaths;
      }
    }

    return new ArrayList<>();
  }

  public static PathPatternTree getAuthorizedPathTree(String userName, PrivilegeType permission)
      throws AuthException {
    // Get RBAC authorized path tree
    PathPatternTree rbacTree = RbacChecker.getAuthorizedPathTree(userName, permission);

    // Get LBAC authorized path tree if enabled
    if (LbacPermissionChecker.isLbacEnabled()) {
      PathPatternTree lbacTree = LbacChecker.getAuthorizedPathTree(userName, permission);
      // Intersect RBAC and LBAC trees to get the final authorized path tree
      return PathPatternTreeUtils.intersectWithFullPathPrefixTree(rbacTree, lbacTree);
    }

    return rbacTree;
  }

  // ==================== System Permission Check Methods (RBAC Only) ==================== //

  public static boolean checkSystemPermission(String userName, PrivilegeType permission) {
    // System permissions only check RBAC
    return RbacChecker.checkSystemPermission(userName, permission);
  }

  public static boolean checkSystemPermissionGrantOption(
      String userName, PrivilegeType permission) {
    // System permission grant options only check RBAC
    return RbacChecker.checkSystemPermissionGrantOption(userName, permission);
  }

  public static boolean checkAnyScopePermissionGrantOption(
      String userName, PrivilegeType permission) {
    // Any scope permission grant options only check RBAC
    return RbacChecker.checkAnyScopePermissionGrantOption(userName, permission);
  }

  // ==================== Database Permission Check Methods ====================

  public static boolean checkDBPermission(
      String userName, String database, PrivilegeType permission) {
    if (!isValidPermissionParams(userName, permission) || database == null) {
      return false;
    }

    // Check RBAC permissions first
    boolean rbacResult = RbacChecker.checkDBPermission(userName, database, permission);
    if (!rbacResult) {
      return false;
    }

    // Check LBAC permissions if enabled
    if (LbacPermissionChecker.isLbacEnabled()) {
      return LbacChecker.checkDBPermission(userName, database, permission);
    }

    return true;
  }

  public static boolean checkDBPermissionGrantOption(
      String userName, String database, PrivilegeType permission) {
    // Database permission grant options only check RBAC
    return RbacChecker.checkDBPermissionGrantOption(userName, database, permission);
  }

  // ==================== Table Permission Check Methods ====================

  public static boolean checkTablePermission(
      String userName, String database, String table, PrivilegeType permission) {
    if (!isValidPermissionParams(userName, permission) || database == null || table == null) {
      return false;
    }

    // Check RBAC permissions first
    boolean rbacResult = RbacChecker.checkTablePermission(userName, database, table, permission);
    if (!rbacResult) {
      return false;
    }

    // Check LBAC permissions if enabled (only at database level)
    if (LbacPermissionChecker.isLbacEnabled()) {
      return LbacChecker.checkTablePermission(userName, database, table, permission);
    }

    return true;
  }

  public static boolean checkTablePermissionGrantOption(
      String userName, String database, String table, PrivilegeType permission) {
    // Table permission grant options only check RBAC
    return RbacChecker.checkTablePermissionGrantOption(userName, database, table, permission);
  }

  // ==================== Visibility Check Methods ====================

  public static boolean checkDBVisible(String userName, String database) {
    // Check RBAC visibility first
    boolean rbacResult = RbacChecker.checkDBVisible(userName, database);
    if (!rbacResult) {
      return false;
    }

    // Check LBAC visibility if enabled
    if (LbacPermissionChecker.isLbacEnabled()) {
      return LbacChecker.checkDBVisible(userName, database);
    }

    return true;
  }

  public static boolean checkTableVisible(String userName, String database, String table) {
    // Check RBAC visibility first
    boolean rbacResult = RbacChecker.checkTableVisible(userName, database, table);
    if (!rbacResult) {
      return false;
    }

    // Check LBAC visibility if enabled (only at database level)
    if (LbacPermissionChecker.isLbacEnabled()) {
      return LbacChecker.checkTableVisible(userName, database, table);
    }

    return true;
  }

  // ==================== Path Permission Grant Option Methods (RBAC Only) ==================== //

  public static boolean checkPathPermissionGrantOption(
      String userName, PrivilegeType privilegeType, List<PartialPath> nodeNameList) {
    // Path permission grant options only check RBAC
    return RbacChecker.checkPathPermissionGrantOption(userName, privilegeType, nodeNameList);
  }

  // ==================== Role Check Methods (RBAC Only) ====================

  public static boolean checkRole(String username, String roleName) {
    // Role checks only check RBAC
    return RbacChecker.checkRole(username, roleName);
  }

  public static TSStatus checkSuperUserOrMaintain(String userName) {
    // Super user or maintain permission only check RBAC
    return RbacChecker.checkSuperUserOrMaintain(userName);
  }

  // ==================== Result Building Methods ====================

  public static void buildTSBlock(
      TAuthorizerResp authResp, SettableFuture<ConfigTaskResult> future) {
    List<TSDataType> types = new ArrayList<>();
    boolean listRoleUser =
        authResp.tag.equals(ColumnHeaderConstant.ROLE)
            || authResp.tag.equals(ColumnHeaderConstant.USER);

    List<ColumnHeader> headerList = new ArrayList<>();
    TsBlockBuilder builder;
    if (listRoleUser) {
      headerList.add(new ColumnHeader(authResp.getTag(), TSDataType.TEXT));
      types.add(TSDataType.TEXT);
      builder = new TsBlockBuilder(types);
      for (String name : authResp.getMemberInfo()) {
        builder.getTimeColumnBuilder().writeLong(0L);
        builder.getColumnBuilder(0).writeBinary(new Binary(name, TSFileConfig.STRING_CHARSET));
        builder.declarePosition();
      }
    } else {
      headerList = LIST_USER_OR_ROLE_PRIVILEGES_COLUMN_HEADERS;
      types =
          LIST_USER_OR_ROLE_PRIVILEGES_COLUMN_HEADERS.stream()
              .map(ColumnHeader::getColumnType)
              .collect(Collectors.toList());
      builder = new TsBlockBuilder(types);
      TUserResp user = authResp.getPermissionInfo().getUserInfo();
      if (user != null) {
        appendEntryInfo("", user.getPermissionInfo(), builder);
      }
      for (Map.Entry<String, TRoleResp> stringTRoleRespEntry :
          authResp.getPermissionInfo().getRoleInfo().entrySet()) {
        TRoleResp role = stringTRoleRespEntry.getValue();
        appendEntryInfo(role.getName(), role, builder);
      }
    }
    DatasetHeader datasetHeader = new DatasetHeader(headerList, true);
    future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS, builder.build(), datasetHeader));
  }

  private static void appendPriBuilder(
      String name, String scope, Set<Integer> priv, Set<Integer> grantOpt, TsBlockBuilder builder) {
    for (int i : priv) {
      builder.getColumnBuilder(0).writeBinary(new Binary(name, TSFileConfig.STRING_CHARSET));
      builder.getColumnBuilder(1).writeBinary(new Binary(scope, TSFileConfig.STRING_CHARSET));
      builder
          .getColumnBuilder(2)
          .writeBinary(
              new Binary(PrivilegeType.values()[i].toString(), TSFileConfig.STRING_CHARSET));
      builder.getColumnBuilder(3).writeBoolean(grantOpt.contains(i));
      builder.getTimeColumnBuilder().writeLong(0L);
      builder.declarePosition();
    }
  }

  private static void appendEntryInfo(String name, TRoleResp resp, TsBlockBuilder builder) {
    // System privilege.
    appendPriBuilder(name, "", resp.getSysPriSet(), resp.getSysPriSetGrantOpt(), builder);
    // Any scope privilege.
    appendPriBuilder(name, "*.*", resp.getAnyScopeSet(), resp.getAnyScopeGrantSet(), builder);
    // Path privilege.
    for (TPathPrivilege path : resp.getPrivilegeList()) {
      appendPriBuilder(name, path.getPath(), path.getPriSet(), path.getPriGrantOpt(), builder);
    }
    for (Map.Entry<String, TDBPrivilege> entry : resp.getDbPrivilegeMap().entrySet()) {
      TDBPrivilege priv = entry.getValue();
      appendPriBuilder(
          name, entry.getKey() + ".*", priv.getPrivileges(), priv.getGrantOpt(), builder);
      for (Map.Entry<String, TTablePrivilege> tbEntry : priv.getTablePrivilegeMap().entrySet()) {
        TTablePrivilege tb = tbEntry.getValue();
        appendPriBuilder(
            name,
            entry.getKey() + "." + tbEntry.getKey(),
            tb.getPrivileges(),
            tb.getGrantOption(),
            builder);
      }
    }
  }

  // ==================== RBAC Internal Class ====================

  /** RBAC (Role-Based Access Control) internal class Handles role-based access control logic */
  public static class RbacChecker {

    private RbacChecker() {
      // Private constructor to prevent instantiation
    }

    /** Check RBAC authority for a statement */
    public static TSStatus checkAuthority(Statement statement, String userName) {
      return statement.checkPermissionBeforeProcess(userName);
    }

    /** Check full path or pattern permission */
    public static boolean checkFullPathOrPatternPermission(
        String userName, PartialPath fullPath, PrivilegeType permission) {
      return authorityFetcher
          .get()
          .checkUserPathPrivileges(userName, Collections.singletonList(fullPath), permission)
          .isEmpty();
    }

    /** Check full path or pattern list permission */
    public static List<Integer> checkFullPathOrPatternListPermission(
        String userName, List<? extends PartialPath> pathPatterns, PrivilegeType permission) {
      return authorityFetcher.get().checkUserPathPrivileges(userName, pathPatterns, permission);
    }

    /** Get authorized path tree */
    public static PathPatternTree getAuthorizedPathTree(String userName, PrivilegeType permission)
        throws AuthException {
      return authorityFetcher.get().getAuthorizedPatternTree(userName, permission);
    }

    /** Check system permission */
    public static boolean checkSystemPermission(String userName, PrivilegeType permission) {
      return authorityFetcher.get().checkUserSysPrivileges(userName, permission).getCode()
          == TSStatusCode.SUCCESS_STATUS.getStatusCode();
    }

    /** Check system permission grant option */
    public static boolean checkSystemPermissionGrantOption(
        String userName, PrivilegeType permission) {
      return authorityFetcher.get().checkUserSysPrivilegesGrantOpt(userName, permission).getCode()
          == TSStatusCode.SUCCESS_STATUS.getStatusCode();
    }

    /** Check any scope permission grant option */
    public static boolean checkAnyScopePermissionGrantOption(
        String userName, PrivilegeType permission) {
      return authorityFetcher
              .get()
              .checkUserAnyScopePrivilegeGrantOption(userName, permission)
              .getCode()
          == TSStatusCode.SUCCESS_STATUS.getStatusCode();
    }

    /** Check database permission */
    public static boolean checkDBPermission(
        String userName, String database, PrivilegeType permission) {
      return authorityFetcher.get().checkUserDBPrivileges(userName, database, permission).getCode()
          == TSStatusCode.SUCCESS_STATUS.getStatusCode();
    }

    /** Check database permission grant option */
    public static boolean checkDBPermissionGrantOption(
        String userName, String database, PrivilegeType permission) {
      return authorityFetcher
              .get()
              .checkUserDBPrivilegesGrantOpt(userName, database, permission)
              .getCode()
          == TSStatusCode.SUCCESS_STATUS.getStatusCode();
    }

    /** Check table permission */
    public static boolean checkTablePermission(
        String userName, String database, String table, PrivilegeType permission) {
      return authorityFetcher
              .get()
              .checkUserTBPrivileges(userName, database, table, permission)
              .getCode()
          == TSStatusCode.SUCCESS_STATUS.getStatusCode();
    }

    /** Check table permission grant option */
    public static boolean checkTablePermissionGrantOption(
        String userName, String database, String table, PrivilegeType permission) {
      return authorityFetcher
              .get()
              .checkUserTBPrivilegesGrantOpt(userName, database, table, permission)
              .getCode()
          == TSStatusCode.SUCCESS_STATUS.getStatusCode();
    }

    /** Check database visibility */
    public static boolean checkDBVisible(String userName, String database) {
      return authorityFetcher.get().checkDBVisible(userName, database).getCode()
          == TSStatusCode.SUCCESS_STATUS.getStatusCode();
    }

    /** Check table visibility */
    public static boolean checkTableVisible(String userName, String database, String table) {
      return authorityFetcher.get().checkTBVisible(userName, database, table).getCode()
          == TSStatusCode.SUCCESS_STATUS.getStatusCode();
    }

    /** Check path permission grant option */
    public static boolean checkPathPermissionGrantOption(
        String userName, PrivilegeType privilegeType, List<PartialPath> nodeNameList) {
      return authorityFetcher
              .get()
              .checkUserPathPrivilegesGrantOpt(userName, nodeNameList, privilegeType)
              .getCode()
          == TSStatusCode.SUCCESS_STATUS.getStatusCode();
    }

    /** Check role */
    public static boolean checkRole(String username, String roleName) {
      return authorityFetcher.get().checkRole(username, roleName);
    }

    /** Check super user or maintain permission */
    public static TSStatus checkSuperUserOrMaintain(String userName) {
      if (AuthorityChecker.SUPER_USER.equals(userName)) {
        return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
      }
      return AuthorityChecker.getTSStatus(
          AuthorityChecker.checkSystemPermission(userName, PrivilegeType.MAINTAIN),
          PrivilegeType.MAINTAIN);
    }
  }

  // ==================== LBAC Internal Class ====================

  /**
   * LBAC (Label-Based Access Control) internal class Handles label-based access control logic at
   * database level only Mimics RBAC structure to provide consistent interface
   */
  public static class LbacChecker {

    private LbacChecker() {
      // Private constructor to prevent instantiation
    }

    /** Check LBAC authority for a statement */
    public static TSStatus checkAuthority(Statement statement, String userName) {
      try {
        // Check if LBAC is enabled
        if (!LbacPermissionChecker.isLbacEnabled()) {}

        // Check if it's super user
        if (isSuperUser(userName)) {
          return SUCCEED;
        }

        // Check if it's a database-related operation
        if (!LbacOperationClassifier.isLbacRelevantOperation(statement)) {
          return SUCCEED;
        }

        // Execute LBAC permission check
        return LbacPermissionChecker.checkLbacPermissionForStatement(statement, userName);

      } catch (Exception e) {
        return handleAuthException("LBAC check", userName, e);
      }
    }

    /** Check full path or pattern permission */
    public static boolean checkFullPathOrPatternPermission(
        String userName, PartialPath fullPath, PrivilegeType permission) {
      try {
        // Check if LBAC is enabled
        if (!LbacPermissionChecker.isLbacEnabled()) {
          return true;
        }

        // Check if it's super user
        if (isSuperUser(userName)) {
          return true;
        }

        // 添加路径验证
        if (!isValidPath(fullPath)) {
          LOGGER.warn("Invalid path for LBAC check: {}", fullPath);
          return false;
        }

        // Extract database path from device path
        String databasePath =
            LbacPermissionChecker.extractDatabasePathFromPath(fullPath.getFullPath());
        if (databasePath == null) {
          return true; // Cannot extract database path, allow access
        }

        // Determine operation type
        LbacOperationClassifier.OperationType operationType = determineOperationType(permission);

        // Check LBAC permission at database level
        return LbacPermissionChecker.checkLbacPermissionForDatabase(
            userName, databasePath, operationType);

      } catch (Exception e) {
        return false; // Deny access on error
      }
    }

    /** Check full path or pattern list permission */
    public static List<Integer> checkFullPathOrPatternListPermission(
        String userName, List<? extends PartialPath> pathPatterns, PrivilegeType permission) {
      List<Integer> noPermissionIndexList = new ArrayList<>();

      try {
        // Check if LBAC is enabled
        if (!LbacPermissionChecker.isLbacEnabled()) {
          return noPermissionIndexList; // Empty list means all allowed
        }

        // Check if it's super user
        if (isSuperUser(userName)) {
          return noPermissionIndexList; // Empty list means all allowed
        }

        // Determine operation type
        LbacOperationClassifier.OperationType operationType = determineOperationType(permission);

        // Check LBAC permission for each path at database level
        for (int i = 0; i < pathPatterns.size(); i++) {
          PartialPath path = pathPatterns.get(i);
          if (!isValidPath(path)) {
            noPermissionIndexList.add(i);
            continue;
          }

          String databasePath =
              LbacPermissionChecker.extractDatabasePathFromPath(path.getFullPath());

          if (databasePath != null) {
            boolean hasPermission =
                LbacPermissionChecker.checkLbacPermissionForDatabase(
                    userName, databasePath, operationType);
            if (!hasPermission) {
              noPermissionIndexList.add(i);
            }
          }
        }

      } catch (Exception e) {
        // On error, add all paths to no permission list
        for (int i = 0; i < pathPatterns.size(); i++) {
          noPermissionIndexList.add(i);
        }
      }

      return noPermissionIndexList;
    }

    /** Get authorized path tree */
    public static PathPatternTree getAuthorizedPathTree(String userName, PrivilegeType permission)
        throws AuthException {
      try {
        // Check if LBAC is enabled
        if (!LbacPermissionChecker.isLbacEnabled()) {
          // If LBAC is disabled, return tree containing all paths
          return createAllPathsTree();
        }

        // Check if it's super user
        if (isSuperUser(userName)) {
          // Super user returns tree containing all paths
          return createAllPathsTree();
        }

        // Get user object for LBAC policy check
        User user = LbacPermissionChecker.getUserByName(userName);
        if (user == null) {
          // User not found, return empty tree
          return new PathPatternTree();
        }

        // Determine operation type
        LbacOperationClassifier.OperationType operationType = determineOperationType(permission);

        // Get user's policies for the operation type
        String readPolicy = null;
        String writePolicy = null;

        if (operationType == LbacOperationClassifier.OperationType.READ
            || operationType == LbacOperationClassifier.OperationType.BOTH) {
          readPolicy =
              LbacPermissionChecker.getUserLabelPolicy(
                  user, LbacOperationClassifier.OperationType.READ);
        }

        if (operationType == LbacOperationClassifier.OperationType.WRITE
            || operationType == LbacOperationClassifier.OperationType.BOTH) {
          writePolicy =
              LbacPermissionChecker.getUserLabelPolicy(
                  user, LbacOperationClassifier.OperationType.WRITE);
        }

        boolean hasReadPolicy = (readPolicy != null && !readPolicy.trim().isEmpty());
        boolean hasWritePolicy = (writePolicy != null && !writePolicy.trim().isEmpty());
        boolean hasAnyPolicy = hasReadPolicy || hasWritePolicy;

        // If user has no policies, allow access to all databases
        if (!hasAnyPolicy) {
          return createAllPathsTree();
        }

        PathPatternTree lbacTree = new PathPatternTree();
        List<String> allDatabases = getAllDatabasePaths();

        for (String dbPath : allDatabases) {
          boolean hasLbacPermission =
              LbacPermissionChecker.checkLbacPermissionForDatabase(userName, dbPath, operationType);

          if (hasLbacPermission) {
            String devicePath = dbPath + ".**";
            try {
              lbacTree.appendPathPattern(new PartialPath(devicePath));
            } catch (IllegalPathException e) {
            }
          } else {
          }
        }

        lbacTree.constructTree();
        return lbacTree;

      } catch (Exception e) {

        throw new AuthException(
            TSStatusCode.INTERNAL_SERVER_ERROR,
            "Failed to get LBAC authorized path tree: " + e.getMessage());
      }
    }

    /** Check database permission */
    public static boolean checkDBPermission(
        String userName, String database, PrivilegeType permission) {
      try {
        // Check if LBAC is enabled
        if (!LbacPermissionChecker.isLbacEnabled()) {
          return true;
        }

        // Check if it's super user
        if (isSuperUser(userName)) {
          return true;
        }

        // Determine operation type
        LbacOperationClassifier.OperationType operationType = determineOperationType(permission);

        // Check LBAC permission at database level
        return LbacPermissionChecker.checkLbacPermissionForDatabase(
            userName, database, operationType);

      } catch (Exception e) {
        return false; // Deny access on error
      }
    }

    /** Check table permission (delegates to database level) */
    public static boolean checkTablePermission(
        String userName, String database, String table, PrivilegeType permission) {
      // Table-level LBAC check delegates to database level
      return checkDBPermission(userName, database, permission);
    }

    /** Check database visibility */
    public static boolean checkDBVisible(String userName, String database) {
      // Database visibility check, uses read permission
      return checkDBPermission(userName, database, PrivilegeType.READ_SCHEMA);
    }

    /** Check table visibility (delegates to database level) */
    public static boolean checkTableVisible(String userName, String database, String table) {
      // Table visibility check delegates to database level
      return checkDBPermission(userName, database, PrivilegeType.READ_SCHEMA);
    }
  }
}
