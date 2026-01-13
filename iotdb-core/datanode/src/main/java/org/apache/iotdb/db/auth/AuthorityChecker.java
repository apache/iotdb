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
import org.apache.iotdb.commons.audit.IAuditEntity;
import org.apache.iotdb.commons.audit.UserEntity;
import org.apache.iotdb.commons.auth.AuthException;
import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.auth.entity.User;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.commons.schema.column.ColumnHeader;
import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.commons.service.metric.PerformanceOverviewMetrics;
import org.apache.iotdb.confignode.rpc.thrift.TAuthorizerResp;
import org.apache.iotdb.confignode.rpc.thrift.TDBPrivilege;
import org.apache.iotdb.confignode.rpc.thrift.TListUserInfo;
import org.apache.iotdb.confignode.rpc.thrift.TPathPrivilege;
import org.apache.iotdb.confignode.rpc.thrift.TRoleResp;
import org.apache.iotdb.confignode.rpc.thrift.TTablePrivilege;
import org.apache.iotdb.confignode.rpc.thrift.TUserResp;
import org.apache.iotdb.db.pipe.source.dataregion.realtime.listener.PipeInsertionDataNodeListener;
import org.apache.iotdb.db.protocol.session.IClientSession;
import org.apache.iotdb.db.queryengine.common.header.DatasetHeader;
import org.apache.iotdb.db.queryengine.plan.execution.config.ConfigTaskResult;
import org.apache.iotdb.db.queryengine.plan.relational.security.AccessControl;
import org.apache.iotdb.db.queryengine.plan.relational.security.AccessControlImpl;
import org.apache.iotdb.db.queryengine.plan.relational.security.ITableAuthCheckerImpl;
import org.apache.iotdb.db.queryengine.plan.relational.security.TreeAccessCheckContext;
import org.apache.iotdb.db.queryengine.plan.relational.security.TreeAccessCheckVisitor;
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.StringJoiner;
import java.util.stream.Collectors;

import static org.apache.iotdb.commons.schema.column.ColumnHeaderConstant.LIST_USER_COLUMN_HEADERS;
import static org.apache.iotdb.commons.schema.column.ColumnHeaderConstant.LIST_USER_OR_ROLE_PRIVILEGES_COLUMN_HEADERS;

// Authority checker is SingleTon working at datanode.
// It checks permission in local. DCL statement will send to configNode.
public class AuthorityChecker {

  public static int SUPER_USER_ID = 0;
  public static String SUPER_USER =
      CommonDescriptor.getInstance().getConfig().getDefaultAdminName();
  public static String SUPER_USER_ID_IN_STR = "0";

  public static final TSStatus SUCCEED = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());

  public static final int INTERNAL_AUDIT_USER_ID = 4;
  public static final String INTERNAL_AUDIT_USER = "__internal_auditor";

  public static String ANY_SCOPE = "any";

  public static final String ONLY_ADMIN_ALLOWED =
      "No permissions for this operation, only root user is allowed";

  public static final String NO_PERMISSION_PROMOTION =
      "No permissions for this operation, please add privilege ";

  private static final String NO_GRANT_OPT_PERMISSION_PROMOTION =
      "No permissions for this operation, please add grant option to privilege ";

  private static final MemoizedSupplier<IAuthorityFetcher> authorityFetcher =
      MemoizedSupplier.valueOf(() -> new ClusterAuthorityFetcher(new BasicAuthorityCache()));

  private static final PerformanceOverviewMetrics PERFORMANCE_OVERVIEW_METRICS =
      PerformanceOverviewMetrics.getInstance();

  private static volatile AccessControl accessControl =
      new AccessControlImpl(new ITableAuthCheckerImpl(), new TreeAccessCheckVisitor());

  private AuthorityChecker() {
    // empty constructor
  }

  public static AccessControl getAccessControl() {
    return accessControl;
  }

  public static void setAccessControl(AccessControl accessControl) {
    AuthorityChecker.accessControl = accessControl;
  }

  public static void setSuperUser(String superUser) {
    SUPER_USER = superUser;
  }

  public static IAuthorityFetcher getAuthorityFetcher() {
    return authorityFetcher.get();
  }

  public static boolean invalidateCache(String username, String roleName) {
    PipeInsertionDataNodeListener.getInstance().invalidateAllCache();
    return authorityFetcher.get().getAuthorCache().invalidateCache(username, roleName);
  }

  public static User getUser(String username) {
    return authorityFetcher.get().getUser(username);
  }

  public static Optional<Long> getUserId(String username) {
    User user = authorityFetcher.get().getUser(username);
    return Optional.ofNullable(user == null ? null : user.getUserId());
  }

  public static TSStatus checkUser(String userName, String password) {
    TSStatus status = authorityFetcher.get().checkUser(userName, password);
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return status;
    }
    return accessControl.allowUserToLogin(userName);
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

  public static IAuditEntity createIAuditEntity(String userName, IClientSession clientSession) {
    if (clientSession != null && clientSession.getUsername() != null) {
      return new UserEntity(
          clientSession.getUserId(), clientSession.getUsername(), clientSession.getClientAddress());
    } else if (userName != null) {
      return new UserEntity(AuthorityChecker.getUserId(userName).orElse(-1L), userName, "");
    } else {
      return new UserEntity(-1, "unknown", "");
    }
  }

  public static TSStatus checkAuthority(Statement statement, IAuditEntity auditEntity) {
    long startTime = System.nanoTime();
    try {
      if (auditEntity instanceof TreeAccessCheckContext) {
        return accessControl.checkPermissionBeforeProcess(
            statement, (TreeAccessCheckContext) auditEntity);
      }
      return accessControl.checkPermissionBeforeProcess(
          statement,
          (TreeAccessCheckContext)
              new TreeAccessCheckContext(
                      auditEntity.getUserId(),
                      auditEntity.getUsername(),
                      auditEntity.getCliHostname())
                  .setSqlString(auditEntity.getSqlString()));
    } finally {
      PERFORMANCE_OVERVIEW_METRICS.recordAuthCost(System.nanoTime() - startTime);
    }
  }

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

  public static TSStatus getTSStatus(Collection<PrivilegeType> missingPrivileges) {
    return missingPrivileges.isEmpty()
        ? SUCCEED
        : new TSStatus(TSStatusCode.NO_PERMISSION.getStatusCode())
            .setMessage(
                NO_PERMISSION_PROMOTION + getMissingAllNeededPrivilegeString(missingPrivileges));
  }

  public static TSStatus getTSStatus(boolean hasPermission, PrivilegeType neededPrivilege) {
    return hasPermission
        ? SUCCEED
        : new TSStatus(TSStatusCode.NO_PERMISSION.getStatusCode())
            .setMessage(
                NO_PERMISSION_PROMOTION
                    + getSatisfyAnyNeededPrivilegeString(
                        neededPrivilege.getReplacedPrivilegeType()));
  }

  private static String getSatisfyAnyNeededPrivilegeString(PrivilegeType... privileges) {
    StringJoiner sj = new StringJoiner("/");
    for (PrivilegeType privilege : privileges) {
      sj.add(privilege.toString());
    }
    return sj.toString();
  }

  private static String getMissingAllNeededPrivilegeString(Collection<PrivilegeType> privileges) {
    StringJoiner sj = new StringJoiner(",");
    for (PrivilegeType privilege : privileges) {
      sj.add(privilege.toString());
    }
    return sj.toString();
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

    StringBuilder prompt = new StringBuilder(NO_PERMISSION_PROMOTION);
    prompt.append(neededPrivilege);
    prompt.append(" on [");
    prompt.append(pathList.get(noPermissionIndexList.get(0)));
    final int size =
        Math.min(
            noPermissionIndexList.size(),
            CommonDescriptor.getInstance().getConfig().getPathLogMaxSize());
    for (int i = 1; i < size; i++) {
      prompt.append(", ");
      prompt.append(pathList.get(noPermissionIndexList.get(i)));
    }
    if (size < noPermissionIndexList.size()) {
      prompt.append(", ...");
    }
    prompt.append("]");
    return new TSStatus(TSStatusCode.NO_PERMISSION.getStatusCode()).setMessage(prompt.toString());
  }

  public static boolean checkFullPathOrPatternPermission(
      String userName, PartialPath fullPath, PrivilegeType permission) {
    return authorityFetcher
        .get()
        .checkUserPathPrivileges(userName, Collections.singletonList(fullPath), permission)
        .isEmpty();
  }

  public static List<Integer> checkFullPathOrPatternListPermission(
      String userName, List<? extends PartialPath> pathPatterns, PrivilegeType permission) {
    return authorityFetcher.get().checkUserPathPrivileges(userName, pathPatterns, permission);
  }

  public static PathPatternTree getAuthorizedPathTree(String userName, PrivilegeType permission)
      throws AuthException {
    return authorityFetcher.get().getAuthorizedPatternTree(userName, permission);
  }

  public static boolean checkSystemPermission(String userName, PrivilegeType permission) {
    return authorityFetcher.get().checkUserSysPrivilege(userName, permission).getCode()
        == TSStatusCode.SUCCESS_STATUS.getStatusCode();
  }

  public static boolean checkSystemPermissionGrantOption(
      String userName, PrivilegeType permission) {
    return authorityFetcher.get().checkUserSysPrivilegesGrantOpt(userName, permission).getCode()
        == TSStatusCode.SUCCESS_STATUS.getStatusCode();
  }

  public static boolean checkAnyScopePermissionGrantOption(
      String userName, PrivilegeType permission) {
    return authorityFetcher
            .get()
            .checkUserAnyScopePrivilegeGrantOption(userName, permission)
            .getCode()
        == TSStatusCode.SUCCESS_STATUS.getStatusCode();
  }

  public static boolean checkDBPermission(
      String userName, String database, PrivilegeType permission) {
    return authorityFetcher.get().checkUserDBPrivileges(userName, database, permission).getCode()
        == TSStatusCode.SUCCESS_STATUS.getStatusCode();
  }

  public static boolean checkDBPermissionGrantOption(
      String userName, String database, PrivilegeType permission) {
    return authorityFetcher
            .get()
            .checkUserDBPrivilegesGrantOpt(userName, database, permission)
            .getCode()
        == TSStatusCode.SUCCESS_STATUS.getStatusCode();
  }

  public static boolean checkTablePermission(
      String userName, String database, String table, PrivilegeType permission) {
    return authorityFetcher
            .get()
            .checkUserTBPrivileges(userName, database, table, permission)
            .getCode()
        == TSStatusCode.SUCCESS_STATUS.getStatusCode();
  }

  public static boolean checkTablePermissionGrantOption(
      String userName, String database, String table, PrivilegeType permission) {
    return authorityFetcher
            .get()
            .checkUserTBPrivilegesGrantOpt(userName, database, table, permission)
            .getCode()
        == TSStatusCode.SUCCESS_STATUS.getStatusCode();
  }

  public static boolean checkDBVisible(String userName, String database) {
    return authorityFetcher.get().checkDBVisible(userName, database).getCode()
        == TSStatusCode.SUCCESS_STATUS.getStatusCode();
  }

  public static boolean checkTableVisible(String userName, String database, String table) {
    return authorityFetcher.get().checkTBVisible(userName, database, table).getCode()
        == TSStatusCode.SUCCESS_STATUS.getStatusCode();
  }

  public static boolean checkPathPermissionGrantOption(
      String userName, PrivilegeType privilegeType, List<PartialPath> nodeNameList) {
    return authorityFetcher
            .get()
            .checkUserPathPrivilegesGrantOpt(userName, nodeNameList, privilegeType)
            .getCode()
        == TSStatusCode.SUCCESS_STATUS.getStatusCode();
  }

  public static boolean checkRole(String username, String roleName) {
    return authorityFetcher.get().checkRole(username, roleName);
  }

  public static Collection<PrivilegeType> checkUserMissingSystemPermissions(
      String userName, Collection<PrivilegeType> permissions) {
    if (SUPER_USER.equals(userName)) {
      return Collections.emptySet();
    }
    return authorityFetcher.get().checkUserSysPrivileges(userName, permissions);
  }

  public static void buildTSBlock(
      TAuthorizerResp authResp, SettableFuture<ConfigTaskResult> future) {
    List<TSDataType> types = new ArrayList<>();
    boolean listRoleUser =
        authResp.tag.equals(ColumnHeaderConstant.ROLE)
            || authResp.tag.equals(ColumnHeaderConstant.USER);

    List<ColumnHeader> headerList = new ArrayList<>();
    TsBlockBuilder builder;
    if (authResp.tag.equals(ColumnHeaderConstant.ROLE)) {
      headerList.add(new ColumnHeader(authResp.getTag(), TSDataType.TEXT));
      types.add(TSDataType.TEXT);
      builder = new TsBlockBuilder(types);
      for (String name : authResp.getMemberInfo()) {
        builder.getTimeColumnBuilder().writeLong(0L);
        builder.getColumnBuilder(0).writeBinary(new Binary(name, TSFileConfig.STRING_CHARSET));
        builder.declarePosition();
      }
    } else if (authResp.tag.equals(ColumnHeaderConstant.USER)) {
      headerList = LIST_USER_COLUMN_HEADERS;
      types =
          LIST_USER_COLUMN_HEADERS.stream()
              .map(ColumnHeader::getColumnType)
              .collect(Collectors.toList());
      builder = new TsBlockBuilder(types);
      for (TListUserInfo userinfo : authResp.getUsersInfo()) {
        builder.getTimeColumnBuilder().writeLong(0L);
        builder.getColumnBuilder(0).writeLong(userinfo.getUserId());
        builder
            .getColumnBuilder(1)
            .writeBinary(new Binary(userinfo.getUsername(), TSFileConfig.STRING_CHARSET));
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
      if (isIgnoredPrivilege(i)) {
        continue;
      }
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

  private static boolean isIgnoredPrivilege(int i) {
    return PrivilegeType.values()[i].isHided();
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
}
