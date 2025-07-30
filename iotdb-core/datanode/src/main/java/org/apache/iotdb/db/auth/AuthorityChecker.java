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
import org.apache.iotdb.commons.schema.column.ColumnHeader;
import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.commons.service.metric.PerformanceOverviewMetrics;
import org.apache.iotdb.confignode.rpc.thrift.TAuthorizerResp;
import org.apache.iotdb.confignode.rpc.thrift.TDBPrivilege;
import org.apache.iotdb.confignode.rpc.thrift.TPathPrivilege;
import org.apache.iotdb.confignode.rpc.thrift.TRoleResp;
import org.apache.iotdb.confignode.rpc.thrift.TTablePrivilege;
import org.apache.iotdb.confignode.rpc.thrift.TUserResp;
import org.apache.iotdb.db.pipe.source.dataregion.realtime.listener.PipeInsertionDataNodeListener;
import org.apache.iotdb.db.protocol.client.ConfigNodeClient;
import org.apache.iotdb.db.protocol.client.ConfigNodeClientManager;
import org.apache.iotdb.db.protocol.client.ConfigNodeInfo;
import org.apache.iotdb.db.pipe.extractor.dataregion.realtime.listener.PipeInsertionDataNodeListener;
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
import java.util.Collections;
import java.util.HashMap;
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

  private static final MemoizedSupplier<IAuthorityFetcher> authorityFetcher =
      MemoizedSupplier.valueOf(() -> new ClusterAuthorityFetcher(new BasicAuthorityCache()));

  private static final PerformanceOverviewMetrics PERFORMANCE_OVERVIEW_METRICS =
      PerformanceOverviewMetrics.getInstance();

  private AuthorityChecker() {
    // empty constructor
  }

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
    long startTime = System.nanoTime();
    try {
      return statement.checkPermissionBeforeProcess(userName);
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
    return authorityFetcher.get().checkUserSysPrivileges(userName, permission).getCode()
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
    TSStatus status = authorityFetcher.get().checkDBVisible(userName, database);
    boolean result = status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode();
    if (!result) {
      LOGGER.warn(
          "RBAC check failed for user {} on database {}: {}",
          userName,
          database,
          status.getMessage());
    } else {
      LOGGER.debug("RBAC check passed for user {} on database {}", userName, database);
    }
    return result;
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

  public static TSStatus checkSuperUserOrMaintain(String userName) {
    if (AuthorityChecker.SUPER_USER.equals(userName)) {
      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    }
    return AuthorityChecker.getTSStatus(
        AuthorityChecker.checkSystemPermission(userName, PrivilegeType.MAINTAIN),
        PrivilegeType.MAINTAIN);
  }

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

  /**
   * Convert RBAC privilege type to LBAC operation type. RBAC write_data + write_schema → LBAC write
   * RBAC read_data + read_schema → LBAC read
   *
   * @param privilegeType The RBAC privilege type
   * @return The corresponding LBAC operation type, or null for non-database operations
   */
  public static LbacOperationClassifier.OperationType convertPrivilegeTypeToLbacOperation(
      PrivilegeType privilegeType) {
    switch (privilegeType) {
      case READ_DATA:
      case READ_SCHEMA:
        return LbacOperationClassifier.OperationType.READ;
      case WRITE_DATA:
      case WRITE_SCHEMA:
        return LbacOperationClassifier.OperationType.WRITE;
      default:
        // For non-database operations, return null
        return null;
    }
  }

  /**
   * Unified RBAC+LBAC filtering for databases with proper operation type conversion. This method
   * provides a unified interface for filtering database maps based on both RBAC and LBAC
   * permissions, automatically handling the LBAC enable/disable switch.
   *
   * @param originalMap Original database map
   * @param userName User name
   * @param privilegeType RBAC privilege type (will be converted to LBAC operation type)
   * @return Filtered map containing only databases the user can access
   */
  public static <T> Map<String, T> filterDatabaseMapByPermissions(
      Map<String, T> originalMap, String userName, PrivilegeType privilegeType) {

    if (originalMap == null || originalMap.isEmpty()) {
      return Collections.emptyMap();
    }

    // Super user sees all
    if (SUPER_USER.equals(userName)) {
      return originalMap;
    }

    Map<String, T> filteredMap = new HashMap<>();
    int totalCount = originalMap.size();
    int rbacAllowedCount = 0;
    int lbacAllowedCount = 0;

    for (Map.Entry<String, T> entry : originalMap.entrySet()) {
      String database = entry.getKey();

      try {
        // Step 1: RBAC check using checkFullPathOrPatternPermission method
        boolean rbacAllowed =
            checkFullPathOrPatternPermission(userName, new PartialPath(database), privilegeType);

        if (rbacAllowed) {
          rbacAllowedCount++;

          // Step 2: LBAC check (only if enabled)
          if (LbacPermissionChecker.isLbacEnabled()) {
            // Convert PrivilegeType to LBAC operation type
            LbacOperationClassifier.OperationType lbacOpType =
                convertPrivilegeTypeToLbacOperation(privilegeType);

            if (lbacOpType != null) {
              boolean lbacAllowed =
                  LbacPermissionChecker.checkLbacPermissionForDatabase(
                      userName, database, lbacOpType);
              if (lbacAllowed) {
                filteredMap.put(database, entry.getValue());
                lbacAllowedCount++;
              } else {
                LOGGER.warn("Database {} denied by LBAC for user {}", database, userName);
              }
            } else {
              LOGGER.warn("Cannot convert privilege type {} to LBAC operation type", privilegeType);
            }
            // If lbacOpType is null (non-database operation), don't add to filtered map
          } else {
            // LBAC disabled, only RBAC check needed
            filteredMap.put(database, entry.getValue());
          }
        } else {
          LOGGER.warn("Database {} denied by RBAC for user {}", database, userName);
        }
      } catch (Exception e) {
        LOGGER.warn("Permission check failed for database {}: {}", database, e.getMessage());
      }
    }

    LOGGER.info(
        "Database filtering complete - Total: {}, RBAC allowed: {}, LBAC allowed: {}, Final: {}",
        totalCount,
        rbacAllowedCount,
        lbacAllowedCount,
        filteredMap.size());

    return filteredMap;
  }

  /**
   * Extract database path from full path, supporting wildcards. This method handles various path
   * patterns and extracts the database portion.
   *
   * <p>Examples: - "root.db1" → "root.db1" - "root.db1.device1" → "root.db1" - "root.db1.**" →
   * "root.db1" - "root.**" → null (too broad, requires special handling)
   *
   * @param fullPath The full path potentially containing wildcards
   * @return The extracted database path, or null if cannot extract valid database
   */
  public static String extractDatabaseFromPath(String fullPath) {
    if (fullPath == null || !fullPath.startsWith("root.")) {
      return null;
    }

    // Remove wildcard patterns
    String cleanPath = fullPath.replace(".**", "").replace(".*", "");

    String[] parts = cleanPath.split("\\.");
    if (parts.length >= 2) {
      return parts[0] + "." + parts[1]; // root.database format
    }

    return null;
  }
}
