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
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.commons.schema.SecurityLabel;
import org.apache.iotdb.commons.schema.column.ColumnHeader;
import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.commons.service.metric.PerformanceOverviewMetrics;
import org.apache.iotdb.confignode.rpc.thrift.TAuthorizerResp;
import org.apache.iotdb.confignode.rpc.thrift.TCheckUserPrivilegesReq;
import org.apache.iotdb.confignode.rpc.thrift.TDBPrivilege;
import org.apache.iotdb.confignode.rpc.thrift.TDatabaseInfo;
import org.apache.iotdb.confignode.rpc.thrift.TGetDatabaseReq;
import org.apache.iotdb.confignode.rpc.thrift.TPathPrivilege;
import org.apache.iotdb.confignode.rpc.thrift.TPermissionInfoResp;
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
    // Utility class, prevent instantiation
  }

  public static IAuthorityFetcher getAuthorityFetcher() {
    return authorityFetcher.get();
  }

  public static boolean invalidateCache(String username, String roleName) {
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

  public static TSStatus checkAuthority(Statement statement, IClientSession session) {
    long startTime = System.nanoTime();
    try {
      return checkAuthority(statement, session.getUsername());
    } finally {
      PERFORMANCE_OVERVIEW_METRICS.recordAuthCost(System.nanoTime() - startTime);
    }
  }

  public static TSStatus checkAuthority(Statement statement, String userName) {
    long startTime = System.nanoTime();
    if (statement == null) {
      return new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
          .setMessage("Statement cannot be null");
    }

    // Check if this is a database-related operation that should be subject to LBAC
    if (!LbacOperationClassifier.isLbacRelevantOperation(statement)) {
      // Not a database-related operation, skip LBAC check
      return SUCCEED;
    }

    // Get paths from statement for LBAC check
    List<? extends PartialPath> paths = statement.getPaths();
    if (paths == null || paths.isEmpty()) {
      return SUCCEED;
    }

    // Convert to list for LBAC check
    List<PartialPath> pathList = new ArrayList<>(paths);

    // Determine privilege type based on statement type
    PrivilegeType privilegeType = statement.determinePrivilegeType();

    // Perform LBAC permission check with correct privilege type
    try {
      return checkPermissionWithLbac(userName, pathList, privilegeType);
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
   * Enhanced permission check with LBAC integration. This method performs RBAC check first, then
   * LBAC check if RBAC passes.
   *
   * @param userName The username requesting access
   * @param paths The paths involved in the operation
   * @param privilegeType The privilege type (READ_DATA, WRITE_DATA, etc.)
   * @return TSStatus indicating success or failure
   */
  public static TSStatus checkPermissionWithLbac(
      String userName, List<PartialPath> paths, PrivilegeType privilegeType) {


    // Step 1: Perform RBAC check first using the original path filtering logic
    List<Integer> noPermissionIndexList =
        checkFullPathOrPatternListPermission(userName, paths, privilegeType);

    if (!noPermissionIndexList.isEmpty()) {
      // RBAC check failed, return error with the failed paths
      return getTSStatus(noPermissionIndexList, paths, privilegeType);
    }

    // Step 2: Check if LBAC is enabled
    if (!LbacPermissionChecker.isLbacEnabled()) {
      // LBAC is disabled, only perform RBAC check (which already passed)
      LOGGER.debug("LBAC is disabled, skipping LBAC check for user: {}", userName);
      return SUCCEED;
    }

    // Step 3: If LBAC is enabled, perform LBAC check
    try {
      // Convert privilege type to LBAC operation type
      LbacOperationClassifier.OperationType lbacOperationType =
          convertPrivilegeTypeToLbacOperation(privilegeType);
      LOGGER.warn("=== AUTHORITY CHECKER DEBUG ===");
      LOGGER.warn("PrivilegeType: {}", privilegeType);
      LOGGER.warn("Converted LBAC OperationType: {}", lbacOperationType);
      if (lbacOperationType == null) {
        // Not a LBAC-relevant operation, allow access
        LOGGER.warn("Not a LBAC-relevant operation, allowing access");
        return SUCCEED;
      }

      // Check LBAC permission for each path
      for (PartialPath path : paths) {
        String devicePath = path.getFullPath();
        String databasePath = LbacPermissionChecker.extractDatabasePathFromDevicePath(devicePath);

        if (databasePath != null) {
          // Check LBAC permission for this database
          TSStatus lbacStatus =
              checkDatabaseLbacPermission(userName, databasePath, lbacOperationType);
          if (lbacStatus.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
            return lbacStatus;
          }
        } else {
          // If database path is null (e.g., path contains wildcards),
          // we need to check all matching databases
          List<String> matchingDatabases = findMatchingDatabases(devicePath);
          for (String database : matchingDatabases) {
            TSStatus lbacStatus =
                checkDatabaseLbacPermission(userName, database, lbacOperationType);
            if (lbacStatus.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
              return lbacStatus;
            }
          }
        }
      }

      return SUCCEED;
    } catch (Exception e) {
      LOGGER.error("Error during LBAC permission check for user: {}", userName, e);
      return new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
          .setMessage("Error during LBAC permission check: " + e.getMessage());
    }
  }

  /**
   * Convert RBAC privilege type to LBAC operation type.
   *
   * @param privilegeType The RBAC privilege type
   * @return The corresponding LBAC operation type
   */
  private static LbacOperationClassifier.OperationType convertPrivilegeTypeToLbacOperation(
      PrivilegeType privilegeType) {
    switch (privilegeType) {
      case READ_DATA:
      case READ_SCHEMA:
        return LbacOperationClassifier.OperationType.READ;
      case WRITE_DATA:
      case WRITE_SCHEMA:
        return LbacOperationClassifier.OperationType.WRITE;
      default:
        return null; // Not a LBAC-relevant operation
    }
  }

  /**
   * Check database-level LBAC permission for a specific user and operation.
   *
   * @param userName The username
   * @param databasePath The database path
   * @param operationType The LBAC operation type
   * @return TSStatus indicating the LBAC permission check result
   */
  private static TSStatus checkDatabaseLbacPermission(
      String userName, String databasePath, LbacOperationClassifier.OperationType operationType) {

    try {
      // Get user object
      User user = getUserByName(userName);
      if (user == null) {
        return new TSStatus(TSStatusCode.USER_NOT_EXIST.getStatusCode())
            .setMessage("User not found for LBAC check");
      }

      // Get database information from ConfigNode
      TDatabaseInfo databaseInfo = getDatabaseInfo(databasePath);
      if (databaseInfo == null) {
        databaseInfo = new TDatabaseInfo();
        databaseInfo.setSecurityLabel(new HashMap<>());
      }

      boolean hasPermission =
          LbacPermissionChecker.checkDatabaseLbacPermission(
              user, databasePath, databaseInfo, operationType);

      if (hasPermission) {
        return SUCCEED;
      } else {
        return new TSStatus(TSStatusCode.NO_PERMISSION.getStatusCode())
            .setMessage("LBAC permission denied for database: " + databasePath);
      }

    } catch (Exception e) {
      LOGGER.error("Error checking LBAC permission for database: {}", databasePath, e);
      return new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
          .setMessage("Internal error during LBAC check: " + e.getMessage());
    }
  }

  /**
   * Get user by name from cache or ConfigNode.
   *
   * @param userName The username
   * @return User object or null if not found
   */
  private static User getUserByName(String userName) {
    try {
      // Try to get user from cache first
      User user = authorityFetcher.get().getAuthorCache().getUserCache(userName);

      if (user != null) {
        return user;
      }

      // Fetch user from ConfigNode if not in cache
      TCheckUserPrivilegesReq req =
          new TCheckUserPrivilegesReq(userName, 0, PrivilegeType.READ_DATA.ordinal(), false);

      try (ConfigNodeClient configNodeClient =
          ConfigNodeClientManager.getInstance().borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {

        TPermissionInfoResp permissionInfoResp = configNodeClient.checkUserPrivileges(req);

        if (permissionInfoResp != null
            && permissionInfoResp.getStatus().getCode()
                == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {

          // Use ClusterAuthorityFetcher's cacheUser method to construct User object
          ClusterAuthorityFetcher clusterFetcher = (ClusterAuthorityFetcher) authorityFetcher.get();
          User fetchedUser = clusterFetcher.cacheUser(permissionInfoResp);

          // Cache the user for future use
          authorityFetcher.get().getAuthorCache().putUserCache(userName, fetchedUser);

          return fetchedUser;
        }
      } catch (Exception e) {
        LOGGER.warn("Failed to fetch user from ConfigNode: {}", userName, e);
      }

    } catch (Exception e) {
      LOGGER.error("Error getting user: {}", userName, e);
    }

    return null;
  }

  /**
   * Get database information from ConfigNode. This method now uses LBAC security label information
   * for better integration.
   *
   * @param databasePath The database path
   * @return TDatabaseInfo or null if not found
   */
  private static TDatabaseInfo getDatabaseInfo(String databasePath) {
    try {
      LOGGER.debug("Getting database info for path: {}", databasePath);

      try (ConfigNodeClient configNodeClient =
          ConfigNodeClientManager.getInstance().borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {

        TGetDatabaseReq req = new TGetDatabaseReq();
        req.setDatabasePathPattern(Collections.singletonList(databasePath));
        req.setScopePatternTree(new PathPatternTree().serialize());
        req.setIsTableModel(false);

        TShowDatabaseResp resp = configNodeClient.showDatabase(req);

        if (resp != null
            && resp.getStatus().getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {

          if (resp.getDatabaseInfoMap() != null
              && resp.getDatabaseInfoMap().containsKey(databasePath)) {
            TDatabaseInfo databaseInfo = resp.getDatabaseInfoMap().get(databasePath);

            try {

              SecurityLabel securityLabel =
                  DatabaseLabelFetcher.getDatabaseSecurityLabel(databasePath);
              if (securityLabel != null && !securityLabel.getLabels().isEmpty()) {
                LOGGER.debug(
                    "Found security label for database {}: {}",
                    databasePath,
                    securityLabel.getLabels());

                Map<String, String> securityLabelMap = new HashMap<>();
                for (Map.Entry<String, String> entry : securityLabel.getLabels().entrySet()) {
                  securityLabelMap.put(entry.getKey(), entry.getValue());
                }

                if (databaseInfo.getSecurityLabel() == null) {
                  databaseInfo.setSecurityLabel(securityLabelMap);
                }
              }
            } catch (Exception e) {
              LOGGER.debug(
                  "Could not get security label for database {}: {}", databasePath, e.getMessage());
            }

            return databaseInfo;
          }
        }
      } catch (Exception e) {
        LOGGER.warn("Failed to get database info from ConfigNode for: {}", databasePath, e);
      }

      try {
        SecurityLabel securityLabel = DatabaseLabelFetcher.getDatabaseSecurityLabel(databasePath);
        if (securityLabel != null && !securityLabel.getLabels().isEmpty()) {
          LOGGER.debug("Using LBAC fallback for database info: {}", databasePath);

          TDatabaseInfo fallbackInfo = new TDatabaseInfo();
          fallbackInfo.setName(databasePath);

          Map<String, String> securityLabelMap = new HashMap<>();
          for (Map.Entry<String, String> entry : securityLabel.getLabels().entrySet()) {
            securityLabelMap.put(entry.getKey(), entry.getValue());
          }
          fallbackInfo.setSecurityLabel(securityLabelMap);

          return fallbackInfo;
        }
      } catch (Exception e) {
        LOGGER.debug("LBAC fallback also failed for database {}: {}", databasePath, e.getMessage());
      }

      return null;
    } catch (Exception e) {
      LOGGER.warn("Error in getDatabaseInfo for: {}", databasePath, e);
      return null;
    }
  }

  /**
   * Extract database path from device path. This is a utility method to extract database path from
   * device path.
   *
   * @param devicePath The device path
   * @return The database path or null if extraction fails
   */
  private static String extractDatabasePathFromDevicePath(String devicePath) {
    if (devicePath == null || devicePath.trim().isEmpty()) {
      return null;
    }

    // Split by dots and take the first two parts as database path
    String[] parts = devicePath.split("\\.");
    if (parts.length >= 2) {
      return parts[0] + "." + parts[1];
    } else if (parts.length == 1) {
      return parts[0];
    }

    return null;
  }

  /**
   * Get all databases from ConfigNode.
   *
   * @return List of database names
   */
  private static List<String> getAllDatabases() {
    try {
      try (ConfigNodeClient configNodeClient =
          ConfigNodeClientManager.getInstance().borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {

        // Create request to get all databases
        TGetDatabaseReq req = new TGetDatabaseReq();
        req.setDatabasePathPattern(Collections.emptyList()); // Get all databases
        req.setScopePatternTree(new PathPatternTree().serialize());
        req.setIsTableModel(false);

        TShowDatabaseResp resp = configNodeClient.showDatabase(req);
        if (resp != null && resp.getDatabaseInfoMap() != null) {
          return new ArrayList<>(resp.getDatabaseInfoMap().keySet());
        }
      }
    } catch (Exception e) {
      LOGGER.error("Failed to get all databases: {}", e.getMessage(), e);
    }
    return new ArrayList<>();
  }

  /**
   * Find databases that match the given pattern.
   *
   * @param pattern The pattern to match (e.g., "root.**")
   * @return List of matching database names
   */
  private static List<String> findMatchingDatabases(String pattern) {
    List<String> allDatabases = getAllDatabases();
    List<String> matchingDatabases = new ArrayList<>();

    for (String database : allDatabases) {
      if (LbacPermissionChecker.isDatabasePathMatchPattern(database, pattern)) {
        matchingDatabases.add(database);
      }
    }

    LOGGER.debug(
        "Found {} databases matching pattern '{}': {}",
        matchingDatabases.size(),
        pattern,
        matchingDatabases);
    return matchingDatabases;
  }
}
