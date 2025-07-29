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

package org.apache.iotdb.db.queryengine.plan.statement.metadata;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.column.ColumnHeader;
import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.confignode.rpc.thrift.TDatabaseInfo;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.protocol.session.SessionManager;
import org.apache.iotdb.db.queryengine.common.header.DatasetHeaderFactory;
import org.apache.iotdb.db.queryengine.plan.analyze.QueryType;
import org.apache.iotdb.db.queryengine.plan.execution.config.ConfigTaskResult;
import org.apache.iotdb.db.queryengine.plan.statement.IConfigStatement;
import org.apache.iotdb.db.queryengine.plan.statement.StatementVisitor;
import org.apache.iotdb.rpc.TSStatusCode;

import com.google.common.util.concurrent.SettableFuture;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.utils.Binary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * SHOW DATABASES statement
 *
 * <p>Here is the syntax definition:
 *
 * <p>SHOW DATABASES prefixPath?
 */
public class ShowDatabaseStatement extends ShowStatement implements IConfigStatement {

  private static final Logger LOGGER = LoggerFactory.getLogger(ShowDatabaseStatement.class);

  private final PartialPath pathPattern;
  private boolean isDetailed;
  private boolean showSecurityLabel; // Add field for security label display

  public ShowDatabaseStatement(final PartialPath pathPattern) {
    super();
    this.pathPattern = pathPattern;
    this.isDetailed = false;
    this.showSecurityLabel = false; // Initialize to false
  }

  public PartialPath getPathPattern() {
    return pathPattern;
  }

  public boolean isDetailed() {
    return isDetailed;
  }

  public void setDetailed(final boolean detailed) {
    isDetailed = detailed;
  }

  public boolean isShowSecurityLabel() {
    return showSecurityLabel;
  }

  public void setShowSecurityLabel(final boolean showSecurityLabel) {
    this.showSecurityLabel = showSecurityLabel;
  }

  @Override
  public TSStatus checkPermissionBeforeProcess(String userName) {
    // Use the enhanced LBAC-integrated permission check
    return checkPermissionWithLbac(userName);
  }

  @Override
  protected PrivilegeType determinePrivilegeType() {
    // Show database operations require READ_SCHEMA privilege
    return PrivilegeType.READ_SCHEMA;
  }

  public void buildTSBlock(
      final Map<String, TDatabaseInfo> databaseInfoMap,
      final SettableFuture<ConfigTaskResult> future) {

    try {
      LOGGER.debug("Building TSBlock for {} databases", databaseInfoMap.size());

      // Get current user for filtering
      String currentUser = SessionManager.getInstance().getCurrSession().getUsername();

      // Filter databases based on RBAC and LBAC permissions
      Map<String, TDatabaseInfo> filteredDatabaseMap =
          filterDatabasesByPermissions(databaseInfoMap, currentUser);

      LOGGER.debug("After permission filtering: {} databases", filteredDatabaseMap.size());

      // Convert to TreeMap for ordered output
      TreeMap<String, TDatabaseInfo> orderedDatabaseMap = new TreeMap<>(filteredDatabaseMap);

      // Build column headers based on whether detailed info is requested
      List<ColumnHeader> columnHeaders;
      if (isDetailed) {
        columnHeaders = ColumnHeaderConstant.showDatabasesDetailColumnHeaders;
      } else {
        columnHeaders = ColumnHeaderConstant.showDatabasesColumnHeaders;
      }

      List<TSDataType> outputDataTypes =
          columnHeaders.stream().map(ColumnHeader::getColumnType).collect(Collectors.toList());

      TsBlockBuilder builder = new TsBlockBuilder(outputDataTypes);

      // Process filtered databases in order
      for (Map.Entry<String, TDatabaseInfo> entry : orderedDatabaseMap.entrySet()) {
        String databaseName = entry.getKey();
        TDatabaseInfo databaseInfo = entry.getValue();

        if (databaseName != null && !databaseName.trim().isEmpty()) {
          builder.getTimeColumnBuilder().writeLong(0L);
          builder
              .getColumnBuilder(0)
              .writeBinary(new Binary(databaseName, TSFileConfig.STRING_CHARSET));

          if (isDetailed && databaseInfo != null) {
            // Add detailed information - ensure we have the correct number of columns
            int columnIndex = 1;
            if (columnIndex < outputDataTypes.size()) {
              builder
                  .getColumnBuilder(columnIndex++)
                  .writeBinary(
                      new Binary(
                          String.valueOf(databaseInfo.getTTL()), TSFileConfig.STRING_CHARSET));
            }
            if (columnIndex < outputDataTypes.size()) {
              builder
                  .getColumnBuilder(columnIndex++)
                  .writeBinary(
                      new Binary(
                          String.valueOf(databaseInfo.getSchemaReplicationFactor()),
                          TSFileConfig.STRING_CHARSET));
            }
            if (columnIndex < outputDataTypes.size()) {
              builder
                  .getColumnBuilder(columnIndex++)
                  .writeBinary(
                      new Binary(
                          String.valueOf(databaseInfo.getDataReplicationFactor()),
                          TSFileConfig.STRING_CHARSET));
            }
            if (columnIndex < outputDataTypes.size()) {
              builder
                  .getColumnBuilder(columnIndex++)
                  .writeBinary(
                      new Binary(
                          String.valueOf(databaseInfo.getTimePartitionInterval()),
                          TSFileConfig.STRING_CHARSET));
            }
          } else if (!isDetailed && databaseInfo != null) {
            // For non-detailed view, add basic database info
            int columnIndex = 1;
            if (columnIndex < outputDataTypes.size()) {
              builder
                  .getColumnBuilder(columnIndex++)
                  .writeInt(databaseInfo.getSchemaReplicationFactor());
            }
            if (columnIndex < outputDataTypes.size()) {
              builder
                  .getColumnBuilder(columnIndex++)
                  .writeInt(databaseInfo.getDataReplicationFactor());
            }
            if (columnIndex < outputDataTypes.size()) {
              builder
                  .getColumnBuilder(columnIndex++)
                  .writeLong(databaseInfo.getTimePartitionOrigin());
            }
            if (columnIndex < outputDataTypes.size()) {
              builder
                  .getColumnBuilder(columnIndex++)
                  .writeLong(databaseInfo.getTimePartitionInterval());
            }
          }

          builder.declarePosition();
        }
      }

      future.set(
          new ConfigTaskResult(
              TSStatusCode.SUCCESS_STATUS,
              builder.build(),
              DatasetHeaderFactory.getShowDatabaseHeader(isDetailed)));

      LOGGER.info("Successfully created TSBlock with {} databases", builder.getPositionCount());

    } catch (Exception e) {
      LOGGER.error("Error in buildTSBlock", e);
      future.setException(e);
    }
  }

  public void buildTSBlockFromSecurityLabel(
      Map<String, String> securityLabelMap, SettableFuture<ConfigTaskResult> future) {

    try {
      LOGGER.debug(
          "Building TSBlock from security labels for {} databases", securityLabelMap.size());

      // Get current user for filtering
      String currentUser = SessionManager.getInstance().getCurrSession().getUsername();

      // Filter security labels based on RBAC and LBAC permissions
      Map<String, String> filteredSecurityLabelMap =
          filterSecurityLabelsByPermissions(securityLabelMap, currentUser);

      LOGGER.debug(
          "After permission filtering: {} databases with security labels",
          filteredSecurityLabelMap.size());

      List<TSDataType> outputDataTypes =
          ColumnHeaderConstant.SHOW_DATABASE_SECURITY_LABEL_COLUMN_HEADERS.stream()
              .map(ColumnHeader::getColumnType)
              .collect(Collectors.toList());

      TsBlockBuilder builder = new TsBlockBuilder(outputDataTypes);

      // Convert to TreeMap for ordered output
      TreeMap<String, String> orderedSecurityLabelMap = new TreeMap<>(filteredSecurityLabelMap);

      // Process filtered security labels in order
      for (Map.Entry<String, String> entry : orderedSecurityLabelMap.entrySet()) {
        String databaseName = entry.getKey();
        String securityLabel = entry.getValue();

        if (databaseName != null && !databaseName.trim().isEmpty()) {
          builder.getTimeColumnBuilder().writeLong(0L);
          builder
              .getColumnBuilder(0)
              .writeBinary(new Binary(databaseName, TSFileConfig.STRING_CHARSET));
          builder
              .getColumnBuilder(1)
              .writeBinary(
                  new Binary(
                      (securityLabel == null) ? "" : securityLabel, TSFileConfig.STRING_CHARSET));
          builder.declarePosition();
        }
      }

      future.set(
          new ConfigTaskResult(
              TSStatusCode.SUCCESS_STATUS,
              builder.build(),
              DatasetHeaderFactory.getShowDatabaseSecurityLabelHeader()));

      LOGGER.info(
          "Successfully created TSBlock with {} databases with security labels",
          builder.getPositionCount());

    } catch (Exception e) {
      LOGGER.error("Error in buildTSBlockFromSecurityLabel", e);
      future.setException(e);
    }
  }

  /**
   * Filter databases based on RBAC and LBAC permissions using AuthorityChecker methods
   *
   * @param databaseInfoMap Original database info map
   * @param userName Current user name
   * @return Filtered database info map
   */
  private Map<String, TDatabaseInfo> filterDatabasesByPermissions(
      Map<String, TDatabaseInfo> databaseInfoMap, String userName) {

    // Check if user is super user
    if (AuthorityChecker.SUPER_USER.equals(userName)) {
      LOGGER.debug("User {} is super user, bypassing permission filtering", userName);
      return databaseInfoMap;
    }

    Map<String, TDatabaseInfo> filteredMap = new HashMap<>();

    for (Map.Entry<String, TDatabaseInfo> entry : databaseInfoMap.entrySet()) {
      String databaseName = entry.getKey();
      TDatabaseInfo databaseInfo = entry.getValue();

      // Check permissions for this database using AuthorityChecker methods
      if (checkDatabasePermissions(userName, databaseName)) {
        filteredMap.put(databaseName, databaseInfo);
        LOGGER.debug("Database {} allowed by permissions", databaseName);
      } else {
        LOGGER.debug("Database {} denied by permissions", databaseName);
      }
    }

    LOGGER.debug(
        "Permission filtering complete: {} databases accessible out of {}",
        filteredMap.size(),
        databaseInfoMap.size());
    return filteredMap;
  }

  /**
   * Filter security labels based on RBAC and LBAC permissions using AuthorityChecker methods
   *
   * @param securityLabelMap Original security label map
   * @param userName Current user name
   * @return Filtered security label map
   */
  private Map<String, String> filterSecurityLabelsByPermissions(
      Map<String, String> securityLabelMap, String userName) {

    // Check if user is super user
    if (AuthorityChecker.SUPER_USER.equals(userName)) {
      LOGGER.debug("User {} is super user, bypassing permission filtering", userName);
      return securityLabelMap;
    }

    Map<String, String> filteredMap = new HashMap<>();

    for (Map.Entry<String, String> entry : securityLabelMap.entrySet()) {
      String databaseName = entry.getKey();
      String securityLabel = entry.getValue();

      // Check permissions for this database using AuthorityChecker methods
      if (checkDatabasePermissions(userName, databaseName)) {
        filteredMap.put(databaseName, securityLabel);
        LOGGER.debug("Database {} allowed by permissions", databaseName);
      } else {
        LOGGER.debug("Database {} denied by permissions", databaseName);
      }
    }

    LOGGER.debug(
        "Permission filtering complete: {} databases accessible out of {}",
        filteredMap.size(),
        securityLabelMap.size());
    return filteredMap;
  }

  /**
   * Check RBAC and LBAC permissions for a specific database using AuthorityChecker methods
   *
   * @param userName The username
   * @param databaseName The database name
   * @return true if user has permission, false otherwise
   */
  private boolean checkDatabasePermissions(String userName, String databaseName) {
    try {
      // Create a PartialPath for the database
      PartialPath databasePath = new PartialPath(databaseName);
      List<PartialPath> paths = Collections.singletonList(databasePath);

      // Use AuthorityChecker's checkPermissionWithLbac method which handles both RBAC
      // and LBAC
      TSStatus permissionStatus =
          AuthorityChecker.checkPermissionWithLbac(userName, paths, PrivilegeType.READ_SCHEMA);

      boolean hasPermission =
          (permissionStatus.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode());

      if (hasPermission) {
        LOGGER.debug("Permission check passed for user {} on database {}", userName, databaseName);
      } else {
        LOGGER.debug(
            "Permission check failed for user {} on database {}: {}",
            userName,
            databaseName,
            permissionStatus.getMessage());
      }

      return hasPermission;

    } catch (Exception e) {
      LOGGER.error("Error checking permissions for database: {}", databaseName, e);
      return false;
    }
  }

  @Override
  public <R, C> R accept(final StatementVisitor<R, C> visitor, C context) {
    return visitor.visitShowStorageGroup(this, context);
  }

  @Override
  public QueryType getQueryType() {
    return QueryType.READ;
  }

  @Override
  public List<PartialPath> getPaths() {
    return Collections.singletonList(pathPattern);
  }
}
