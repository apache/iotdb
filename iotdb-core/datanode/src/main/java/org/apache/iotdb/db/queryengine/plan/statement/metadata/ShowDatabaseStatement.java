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

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.column.ColumnHeader;
import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.confignode.rpc.thrift.TDatabaseInfo;
import org.apache.iotdb.db.auth.LbacPermissionChecker;
import org.apache.iotdb.db.protocol.session.IClientSession;
import org.apache.iotdb.db.protocol.session.SessionManager;
import org.apache.iotdb.db.queryengine.common.header.DatasetHeader;
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
import java.util.List;
import java.util.Map;
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

  public void buildTSBlock(
      final Map<String, TDatabaseInfo> databaseInfoMap,
      final SettableFuture<ConfigTaskResult> future) {

    // Filter databases based on LBAC permissions using centralized checker
    String currentUser = getCurrentUserName();
    Map<String, TDatabaseInfo> filteredDatabaseMap =
        LbacPermissionChecker.filterDatabaseInfoMapByLbac(databaseInfoMap, currentUser);

    final List<TSDataType> outputDataTypes;
    if (showSecurityLabel) {
      // When showing security labels, use only database name and security label
      // columns
      outputDataTypes =
          ColumnHeaderConstant.SHOW_DATABASE_SECURITY_LABEL_COLUMN_HEADERS.stream()
              .map(ColumnHeader::getColumnType)
              .collect(Collectors.toList());
    } else {
      // Normal database info display
      outputDataTypes =
          isDetailed
              ? ColumnHeaderConstant.showDatabasesDetailColumnHeaders.stream()
                  .map(ColumnHeader::getColumnType)
                  .collect(Collectors.toList())
              : ColumnHeaderConstant.showDatabasesColumnHeaders.stream()
                  .map(ColumnHeader::getColumnType)
                  .collect(Collectors.toList());
    }

    final TsBlockBuilder builder = new TsBlockBuilder(outputDataTypes);
    for (final Map.Entry<String, TDatabaseInfo> entry :
        filteredDatabaseMap.entrySet().stream()
            .sorted(Map.Entry.comparingByKey())
            .collect(Collectors.toList())) {
      final String database = entry.getKey();
      final TDatabaseInfo databaseInfo = entry.getValue();

      builder.getTimeColumnBuilder().writeLong(0L);

      if (showSecurityLabel) {
        // Output database name and security label
        builder.getColumnBuilder(0).writeBinary(new Binary(database, TSFileConfig.STRING_CHARSET));
        Map<String, String> securityLabelMap = databaseInfo.getSecurityLabel();
        String securityLabel = "";
        if (securityLabelMap != null && !securityLabelMap.isEmpty()) {
          // Convert map to string representation with proper formatting
          securityLabel =
              securityLabelMap.entrySet().stream()
                  .map(labelEntry -> labelEntry.getKey() + " = " + labelEntry.getValue())
                  .collect(Collectors.joining(" , "));
        }
        builder
            .getColumnBuilder(1)
            .writeBinary(new Binary(securityLabel, TSFileConfig.STRING_CHARSET));
      } else {
        // Normal database info output
        builder.getColumnBuilder(0).writeBinary(new Binary(database, TSFileConfig.STRING_CHARSET));
        builder.getColumnBuilder(1).writeInt(databaseInfo.getSchemaReplicationFactor());
        builder.getColumnBuilder(2).writeInt(databaseInfo.getDataReplicationFactor());
        builder.getColumnBuilder(3).writeLong(databaseInfo.getTimePartitionOrigin());
        builder.getColumnBuilder(4).writeLong(databaseInfo.getTimePartitionInterval());
        if (isDetailed) {
          builder.getColumnBuilder(5).writeInt(databaseInfo.getSchemaRegionNum());
          builder.getColumnBuilder(6).writeInt(databaseInfo.getMinSchemaRegionNum());
          builder.getColumnBuilder(7).writeInt(databaseInfo.getMaxSchemaRegionNum());
          builder.getColumnBuilder(8).writeInt(databaseInfo.getDataRegionNum());
          builder.getColumnBuilder(9).writeInt(databaseInfo.getMinDataRegionNum());
          builder.getColumnBuilder(10).writeInt(databaseInfo.getMaxDataRegionNum());
        }
      }
      builder.declarePosition();
    }

    final DatasetHeader datasetHeader;
    if (showSecurityLabel) {
      datasetHeader = DatasetHeaderFactory.getShowDatabaseSecurityLabelHeader();
    } else {
      datasetHeader = DatasetHeaderFactory.getShowDatabaseHeader(isDetailed);
    }
    future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS, builder.build(), datasetHeader));
  }

  /**
   * Get current user name from execution context This method retrieves the current user from the
   * execution context which contains the session information
   *
   * @return Current user name or null if not available
   */
  private String getCurrentUserName() {
    try {
      // Try to get from SessionManager first
      IClientSession currentSession = SessionManager.getInstance().getCurrSession();
      if (currentSession != null) {
        String userName = currentSession.getUsername();
        LOGGER.debug("Retrieved current user from SessionManager: {}", userName);
        return userName;
      }

      LOGGER.warn("Could not retrieve current user from session manager");
      return null;
    } catch (Exception e) {
      LOGGER.warn("Failed to get current user name: {}", e.getMessage());
      return null;
    }
  }

  /**
   * 构造只包含数据库名和安全标签的 TSBlock 添加LBAC过滤逻辑，只返回用户有权限访问的数据库
   *
   * @param securityLabelMap Map<数据库名, 标签字符串>
   * @param future 返回结果
   */
  public void buildTSBlockFromSecurityLabel(
      Map<String, String> securityLabelMap, SettableFuture<ConfigTaskResult> future) {
    try {
      // Add detailed logging for debugging
      LOGGER.warn("=== SHOW DATABASES SECURITY LABEL BUILD START ===");
      LOGGER.warn("buildTSBlockFromSecurityLabel called with map: {}", securityLabelMap);
      LOGGER.warn("Map size: {}", securityLabelMap.size());
      LOGGER.warn("showSecurityLabel flag: {}", showSecurityLabel);
      LOGGER.warn("pathPattern: {}", pathPattern);

      // Get current user for LBAC filtering
      String currentUser = SessionManager.getInstance().getCurrSession().getUsername();
      LOGGER.warn("Current user for LBAC filtering: {}", currentUser);

      // Apply LBAC filtering to security label map
      Map<String, String> filteredSecurityLabelMap =
          LbacPermissionChecker.filterSecurityLabelMapByLbac(securityLabelMap, currentUser);

      LOGGER.warn("Original security label map size: {}", securityLabelMap.size());
      LOGGER.warn("Filtered security label map size: {}", filteredSecurityLabelMap.size());
      LOGGER.warn(
          "Filtered out {} databases due to LBAC restrictions",
          securityLabelMap.size() - filteredSecurityLabelMap.size());

      // Use filtered map for building result
      Map<String, String> finalSecurityLabelMap = filteredSecurityLabelMap;

      List<TSDataType> outputDataTypes =
          ColumnHeaderConstant.SHOW_DATABASE_SECURITY_LABEL_COLUMN_HEADERS.stream()
              .map(ColumnHeader::getColumnType)
              .collect(Collectors.toList());
      LOGGER.debug("Output data types: {}", outputDataTypes);
      TsBlockBuilder builder = new TsBlockBuilder(outputDataTypes);

      if (finalSecurityLabelMap != null && !finalSecurityLabelMap.isEmpty()) {
        LOGGER.info("Processing {} database security labels", finalSecurityLabelMap.size());

        for (Map.Entry<String, String> entry : finalSecurityLabelMap.entrySet()) {
          String databaseName = entry.getKey();
          String securityLabel = entry.getValue();

          LOGGER.debug(
              "Processing database: '{}', security label: '{}'", databaseName, securityLabel);

          // Skip null or empty database names
          if (databaseName != null && !databaseName.trim().isEmpty()) {
            builder.getTimeColumnBuilder().writeLong(0L);
            builder
                .getColumnBuilder(0)
                .writeBinary(new Binary(databaseName, TSFileConfig.STRING_CHARSET));
            builder
                .getColumnBuilder(1)
                .writeBinary(
                    new Binary(
                        securityLabel != null ? securityLabel : "", TSFileConfig.STRING_CHARSET));
            builder.declarePosition();
            LOGGER.debug(
                "Added database: '{}' with security label: '{}'", databaseName, securityLabel);
          } else {
            LOGGER.warn("Skipping database with null or empty name: '{}'", databaseName);
          }
        }
        LOGGER.info(
            "Successfully processed {} databases with security labels", builder.getPositionCount());
      } else {
        LOGGER.warn("Security label map is null or empty, no databases to process");
      }

      future.set(
          new ConfigTaskResult(
              TSStatusCode.SUCCESS_STATUS,
              builder.build(),
              DatasetHeaderFactory.getShowDatabaseSecurityLabelHeader()));
      LOGGER.info("Successfully created TSBlock with {} databases", builder.getPositionCount());
      LOGGER.warn("=== SHOW DATABASES SECURITY LABEL BUILD END ===");
    } catch (Exception e) {
      LOGGER.error("Error in buildTSBlockFromSecurityLabel", e);
      future.setException(e);
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
