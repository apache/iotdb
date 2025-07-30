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
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.queryengine.common.header.DatasetHeader;
import org.apache.iotdb.db.queryengine.common.header.DatasetHeaderFactory;
import org.apache.iotdb.db.queryengine.plan.analyze.QueryType;
import org.apache.iotdb.db.queryengine.plan.execution.config.ConfigTaskResult;
import org.apache.iotdb.db.queryengine.plan.statement.AuthorityInformationStatement;
import org.apache.iotdb.db.queryengine.plan.statement.IConfigStatement;
import org.apache.iotdb.db.queryengine.plan.statement.StatementType;
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
 * SHOW DATABASES database? SECURITY_LABEL statement for tree model
 *
 * <p>This class is responsible for displaying database security labels in the tree model. It
 * provides a clean separation from the regular SHOW DATABASES statement.
 */
public class ShowDatabaseSecurityLabelStatement extends AuthorityInformationStatement
    implements IConfigStatement {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(ShowDatabaseSecurityLabelStatement.class);

  private final PartialPath pathPattern;

  public ShowDatabaseSecurityLabelStatement(PartialPath pathPattern) {
    super();
    this.pathPattern = pathPattern;
    this.statementType = StatementType.SHOW_DATABASE_SECURITY_LABEL;
  }

  public PartialPath getPathPattern() {
    return pathPattern;
  }

  @Override
  public List<PartialPath> getPaths() {
    return Collections.singletonList(pathPattern);
  }

  @Override
  public QueryType getQueryType() {
    return QueryType.READ;
  }

  public TSStatus checkPermissionBeforeProcess(final String userName) {

    // Database security label operations require MANAGE_DATABASE permission
    if (AuthorityChecker.SUPER_USER.equals(userName)) {
      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    }


    boolean hasPermission =
        AuthorityChecker.checkSystemPermission(userName, PrivilegeType.MANAGE_DATABASE);

    if (!hasPermission) {

      return AuthorityChecker.getTSStatus(
          AuthorityChecker.checkSystemPermission(userName, PrivilegeType.MANAGE_DATABASE),
          PrivilegeType.MANAGE_DATABASE);
    }

    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  /**
   * Build TSBlock from security label map with integrated RBAC and LBAC filtering
   *
   * @param securityLabelMap Map of database names to security labels
   * @param future Future to set the result
   */
  public void buildTSBlockFromSecurityLabel(
      Map<String, String> securityLabelMap,
      SettableFuture<ConfigTaskResult> future,
      String userName) {
    try {
      // Apply unified RBAC+LBAC filtering using AuthorityChecker
      Map<String, String> filteredMap =
          AuthorityChecker.filterDatabaseMapByPermissions(
              securityLabelMap, userName, PrivilegeType.READ_SCHEMA);

      List<TSDataType> outputDataTypes =
          ColumnHeaderConstant.SHOW_DATABASE_SECURITY_LABEL_COLUMN_HEADERS.stream()
              .map(ColumnHeader::getColumnType)
              .collect(Collectors.toList());

      TsBlockBuilder builder = new TsBlockBuilder(outputDataTypes);

      for (Map.Entry<String, String> entry :
          filteredMap.entrySet().stream()
              .sorted(Map.Entry.comparingByKey())
              .collect(Collectors.toList())) {

        String database = entry.getKey();
        String label = entry.getValue();

        builder.getTimeColumnBuilder().writeLong(0L);
        builder.getColumnBuilder(0).writeBinary(new Binary(database, TSFileConfig.STRING_CHARSET));
        builder
            .getColumnBuilder(1)
            .writeBinary(new Binary(label != null ? label : "", TSFileConfig.STRING_CHARSET));
        builder.declarePosition();
      }

      DatasetHeader datasetHeader = DatasetHeaderFactory.getShowDatabaseSecurityLabelHeader();
      future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS, builder.build(), datasetHeader));

    } catch (Exception e) {
      future.setException(
          new RuntimeException("Failed to build security label result: " + e.getMessage()));
    }
  }

  @Override
  public <R, C> R accept(StatementVisitor<R, C> visitor, C context) {
    return visitor.visitShowDatabaseSecurityLabel(this, context);
  }
}
