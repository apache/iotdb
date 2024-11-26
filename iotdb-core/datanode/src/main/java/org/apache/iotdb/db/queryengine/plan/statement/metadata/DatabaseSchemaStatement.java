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
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.queryengine.plan.analyze.QueryType;
import org.apache.iotdb.db.queryengine.plan.statement.IConfigStatement;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.queryengine.plan.statement.StatementType;
import org.apache.iotdb.db.queryengine.plan.statement.StatementVisitor;
import org.apache.iotdb.rpc.TSStatusCode;

import java.util.Collections;
import java.util.List;

public class DatabaseSchemaStatement extends Statement implements IConfigStatement {

  private final DatabaseSchemaStatementType subType;

  private PartialPath databasePath;
  private Long ttl = null;
  private Long timePartitionInterval = null;
  private Integer schemaRegionGroupNum = null;
  private Integer dataRegionGroupNum = null;
  private boolean enablePrintExceptionLog = true;

  // Deprecated
  private Integer schemaReplicationFactor = null;
  private Integer dataReplicationFactor = null;

  public DatabaseSchemaStatement(final DatabaseSchemaStatementType subType) {
    super();
    this.subType = subType;
    statementType = StatementType.STORAGE_GROUP_SCHEMA;
  }

  public DatabaseSchemaStatementType getSubType() {
    return subType;
  }

  public PartialPath getDatabasePath() {
    return databasePath;
  }

  public void setDatabasePath(final PartialPath databasePath) {
    this.databasePath = databasePath;
  }

  public Long getTtl() {
    return ttl;
  }

  public void setTtl(final Long ttl) {
    this.ttl = ttl;
  }

  public Integer getSchemaReplicationFactor() {
    return schemaReplicationFactor;
  }

  public void setSchemaReplicationFactor(final Integer schemaReplicationFactor) {
    this.schemaReplicationFactor = schemaReplicationFactor;
  }

  public Integer getDataReplicationFactor() {
    return dataReplicationFactor;
  }

  public void setDataReplicationFactor(final Integer dataReplicationFactor) {
    this.dataReplicationFactor = dataReplicationFactor;
  }

  public Long getTimePartitionInterval() {
    return timePartitionInterval;
  }

  public void setTimePartitionInterval(final Long timePartitionInterval) {
    this.timePartitionInterval = timePartitionInterval;
  }

  public Integer getSchemaRegionGroupNum() {
    return schemaRegionGroupNum;
  }

  public void setSchemaRegionGroupNum(final Integer schemaRegionGroupNum) {
    this.schemaRegionGroupNum = schemaRegionGroupNum;
  }

  public Integer getDataRegionGroupNum() {
    return dataRegionGroupNum;
  }

  public void setDataRegionGroupNum(final Integer dataRegionGroupNum) {
    this.dataRegionGroupNum = dataRegionGroupNum;
  }

  public boolean getEnablePrintExceptionLog() {
    return enablePrintExceptionLog;
  }

  public void setEnablePrintExceptionLog(final boolean enablePrintExceptionLog) {
    this.enablePrintExceptionLog = enablePrintExceptionLog;
  }

  @Override
  public <R, C> R accept(final StatementVisitor<R, C> visitor, final C context) {
    switch (subType) {
      case CREATE:
        return visitor.visitSetDatabase(this, context);
      case ALTER:
      default:
        return visitor.visitAlterDatabase(this, context);
    }
  }

  @Override
  public QueryType getQueryType() {
    return QueryType.WRITE;
  }

  @Override
  public List<PartialPath> getPaths() {
    return databasePath != null ? Collections.singletonList(databasePath) : Collections.emptyList();
  }

  @Override
  public TSStatus checkPermissionBeforeProcess(final String userName) {
    if (AuthorityChecker.SUPER_USER.equals(userName)) {
      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    }
    return AuthorityChecker.getTSStatus(
        AuthorityChecker.checkSystemPermission(userName, PrivilegeType.MANAGE_DATABASE.ordinal()),
        PrivilegeType.MANAGE_DATABASE);
  }

  @Override
  public String toString() {
    return "SetStorageGroupStatement{"
        + "storageGroupPath="
        + databasePath
        + ", ttl="
        + ttl
        + ", schemaReplicationFactor="
        + schemaReplicationFactor
        + ", dataReplicationFactor="
        + dataReplicationFactor
        + ", timePartitionInterval="
        + timePartitionInterval
        + ", schemaRegionGroupNum="
        + schemaRegionGroupNum
        + ", dataRegionGroupNum="
        + dataRegionGroupNum
        + '}';
  }

  public enum DatabaseSchemaStatementType {
    CREATE,
    ALTER
  }
}
