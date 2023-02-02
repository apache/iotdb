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

package org.apache.iotdb.db.mpp.plan.statement.metadata;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.mpp.plan.analyze.QueryType;
import org.apache.iotdb.db.mpp.plan.statement.IConfigStatement;
import org.apache.iotdb.db.mpp.plan.statement.Statement;
import org.apache.iotdb.db.mpp.plan.statement.StatementType;
import org.apache.iotdb.db.mpp.plan.statement.StatementVisitor;

import java.util.Collections;
import java.util.List;

public class DatabaseSchemaStatement extends Statement implements IConfigStatement {

  private final DatabaseSchemaStatementType subType;

  private PartialPath storageGroupPath;
  private Long TTL = null;
  private Integer schemaReplicationFactor = null;
  private Integer dataReplicationFactor = null;
  private Long timePartitionInterval = null;
  private Integer schemaRegionGroupNum = null;
  private Integer dataRegionGroupNum = null;

  public DatabaseSchemaStatement(DatabaseSchemaStatementType subType) {
    super();
    this.subType = subType;
    statementType = StatementType.STORAGE_GROUP_SCHEMA;
  }

  public DatabaseSchemaStatementType getSubType() {
    return subType;
  }

  public PartialPath getStorageGroupPath() {
    return storageGroupPath;
  }

  public void setStorageGroupPath(PartialPath storageGroupPath) {
    this.storageGroupPath = storageGroupPath;
  }

  public Long getTTL() {
    return TTL;
  }

  public void setTTL(Long TTL) {
    this.TTL = TTL;
  }

  public Integer getSchemaReplicationFactor() {
    return schemaReplicationFactor;
  }

  public void setSchemaReplicationFactor(Integer schemaReplicationFactor) {
    this.schemaReplicationFactor = schemaReplicationFactor;
  }

  public Integer getDataReplicationFactor() {
    return dataReplicationFactor;
  }

  public void setDataReplicationFactor(Integer dataReplicationFactor) {
    this.dataReplicationFactor = dataReplicationFactor;
  }

  public Long getTimePartitionInterval() {
    return timePartitionInterval;
  }

  public void setTimePartitionInterval(Long timePartitionInterval) {
    this.timePartitionInterval = timePartitionInterval;
  }

  public Integer getSchemaRegionGroupNum() {
    return schemaRegionGroupNum;
  }

  public void setSchemaRegionGroupNum(Integer schemaRegionGroupNum) {
    this.schemaRegionGroupNum = schemaRegionGroupNum;
  }

  public Integer getDataRegionGroupNum() {
    return dataRegionGroupNum;
  }

  public void setDataRegionGroupNum(Integer dataRegionGroupNum) {
    this.dataRegionGroupNum = dataRegionGroupNum;
  }

  @Override
  public <R, C> R accept(StatementVisitor<R, C> visitor, C context) {
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
    return storageGroupPath != null
        ? Collections.singletonList(storageGroupPath)
        : Collections.emptyList();
  }

  @Override
  public String toString() {
    return "SetStorageGroupStatement{"
        + "storageGroupPath="
        + storageGroupPath
        + ", ttl="
        + TTL
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
