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
import org.apache.iotdb.db.mpp.plan.constant.StatementType;
import org.apache.iotdb.db.mpp.plan.statement.IConfigStatement;
import org.apache.iotdb.db.mpp.plan.statement.Statement;
import org.apache.iotdb.db.mpp.plan.statement.StatementVisitor;

import java.util.Collections;
import java.util.List;

public class SetStorageGroupStatement extends Statement implements IConfigStatement {
  private PartialPath storageGroupPath;
  private Long ttl = null;
  private Integer schemaReplicationFactor = null;
  private Integer dataReplicationFactor = null;
  private Long timePartitionInterval = null;

  public SetStorageGroupStatement() {
    super();
    statementType = StatementType.SET_STORAGE_GROUP;
  }

  public PartialPath getStorageGroupPath() {
    return storageGroupPath;
  }

  @Override
  public <R, C> R accept(StatementVisitor<R, C> visitor, C context) {
    return visitor.visitSetStorageGroup(this, context);
  }

  public void setStorageGroupPath(PartialPath storageGroupPath) {
    this.storageGroupPath = storageGroupPath;
  }

  public void setTtl(Long ttl) {
    this.ttl = ttl;
  }

  public void setSchemaReplicationFactor(Integer schemaReplicationFactor) {
    this.schemaReplicationFactor = schemaReplicationFactor;
  }

  public void setDataReplicationFactor(Integer dataReplicationFactor) {
    this.dataReplicationFactor = dataReplicationFactor;
  }

  public void setTimePartitionInterval(Long timePartitionInterval) {
    this.timePartitionInterval = timePartitionInterval;
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

  public Long getTTL() {
    return ttl;
  }

  public Integer getSchemaReplicationFactor() {
    return schemaReplicationFactor;
  }

  public Integer getDataReplicationFactor() {
    return dataReplicationFactor;
  }

  public Long getTimePartitionInterval() {
    return timePartitionInterval;
  }
}
