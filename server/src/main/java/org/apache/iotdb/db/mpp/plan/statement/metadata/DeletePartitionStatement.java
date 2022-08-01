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

import java.util.List;
import java.util.Set;

public class DeletePartitionStatement extends Statement implements IConfigStatement {
  private PartialPath prefixPath;
  private Set<Long> partitionIds;

  public DeletePartitionStatement() {
    super();
    statementType = StatementType.DELETE_STORAGE_GROUP;
  }

  @Override
  public List<PartialPath> getPaths() {
    return null;
  }

  public PartialPath getPrefixPath() {
    return prefixPath;
  }

  public Set<Long> getPartitionId() {
    return partitionIds;
  }

  @Override
  public <R, C> R accept(StatementVisitor<R, C> visitor, C context) {
    return visitor.visitDeletePartition(this, context);
  }

  public void setPrefixPath(PartialPath prefixPath) {
    this.prefixPath = prefixPath;
  }

  public void setPartitionIds(Set<Long> partitionIds) {
    this.partitionIds = partitionIds;
  }

  @Override
  public QueryType getQueryType() {
    return QueryType.WRITE;
  }
}
