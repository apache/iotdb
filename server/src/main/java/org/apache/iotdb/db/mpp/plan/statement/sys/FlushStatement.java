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

package org.apache.iotdb.db.mpp.plan.statement.sys;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.mpp.plan.analyze.QueryType;
import org.apache.iotdb.db.mpp.plan.statement.IConfigStatement;
import org.apache.iotdb.db.mpp.plan.statement.Statement;
import org.apache.iotdb.db.mpp.plan.statement.StatementType;
import org.apache.iotdb.db.mpp.plan.statement.StatementVisitor;

import java.util.Collections;
import java.util.List;

public class FlushStatement extends Statement implements IConfigStatement {

  /** list of database */
  private List<PartialPath> storageGroups;

  // being null indicates flushing both seq and unseq data
  private Boolean isSeq;

  private boolean onCluster;

  public FlushStatement(StatementType flushType) {
    this.statementType = flushType;
  }

  public List<PartialPath> getStorageGroups() {
    return storageGroups;
  }

  public void setStorageGroups(List<PartialPath> storageGroups) {
    this.storageGroups = storageGroups;
  }

  public Boolean isSeq() {
    return isSeq;
  }

  public void setSeq(Boolean seq) {
    isSeq = seq;
  }

  public boolean isOnCluster() {
    return onCluster;
  }

  public void setOnCluster(boolean onCluster) {
    this.onCluster = onCluster;
  }

  @Override
  public QueryType getQueryType() {
    return QueryType.WRITE;
  }

  @Override
  public List<PartialPath> getPaths() {
    if (storageGroups == null) {
      return Collections.emptyList();
    }
    return storageGroups;
  }

  @Override
  public <R, C> R accept(StatementVisitor<R, C> visitor, C context) {
    return visitor.visitFlush(this, context);
  }
}
