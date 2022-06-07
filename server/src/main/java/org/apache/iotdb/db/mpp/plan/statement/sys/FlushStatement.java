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
import org.apache.iotdb.db.mpp.plan.constant.StatementType;
import org.apache.iotdb.db.mpp.plan.statement.IConfigStatement;
import org.apache.iotdb.db.mpp.plan.statement.Statement;
import org.apache.iotdb.db.mpp.plan.statement.StatementVisitor;
import org.apache.iotdb.db.qp.physical.sys.FlushPlan;
import org.apache.iotdb.tsfile.utils.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class FlushStatement extends Statement implements IConfigStatement {

  private static final Logger logger = LoggerFactory.getLogger(FlushPlan.class);
  /**
   * key-> storage group, value->list of pair, Pair<PartitionId, isSequence>,
   *
   * <p>Notice, the value maybe null, when it is null, all partitions under the storage groups are
   * flushed, so do not use {@link java.util.concurrent.ConcurrentHashMap} when initializing as
   * ConcurrentMap dose not support null key and value
   */
  private Map<PartialPath, List<Pair<Long, Boolean>>> storageGroupPartitionIds;

  // being null indicates flushing both seq and unseq data
  private Boolean isSeq;

  private boolean isLocal;

  public FlushStatement(StatementType flushType) {
    this.statementType = flushType;
  }

  public Map<PartialPath, List<Pair<Long, Boolean>>> getStorageGroupPartitionIds() {
    return storageGroupPartitionIds;
  }

  public void setStorageGroupPartitionIds(
      Map<PartialPath, List<Pair<Long, Boolean>>> storageGroupPartitionIds) {
    this.storageGroupPartitionIds = storageGroupPartitionIds;
  }

  public Boolean isSeq() {
    return isSeq;
  }

  public void setSeq(Boolean seq) {
    isSeq = seq;
  }

  public boolean isLocal() {
    return isLocal;
  }

  public void setLocal(boolean local) {
    isLocal = local;
  }

  @Override
  public QueryType getQueryType() {
    return QueryType.WRITE;
  }

  @Override
  public List<PartialPath> getPaths() {
    if (storageGroupPartitionIds == null) {
      return Collections.emptyList();
    }
    List<PartialPath> partialPaths = new ArrayList<>();
    for (PartialPath partialPath : storageGroupPartitionIds.keySet()) {
      partialPaths.add(partialPath);
    }
    return partialPaths;
  }

  @Override
  public <R, C> R accept(StatementVisitor<R, C> visitor, C context) {
    return visitor.visitFlush(this, context);
  }
}
