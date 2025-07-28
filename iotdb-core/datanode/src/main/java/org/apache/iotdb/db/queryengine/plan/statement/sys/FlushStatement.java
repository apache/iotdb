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

package org.apache.iotdb.db.queryengine.plan.statement.sys;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.queryengine.plan.analyze.QueryType;
import org.apache.iotdb.db.queryengine.plan.statement.IConfigStatement;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.queryengine.plan.statement.StatementType;
import org.apache.iotdb.db.queryengine.plan.statement.StatementVisitor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class FlushStatement extends Statement implements IConfigStatement {

  /** list of database */
  private List<String> databases;

  // being null indicates flushing both seq and unseq data
  private Boolean isSeq;

  private boolean onCluster;

  public FlushStatement(final StatementType flushType) {
    this.statementType = flushType;
  }

  public List<String> getDatabases() {
    return databases;
  }

  public void setDatabases(final List<String> databases) {
    this.databases = databases;
  }

  public Boolean isSeq() {
    return isSeq;
  }

  public void setSeq(final Boolean seq) {
    isSeq = seq;
  }

  public boolean isOnCluster() {
    return onCluster;
  }

  public void setOnCluster(final boolean onCluster) {
    this.onCluster = onCluster;
  }

  @Override
  public QueryType getQueryType() {
    return QueryType.WRITE;
  }

  @Override
  public List<PartialPath> getPaths() {
    if (databases == null) {
      return Collections.emptyList();
    }

    final List<PartialPath> paths = new ArrayList<>(databases.size());
    try {
      for (final String database : databases) {
        paths.add(new PartialPath(database));
      }
    } catch (final IllegalPathException e) {
      // ignore
    }
    return paths;
  }

  @Override
  public <R, C> R accept(final StatementVisitor<R, C> visitor, final C context) {
    return visitor.visitFlush(this, context);
  }
}
