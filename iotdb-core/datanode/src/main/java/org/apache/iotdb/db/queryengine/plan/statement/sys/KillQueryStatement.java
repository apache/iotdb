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

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.queryengine.plan.analyze.QueryType;
import org.apache.iotdb.db.queryengine.plan.statement.IConfigStatement;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.queryengine.plan.statement.StatementVisitor;

import java.util.Collections;
import java.util.List;

public class KillQueryStatement extends Statement implements IConfigStatement {
  private final String queryId;
  // if allowedUsername is null, this statement can kill other user's queries
  private String allowedUsername;

  public KillQueryStatement(String queryId) {
    this.queryId = queryId;
  }

  public KillQueryStatement() {
    this.queryId = null;
  }

  public boolean isKillAll() {
    return queryId == null;
  }

  public String getQueryId() {
    return queryId;
  }

  public String getAllowedUsername() {
    return allowedUsername;
  }

  public void setAllowedUsername(String allowedUsername) {
    this.allowedUsername = allowedUsername;
  }

  @Override
  public List<PartialPath> getPaths() {
    return Collections.emptyList();
  }

  @Override
  public <R, C> R accept(StatementVisitor<R, C> visitor, C context) {
    return visitor.visitKillQuery(this, context);
  }

  @Override
  public QueryType getQueryType() {
    return QueryType.WRITE;
  }
}
