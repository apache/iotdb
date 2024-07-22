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

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.queryengine.plan.statement.StatementType;
import org.apache.iotdb.db.queryengine.plan.statement.StatementVisitor;
import org.apache.iotdb.db.queryengine.plan.statement.crud.QueryStatement;

import java.util.List;

public class ExplainAnalyzeStatement extends Statement {

  private final QueryStatement queryStatement;

  private boolean verbose = false;

  public ExplainAnalyzeStatement(QueryStatement queryStatement) {
    statementType = StatementType.QUERY;
    this.queryStatement = queryStatement;
  }

  @Override
  public TSStatus checkPermissionBeforeProcess(String userName) {
    return queryStatement.checkPermissionBeforeProcess(userName);
  }

  public QueryStatement getQueryStatement() {
    return queryStatement;
  }

  public void setVerbose(boolean verbose) {
    this.verbose = verbose;
  }

  public boolean isVerbose() {
    return verbose;
  }

  @Override
  public boolean isQuery() {
    return true;
  }

  @Override
  public List<PartialPath> getPaths() {
    return queryStatement.getPaths();
  }

  @Override
  public <R, C> R accept(StatementVisitor<R, C> visitor, C context) {
    return visitor.visitExplainAnalyze(this, context);
  }
}
