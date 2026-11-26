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

package org.apache.iotdb.db.queryengine.plan.statement.metadata.pipe;

import org.apache.iotdb.db.queryengine.plan.analyze.QueryType;
import org.apache.iotdb.db.queryengine.plan.statement.IConfigStatement;
import org.apache.iotdb.db.queryengine.plan.statement.StatementType;
import org.apache.iotdb.db.queryengine.plan.statement.StatementVisitor;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.ShowStatement;

public class ShowPipesStatement extends ShowStatement implements IConfigStatement {
  public ShowPipesStatement() {
    super();
    statementType = StatementType.SHOW_PIPES;
  }

  private String pipeName;

  private boolean whereClause;

  private boolean isTableModel;

  public String getPipeName() {
    return pipeName;
  }

  public boolean getWhereClause() {
    return whereClause;
  }

  public boolean isTableModel() {
    return isTableModel;
  }

  public void setPipeName(final String pipeName) {
    this.pipeName = pipeName;
  }

  public void setWhereClause(final boolean whereClause) {
    this.whereClause = whereClause;
  }

  public void setTableModel(final boolean tableModel) {
    this.isTableModel = tableModel;
  }

  @Override
  public QueryType getQueryType() {
    return QueryType.READ;
  }

  @Override
  public <R, C> R accept(final StatementVisitor<R, C> visitor, final C context) {
    return visitor.visitShowPipes(this, context);
  }
}
