/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.queryengine.plan.statement.pipe;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.InsertRows;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.PipeEnriched;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.queryengine.plan.statement.StatementType;
import org.apache.iotdb.db.queryengine.plan.statement.StatementVisitor;

import org.apache.tsfile.annotations.TableModel;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class PipeEnrichedStatement extends Statement {

  private Statement innerStatement;

  private String originClusterId;

  public PipeEnrichedStatement(final Statement innerStatement) {
    statementType = StatementType.PIPE_ENRICHED;
    this.innerStatement = innerStatement;
  }

  public PipeEnrichedStatement(final Statement innerStatement, final String originClusterId) {
    statementType = StatementType.PIPE_ENRICHED;
    this.innerStatement = innerStatement;
    this.originClusterId = originClusterId;
  }

  public Statement getInnerStatement() {
    return innerStatement;
  }

  public void setInnerStatement(final Statement innerStatement) {
    this.innerStatement = innerStatement;
  }

  public String getOriginClusterId() {
    return originClusterId;
  }

  @Override
  public <R, C> R accept(final StatementVisitor<R, C> visitor, final C context) {
    return visitor.visitPipeEnrichedStatement(this, context);
  }

  @Override
  public boolean isDebug() {
    return innerStatement.isDebug();
  }

  @Override
  public void setDebug(final boolean debug) {
    innerStatement.setDebug(debug);
  }

  @Override
  public boolean isQuery() {
    return !Objects.isNull(innerStatement) && innerStatement.isQuery();
  }

  @Override
  public List<PartialPath> getPaths() {
    return Collections.emptyList();
  }

  @TableModel
  @Override
  public org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Statement toRelationalStatement(
      final MPPQueryContext context) {
    final PipeEnriched pipeEnriched =
        new PipeEnriched(innerStatement.toRelationalStatement(context));
    if (pipeEnriched.getInnerStatement() instanceof InsertRows) {
      ((InsertRows) pipeEnriched.getInnerStatement()).setAllowCreateTable(true);
    }
    return pipeEnriched;
  }
}
