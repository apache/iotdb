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
import org.apache.iotdb.db.protocol.session.IClientSession;
import org.apache.iotdb.db.queryengine.plan.analyze.QueryType;
import org.apache.iotdb.db.queryengine.plan.statement.IConfigStatement;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.queryengine.plan.statement.StatementVisitor;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class SetSqlDialectStatement extends Statement implements IConfigStatement {
  private final IClientSession.SqlDialect sqlDialect;

  public SetSqlDialectStatement(IClientSession.SqlDialect sqlDialect) {
    this.sqlDialect = sqlDialect;
  }

  public IClientSession.SqlDialect getSqlDialect() {
    return sqlDialect;
  }

  @Override
  public int hashCode() {
    return Objects.hash(sqlDialect);
  }

  @Override
  public String toString() {
    return "SET SQL_DIALECT=" + sqlDialect;
  }

  @Override
  public List<? extends PartialPath> getPaths() {
    return Collections.emptyList();
  }

  @Override
  public QueryType getQueryType() {
    return QueryType.WRITE;
  }

  @Override
  public <R, C> R accept(StatementVisitor<R, C> visitor, C context) {
    return visitor.visitSetSqlDialect(this, context);
  }
}
