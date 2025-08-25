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

package org.apache.iotdb.db.queryengine.plan.relational.sql.ast;

import org.apache.iotdb.db.queryengine.plan.analyze.QueryType;
import org.apache.iotdb.db.queryengine.plan.statement.IConfigStatement;
import org.apache.iotdb.db.queryengine.plan.statement.StatementType;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Statement for altering database security labels in table model. */
public class AlterDatabaseSecurityLabelStatement extends Statement implements IConfigStatement {

  private final String database;
  private final Map<String, String> securityLabels;

  public AlterDatabaseSecurityLabelStatement(
      @Nullable NodeLocation location, String database, Map<String, String> securityLabels) {
    super(location);
    this.database = database;
    this.securityLabels = securityLabels;
  }

  public String getDatabase() {
    return database;
  }

  public Map<String, String> getSecurityLabels() {
    return securityLabels;
  }

  public StatementType getStatementType() {
    return StatementType.ALTER_TABLE_DATABASE_SECURITY_LABEL;
  }

  @Override
  public QueryType getQueryType() {
    return QueryType.WRITE;
  }

  @Override
  public List<? extends Node> getChildren() {
    return Collections.emptyList();
  }

  @Override
  public int hashCode() {
    return Objects.hash(database, securityLabels);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    AlterDatabaseSecurityLabelStatement that = (AlterDatabaseSecurityLabelStatement) obj;
    return Objects.equals(database, that.database)
        && Objects.equals(securityLabels, that.securityLabels);
  }

  @Override
  public String toString() {
    return String.format(
        "AlterDatabaseSecurityLabelStatement{database='%s', securityLabels=%s}",
        database, securityLabels);
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitAlterDatabaseSecurityLabel(this, context);
  }
}
