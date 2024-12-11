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

import javax.annotation.Nullable;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class ShowPipes extends PipeStatement {

  private final String pipeName;
  private final boolean hasWhereClause;
  private String sqlDialect;

  public ShowPipes(final @Nullable String pipeName, final boolean hasWhereClause) {
    this.pipeName = pipeName;
    this.hasWhereClause = hasWhereClause;
  }

  public void setSqlDialect(final String sqlDialect) {
    this.sqlDialect = requireNonNull(sqlDialect, "sql dialect can not be null");
  }

  public String getPipeName() {
    return pipeName;
  }

  public boolean hasWhereClause() {
    return hasWhereClause;
  }

  public String getSqlDialect() {
    return sqlDialect;
  }

  @Override
  public <R, C> R accept(final AstVisitor<R, C> visitor, final C context) {
    return visitor.visitShowPipes(this, context);
  }

  @Override
  public int hashCode() {
    return Objects.hash(pipeName, hasWhereClause, sqlDialect);
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    final ShowPipes that = (ShowPipes) obj;
    return Objects.equals(this.pipeName, that.pipeName)
        && Objects.equals(this.hasWhereClause, that.hasWhereClause)
        && Objects.equals(this.sqlDialect, that.sqlDialect);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("pipeName", pipeName)
        .add("hasWhereClause", hasWhereClause)
        .add("sqlDialect", sqlDialect)
        .toString();
  }
}
