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

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class Explain extends Statement {

  private final Statement statement;

  public Explain(Statement statement) {
    super(null);
    this.statement = requireNonNull(statement, "statement is null");
  }

  public Explain(NodeLocation location, Statement statement) {
    super(requireNonNull(location, "location is null"));
    this.statement = requireNonNull(statement, "statement is null");
  }

  public Statement getStatement() {
    return statement;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitExplain(this, context);
  }

  @Override
  public List<Node> getChildren() {
    return ImmutableList.<Node>builder().add(statement).build();
  }

  @Override
  public int hashCode() {
    return Objects.hash(statement);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if ((obj == null) || (getClass() != obj.getClass())) {
      return false;
    }
    Explain o = (Explain) obj;
    return Objects.equals(statement, o.statement);
  }

  @Override
  public String toString() {
    return toStringHelper(this).add("statement", statement).toString();
  }
}
