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

import org.apache.iotdb.db.queryengine.common.MPPQueryContext;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

public abstract class WrappedStatement extends Statement {
  protected org.apache.iotdb.db.queryengine.plan.statement.Statement innerTreeStatement;
  protected MPPQueryContext context;

  protected WrappedStatement(
      org.apache.iotdb.db.queryengine.plan.statement.Statement innerTreeStatement,
      MPPQueryContext context) {
    super(null);
    this.innerTreeStatement = innerTreeStatement;
    this.context = context;
  }

  @Override
  public List<? extends Node> getChildren() {
    return Collections.emptyList();
  }

  @Override
  public int hashCode() {
    return innerTreeStatement.hashCode();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    WrappedStatement that = (WrappedStatement) o;
    return Objects.equals(innerTreeStatement, that.innerTreeStatement);
  }

  @Override
  public String toString() {
    return innerTreeStatement.toString();
  }

  public org.apache.iotdb.db.queryengine.plan.statement.Statement getInnerTreeStatement() {
    return innerTreeStatement;
  }

  public void setInnerTreeStatement(
      org.apache.iotdb.db.queryengine.plan.statement.Statement innerTreeStatement) {
    this.innerTreeStatement = innerTreeStatement;
  }

  public MPPQueryContext getContext() {
    return context;
  }

  public void setContext(MPPQueryContext context) {
    this.context = context;
  }
}
