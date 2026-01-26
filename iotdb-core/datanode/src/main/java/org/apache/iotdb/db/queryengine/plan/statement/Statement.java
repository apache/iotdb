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

package org.apache.iotdb.db.queryengine.plan.statement;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.plan.parser.ASTVisitor;

import java.util.Collections;
import java.util.List;

/**
 * This class is a superclass of all statements.
 *
 * <p>A Statement containing all semantic information of an SQL. It is obtained by traversing the
 * AST via {@link ASTVisitor}.
 */
public abstract class Statement extends StatementNode {
  protected StatementType statementType = StatementType.NULL;

  protected boolean isDebug;

  protected Statement() {}

  public void setType(final StatementType statementType) {
    this.statementType = statementType;
  }

  public StatementType getType() {
    return statementType;
  }

  public boolean isDebug() {
    return isDebug;
  }

  public void setDebug(final boolean debug) {
    isDebug = debug;
  }

  public boolean isQuery() {
    return false;
  }

  public abstract List<? extends PartialPath> getPaths();

  public org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Statement toRelationalStatement(
      final MPPQueryContext context) {
    throw new UnsupportedOperationException("Method not implemented yet");
  }

  public String getPipeLoggingString() {
    return toString();
  }

  /**
   * Checks whether this statement should be split into multiple sub-statements based on the given
   * async requirement. Used to limit resource consumption during statement analysis, etc.
   *
   * @param requireAsync whether async execution is required
   * @return true if the statement should be split, false otherwise. Default implementation returns
   *     false.
   */
  public boolean shouldSplit() {
    return false;
  }

  /**
   * Splits the current statement into multiple sub-statements. Used to limit resource consumption
   * during statement analysis, etc.
   *
   * @return the list of sub-statements. Default implementation returns empty list.
   */
  public List<? extends Statement> getSubStatements() {
    return Collections.emptyList();
  }
}
