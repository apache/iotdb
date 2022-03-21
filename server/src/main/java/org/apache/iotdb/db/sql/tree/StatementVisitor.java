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

package org.apache.iotdb.db.sql.tree;

import org.apache.iotdb.db.sql.statement.Statement;
import org.apache.iotdb.db.sql.statement.StatementNode;
import org.apache.iotdb.db.sql.statement.crud.InsertStatement;
import org.apache.iotdb.db.sql.statement.crud.QueryStatement;

public abstract class StatementVisitor<R, C> {

  public R process(StatementNode node) {
    return process(node, null);
  }

  public R process(StatementNode node, C context) {
    return node.accept(this, context);
  }

  /** Top Level Description */
  public R visitNode(StatementNode node, C context) {
    return null;
  }

  public R visitStatement(Statement statement, C context) {
    return visitNode(statement, context);
  }

  /** Data Manipulation Language (DML) */

  // Select Statement
  public R visitQuery(QueryStatement queryStatement, C context) {
    return visitStatement(queryStatement, context);
  }

  // Insert Statement
  public R visitInsert(InsertStatement insertStatement, C context) {
    return visitStatement(insertStatement, context);
  }
}
