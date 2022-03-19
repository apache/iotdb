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

package org.apache.iotdb.db.sql.statement;

import org.apache.iotdb.db.sql.constant.StatementType;
import org.apache.iotdb.db.sql.parser.ASTVisitor;

/**
 * This class is a superclass of all statements.
 *
 * <p>A Statement containing all semantic information of an SQL. It is obtained by traversing the
 * AST via {@link ASTVisitor}.
 */
public abstract class Statement {

  protected StatementType statementType = StatementType.NULL;

  protected Statement() {}

  public void setType(StatementType statementType) {
    this.statementType = statementType;
  }

  public StatementType getType() {
    return statementType;
  }

  public boolean isQuery() {
    return statementType == StatementType.QUERY;
  }
}
