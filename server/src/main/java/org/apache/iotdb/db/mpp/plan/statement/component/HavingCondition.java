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

package org.apache.iotdb.db.mpp.plan.statement.component;

import org.apache.iotdb.db.mpp.plan.analyze.ExpressionAnalyzer;
import org.apache.iotdb.db.mpp.plan.expression.Expression;
import org.apache.iotdb.db.mpp.plan.statement.StatementNode;

/** This class maintains information of {@code HAVING} clause. */
public class HavingCondition extends StatementNode {

  private Expression predicate;

  public HavingCondition() {}

  public HavingCondition(Expression predicate) {
    // cast functionName to lowercase in havingExpression
    this.predicate = ExpressionAnalyzer.removeAliasFromExpression(predicate);
  }

  public Expression getPredicate() {
    return predicate;
  }

  public void setPredicate(Expression predicate) {
    this.predicate = ExpressionAnalyzer.removeAliasFromExpression(predicate);
  }

  public String toSQLString() {
    return "HAVING " + predicate.toString();
  }
}
