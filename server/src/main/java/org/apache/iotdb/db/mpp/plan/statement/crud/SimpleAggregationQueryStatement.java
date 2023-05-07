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

package org.apache.iotdb.db.mpp.plan.statement.crud;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.mpp.plan.analyze.ExpressionAnalyzer;
import org.apache.iotdb.db.mpp.plan.expression.Expression;
import org.apache.iotdb.db.mpp.plan.expression.leaf.TimeSeriesOperand;
import org.apache.iotdb.db.mpp.plan.statement.StatementVisitor;
import org.apache.iotdb.db.mpp.plan.statement.component.ResultColumn;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

public class SimpleAggregationQueryStatement extends QueryStatement {

  private List<PartialPath> sourcePaths;

  public List<PartialPath> getSourcePaths() {
    if (sourcePaths == null) {
      sourcePaths = new ArrayList<>();
      for (ResultColumn resultColumn : selectComponent.getResultColumns()) {
        List<Expression> sourceExpressions =
            ExpressionAnalyzer.searchSourceExpressions(resultColumn.getExpression());
        for (Expression sourceExpression : sourceExpressions) {
          sourcePaths.add(((TimeSeriesOperand) sourceExpression).getPath());
        }
      }
    }
    return sourcePaths;
  }

  @Override
  public List<PartialPath> getPaths() {
    return new ArrayList<>(new HashSet<>(getSourcePaths()));
  }

  @Override
  public <R, C> R accept(StatementVisitor<R, C> visitor, C context) {
    return visitor.visitSimpleAggregationQuery(this, context);
  }

  public void semanticCheck() {}
}
