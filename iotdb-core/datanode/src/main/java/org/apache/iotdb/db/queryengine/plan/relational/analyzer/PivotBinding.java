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

package org.apache.iotdb.db.queryengine.plan.relational.analyzer;

import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.FunctionCall;

import java.util.List;

/** The analysis-time output layout and typed masks of a static PIVOT. */
public class PivotBinding {
  public record Output(Expression mask, int aggregateIndex) {}

  private final List<Integer> groupingFields;
  private final List<Expression> onExpressions;
  private final List<FunctionCall> aggregates;
  private final List<Expression> finalExpressions;
  private final List<Output> outputs;

  public PivotBinding(
      List<Integer> groupingFields,
      List<Expression> onExpressions,
      List<FunctionCall> aggregates,
      List<Expression> finalExpressions,
      List<Output> outputs) {
    this.groupingFields = List.copyOf(groupingFields);
    this.onExpressions = List.copyOf(onExpressions);
    this.aggregates = List.copyOf(aggregates);
    this.finalExpressions = List.copyOf(finalExpressions);
    this.outputs = List.copyOf(outputs);
  }

  public List<Integer> getGroupingFields() {
    return groupingFields;
  }

  public List<Expression> getOnExpressions() {
    return onExpressions;
  }

  public List<FunctionCall> getAggregates() {
    return aggregates;
  }

  public List<Expression> getFinalExpressions() {
    return finalExpressions;
  }

  public List<Output> getOutputs() {
    return outputs;
  }
}
