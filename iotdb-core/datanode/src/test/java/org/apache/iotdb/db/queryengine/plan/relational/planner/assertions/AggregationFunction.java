/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.db.queryengine.plan.relational.planner.assertions;

import org.apache.iotdb.db.queryengine.plan.relational.planner.OrderingScheme;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;

import java.util.List;
import java.util.Optional;

public class AggregationFunction {
  private final String name;
  private final Optional<Symbol> filter;
  private final Optional<OrderingScheme> orderBy;
  private final boolean distinct;
  private final List<Expression> arguments;

  public AggregationFunction(
      String name,
      Optional<Symbol> filter,
      Optional<OrderingScheme> orderBy,
      boolean distinct,
      List<Expression> arguments) {
    this.name = name;
    this.filter = filter;
    this.orderBy = orderBy;
    this.distinct = distinct;
    this.arguments = arguments;
  }

  public String getName() {
    return name;
  }

  public Optional<Symbol> getFilter() {
    return filter;
  }

  public Optional<OrderingScheme> getOrderBy() {
    return orderBy;
  }

  public boolean isDistinct() {
    return distinct;
  }

  public List<Expression> getArguments() {
    return arguments;
  }
}
