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

import org.apache.iotdb.commons.udf.builtin.relational.TableBuiltinAggregationFunction;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DefaultExpressionTraversalVisitor;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DereferenceExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.FunctionCall;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Identifier;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Node;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.QualifiedName;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.function.Predicate;

import static com.google.common.base.Predicates.alwaysTrue;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Streams.stream;
import static java.util.Objects.requireNonNull;

public final class ExpressionTreeUtils {
  private ExpressionTreeUtils() {}

  static List<FunctionCall> extractAggregateFunctions(Iterable<? extends Node> nodes) {
    return extractExpressions(nodes, FunctionCall.class, ExpressionTreeUtils::isAggregation);
  }

  public static <T extends Expression> List<T> extractExpressions(
      Iterable<? extends Node> nodes, Class<T> clazz) {
    return extractExpressions(nodes, clazz, alwaysTrue());
  }

  private static <T extends Expression> List<T> extractExpressions(
      Iterable<? extends Node> nodes, Class<T> clazz, Predicate<T> predicate) {
    requireNonNull(nodes, "nodes is null");
    requireNonNull(clazz, "clazz is null");
    requireNonNull(predicate, "predicate is null");

    return stream(nodes)
        .flatMap(node -> linearizeNodes(node).stream())
        .filter(clazz::isInstance)
        .map(clazz::cast)
        .filter(predicate)
        .collect(toImmutableList());
  }

  private static boolean isAggregation(FunctionCall functionCall) {
    return isAggregationFunction(functionCall.getName().toString());
  }

  private static List<Node> linearizeNodes(Node node) {
    ImmutableList.Builder<Node> nodes = ImmutableList.builder();
    new DefaultExpressionTraversalVisitor<Void>() {
      @Override
      public Void process(Node node, Void context) {
        super.process(node, context);
        nodes.add(node);
        return null;
      }
    }.process(node, null);
    return nodes.build();
  }

  public static QualifiedName asQualifiedName(Expression expression) {
    QualifiedName name = null;
    if (expression instanceof Identifier) {
      name = QualifiedName.of(((Identifier) expression).getValue());
    } else if (expression instanceof DereferenceExpression) {
      name = DereferenceExpression.getQualifiedName((DereferenceExpression) expression);
    }
    return name;
  }

  static boolean isAggregationFunction(String functionName) {
    // TODO consider UDAF
    return TableBuiltinAggregationFunction.getBuiltInAggregateFunctionName()
        .contains(functionName.toLowerCase());
  }
}
