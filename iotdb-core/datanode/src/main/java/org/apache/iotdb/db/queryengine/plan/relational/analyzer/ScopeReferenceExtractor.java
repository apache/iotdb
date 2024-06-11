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

import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Node;

import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.util.AstUtil.preOrder;

/** Extract expressions that are references to a given scope. */
final class ScopeReferenceExtractor {
  private ScopeReferenceExtractor() {}

  public static boolean hasReferencesToScope(Node node, Analysis analysis, Scope scope) {
    return getReferencesToScope(node, analysis, scope).findAny().isPresent();
  }

  public static Stream<Expression> getReferencesToScope(Node node, Analysis analysis, Scope scope) {
    Map<NodeRef<Expression>, ResolvedField> columnReferences = analysis.getColumnReferenceFields();

    return preOrder(node)
        .filter(Expression.class::isInstance)
        .map(Expression.class::cast)
        .filter(expression -> columnReferences.containsKey(NodeRef.of(expression)))
        .filter(expression -> isReferenceToScope(expression, scope, columnReferences));
  }

  private static boolean isReferenceToScope(
      Expression node, Scope scope, Map<NodeRef<Expression>, ResolvedField> columnReferences) {
    ResolvedField field = columnReferences.get(NodeRef.of(node));
    requireNonNull(field, () -> "No Field for " + node);
    return isFieldFromScope(field.getFieldId(), scope);
  }

  public static boolean isFieldFromScope(FieldId field, Scope scope) {
    return Objects.equals(field.getRelationId(), scope.getRelationId());
  }
}
