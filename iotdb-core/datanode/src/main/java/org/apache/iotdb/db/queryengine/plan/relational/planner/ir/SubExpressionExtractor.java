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
package org.apache.iotdb.db.queryengine.plan.relational.planner.ir;

import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Node;

import com.google.common.graph.SuccessorsFunction;
import com.google.common.graph.Traverser;

import java.util.stream.Stream;

import static com.google.common.collect.Streams.stream;
import static java.util.Objects.requireNonNull;

/**
 * Extracts and returns the stream of all expression subtrees within an Expression, including
 * Expression itself
 */
public final class SubExpressionExtractor {
  private SubExpressionExtractor() {}

  public static Stream<Expression> extract(Expression expression) {
    return preOrder(expression).filter(Expression.class::isInstance).map(Expression.class::cast);
  }

  public static Stream<Node> preOrder(Node node) {
    return stream(
        Traverser.forTree((SuccessorsFunction<Node>) Node::getChildren)
            .depthFirstPreOrder(requireNonNull(node, "node is null")));
  }
}
