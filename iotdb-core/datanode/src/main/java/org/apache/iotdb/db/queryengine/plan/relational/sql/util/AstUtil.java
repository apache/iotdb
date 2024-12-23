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

package org.apache.iotdb.db.queryengine.plan.relational.sql.util;

import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Identifier;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Literal;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Node;

import com.google.common.graph.SuccessorsFunction;
import com.google.common.graph.Traverser;

import java.util.List;
import java.util.OptionalInt;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Stream;

import static com.google.common.collect.Streams.stream;
import static java.util.Objects.requireNonNull;

public final class AstUtil {

  public static Stream<Node> preOrder(Node node) {
    return stream(
        Traverser.forTree((SuccessorsFunction<Node>) Node::getChildren)
            .depthFirstPreOrder(requireNonNull(node, "node is null")));
  }

  /**
   * Compares two AST trees recursively by applying the provided comparator to each pair of nodes.
   *
   * <p>The comparator can perform a hybrid shallow/deep comparison. If it returns true or false,
   * the nodes and any subtrees are considered equal or different, respectively. If it returns null,
   * the nodes are considered shallowly-equal and their children will be compared recursively.
   */
  public static boolean treeEqual(
      Node left, Node right, BiFunction<Node, Node, Boolean> subtreeComparator) {
    Boolean equal = subtreeComparator.apply(left, right);

    if (equal != null) {
      return equal;
    }

    List<? extends Node> leftChildren = left.getChildren();
    List<? extends Node> rightChildren = right.getChildren();

    if (leftChildren.size() != rightChildren.size()) {
      return false;
    }

    for (int i = 0; i < leftChildren.size(); i++) {
      if (!treeEqual(leftChildren.get(i), rightChildren.get(i), subtreeComparator)) {
        return false;
      }
    }

    return true;
  }

  /**
   * Computes a hash of the given AST by applying the provided subtree hasher at each level.
   *
   * <p>If the hasher returns a non-empty {@link OptionalInt}, the value is treated as the hash for
   * the subtree at that node. Otherwise, the hashes of its children are computed and combined.
   */
  public static int treeHash(Node node, Function<Node, OptionalInt> subtreeHasher) {
    OptionalInt hash = subtreeHasher.apply(node);

    if (hash.isPresent()) {
      return hash.getAsInt();
    }

    List<? extends Node> children = node.getChildren();

    int result = node.getClass().hashCode();
    for (Node element : children) {
      result = 31 * result + treeHash(element, subtreeHasher);
    }

    return result;
  }

  public static Object expressionToTsValue(Expression expression) {
    if (expression instanceof Literal) {
      return ((Literal) expression).getTsValue();
    }
    if (expression instanceof Identifier) {
      throw new SemanticException(
          String.format("Cannot insert identifier %s, please use string literal", expression));
    }
    throw new SemanticException("Unsupported expression: " + expression);
  }

  private AstUtil() {}
}
