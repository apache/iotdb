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

import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Node;

import com.google.common.graph.SuccessorsFunction;
import com.google.common.graph.Traverser;

import java.util.List;
import java.util.function.BiFunction;
import java.util.stream.Stream;

import static com.google.common.collect.Streams.stream;
import static java.util.Objects.requireNonNull;

public final class AstUtils {
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

  private AstUtils() {}
}
