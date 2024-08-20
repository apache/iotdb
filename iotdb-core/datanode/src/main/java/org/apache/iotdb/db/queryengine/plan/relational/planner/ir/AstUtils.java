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
