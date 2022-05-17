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

package org.apache.iotdb.commons.schema.tree;

import org.apache.iotdb.commons.path.PartialPath;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.regex.Pattern;

import static org.apache.iotdb.commons.conf.IoTDBConstant.MULTI_LEVEL_PATH_WILDCARD;
import static org.apache.iotdb.commons.conf.IoTDBConstant.ONE_LEVEL_PATH_WILDCARD;

/**
 * This class defines a dfs-based algorithm of tree-traversing with path pattern match, and support
 * iterating each element of the result.
 *
 * <p>This class takes three basis parameters as input:
 *
 * <ol>
 *   <li>N root: the root node of the tree to be traversed.
 *   <li>PartialPath patPattern: the pattern of path that the path of target element matches
 *   <li>boolean isPrefixMatch: whether the pathPattern is used for matching the prefix; if so, all
 *       elements with path starting with the matched prefix will be collected
 * </ol>
 *
 * <p>If any tree wants to integrate and use this class. The following steps must be attained:
 *
 * <ol>
 *   <li>The node of the tree must implement ITreeNode interface and the generic N should be defined
 *       as the node class.
 *   <li>The result type R should be defined.
 *   <li>Implement the abstract methods, and for the concrete requirements, please refer to the
 *       javadoc of specific method.
 * </ol>
 *
 * @param <N> The node consisting the tree.
 * @param <R> The result extracted from the tree.
 */
public abstract class AbstractTreeVisitor<N extends ITreeNode, R> implements Iterator<R> {

  // command parameters
  protected final N root;
  protected final String[] nodes;
  protected final boolean isPrefixMatch;

  // run time variables
  protected final Deque<VisitorStackEntry<N>> visitorStack = new ArrayDeque<>();
  protected final Deque<AncestorStackEntry<N>> ancestorStack = new ArrayDeque<>();
  protected boolean shouldVisitSubtree;

  // result variables
  protected N nextMatchedNode;
  protected int patternIndexOfMatchedNode;
  protected int lastMultiLevelWildcardIndexOfMatchedNode;

  protected AbstractTreeVisitor(N root, PartialPath pathPattern, boolean isPrefixMatch) {
    this.root = root;
    this.nodes = optimizePathPattern(pathPattern);
    this.isPrefixMatch = isPrefixMatch;

    visitorStack.push(
        new VisitorStackEntry<>(Collections.singletonList(root).iterator(), 0, 0, -1));
  }

  /**
   * Optimize the given path pattern. Currently, the node name used for one level match will be
   * transformed into a regex. e.g. given pathPattern {"root", "sg", "d*", "s"} and the
   * optimizedPathPattern is {"root", "sg", "d.*", "s"}.
   */
  private String[] optimizePathPattern(PartialPath pathPattern) {
    String[] rawNodes = pathPattern.getNodes();
    List<String> optimizedNodes = new ArrayList<>(rawNodes.length);
    for (String rawNode : rawNodes) {
      if (rawNode.equals(MULTI_LEVEL_PATH_WILDCARD)) {
        optimizedNodes.add(MULTI_LEVEL_PATH_WILDCARD);
      } else if (rawNode.contains(ONE_LEVEL_PATH_WILDCARD)) {
        optimizedNodes.add(rawNode.replace("*", ".*"));
      } else {
        optimizedNodes.add(rawNode);
      }
    }

    return optimizedNodes.toArray(new String[0]);
  }

  @Override
  public boolean hasNext() {
    if (nextMatchedNode == null) {
      getNext();
    }
    return nextMatchedNode != null;
  }

  @Override
  public R next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    return consumeNextMatchedNode();
  }

  private R consumeNextMatchedNode() {
    R result = generateResult();

    // after the node be consumed, the subTree should be considered.
    if (patternIndexOfMatchedNode == nodes.length) {
      pushChildrenWhilePrefixMatch(
          nextMatchedNode, patternIndexOfMatchedNode, lastMultiLevelWildcardIndexOfMatchedNode);
    } else if (patternIndexOfMatchedNode == nodes.length - 1) {
      pushChildrenWhileTail(
          nextMatchedNode, patternIndexOfMatchedNode, lastMultiLevelWildcardIndexOfMatchedNode);
    } else {
      pushChildrenWhileInternal(
          nextMatchedNode, patternIndexOfMatchedNode, lastMultiLevelWildcardIndexOfMatchedNode);
    }

    nextMatchedNode = null;
    return result;
  }

  /**
   * Basically, the algorithm traverse the tree with dfs strategy. When it comes to push children
   * into stack, the path pattern will be used to filter the children.
   *
   * <p>When there's MULTI_LEVEL_WILDCARD in given path pattern. There are the following notices:
   *
   * <ol>
   *   <li>When it comes to push children into stack and there's MULTI_LEVEL_WILDCARD before the
   *       current patternIndex, all the children will be pushed.
   *   <li>When a node cannot match the target node name in the patternIndex place and there's
   *       MULTI_LEVEL_WILDCARD before the current patternIndex, the node names between the current
   *       patternIndex and lastMultiLevelWildcardIndex will be checked until the partial path end
   *       with current node can match one. The children will be pushed with the matched index + 1.
   * </ol>
   *
   * <p>Each node and fullPath of the tree will be traversed at most once.
   */
  protected void getNext() {
    nextMatchedNode = null;
    VisitorStackEntry<N> stackEntry;
    int patternIndex;
    N node;
    Iterator<N> iterator;
    int lastMultiLevelWildcardIndex;
    while (!visitorStack.isEmpty()) {
      stackEntry = visitorStack.peek();
      iterator = stackEntry.iterator;

      if (!iterator.hasNext()) {
        popStack();
        continue;
      }

      node = iterator.next();
      patternIndex = stackEntry.patternIndex;
      lastMultiLevelWildcardIndex = stackEntry.lastMultiLevelWildcardIndex;

      // only prefixMatch
      if (patternIndex == nodes.length) {

        shouldVisitSubtree = processFullMatchedNode(node) && isInternalNode(node);

        if (nextMatchedNode != null) {
          saveNextMatchedNodeContext(patternIndex, lastMultiLevelWildcardIndex);
          return;
        }

        if (shouldVisitSubtree) {
          pushChildrenWhilePrefixMatch(node, patternIndex, lastMultiLevelWildcardIndex);
        }

        continue;
      }

      if (checkIsMatch(patternIndex, node)) {
        if (patternIndex == nodes.length - 1) {
          shouldVisitSubtree = processFullMatchedNode(node) && isInternalNode(node);

          if (nextMatchedNode != null) {
            saveNextMatchedNodeContext(patternIndex, lastMultiLevelWildcardIndex);
            return;
          }

          if (shouldVisitSubtree) {
            pushChildrenWhileTail(node, patternIndex, lastMultiLevelWildcardIndex);
          }
        } else {
          shouldVisitSubtree = processInternalMatchedNode(node) && isInternalNode(node);

          if (nextMatchedNode != null) {
            saveNextMatchedNodeContext(patternIndex, lastMultiLevelWildcardIndex);
            return;
          }

          if (shouldVisitSubtree) {
            pushChildrenWhileInternal(node, patternIndex, lastMultiLevelWildcardIndex);
          }
        }
      } else {
        if (lastMultiLevelWildcardIndex == -1) {
          continue;
        }

        int lastMatchIndex = findLastMatch(node, patternIndex, lastMultiLevelWildcardIndex);

        shouldVisitSubtree = processInternalMatchedNode(node) && isInternalNode(node);

        if (nextMatchedNode != null) {
          saveNextMatchedNodeContext(lastMatchIndex, lastMultiLevelWildcardIndex);
          return;
        }

        if (shouldVisitSubtree) {
          pushChildrenWhileInternal(node, lastMatchIndex, lastMultiLevelWildcardIndex);
        }
      }
    }
  }

  /**
   * The context, mainly the matching info, of nextedMatchedNode should be saved. When the
   * nextedMatchedNode is consumed, the saved info will be used to process its subtree.
   */
  private void saveNextMatchedNodeContext(
      int patternIndexOfMatchedNode, int lastMultiLevelWildcardIndexOfMatchedNode) {
    this.patternIndexOfMatchedNode = patternIndexOfMatchedNode;
    this.lastMultiLevelWildcardIndexOfMatchedNode = lastMultiLevelWildcardIndexOfMatchedNode;
  }

  /**
   * When current node cannot match the pattern node in nodes[patternIndex] and there is ** before
   * current pattern node, the pattern nodes before current pattern node should be checked. For
   * example, given path root.sg.d.s and path pattern root.**.s. A status, root.sg.d not match
   * root.**.s, may be reached during traversing process, then it should be checked and found that
   * root.sg.d could match root.**, after which the process could continue and find root.sg.d.s
   * matches root.**.s.
   */
  private int findLastMatch(N node, int patternIndex, int lastMultiLevelWildcardIndex) {
    for (int i = patternIndex - 1; i > lastMultiLevelWildcardIndex; i--) {
      if (!checkIsMatch(i, node)) {
        continue;
      }

      Iterator<AncestorStackEntry<N>> ancestors = ancestorStack.iterator();
      boolean allMatch = true;
      AncestorStackEntry<N> ancestor;
      for (int j = i - 1; j > lastMultiLevelWildcardIndex; j--) {
        ancestor = ancestors.next();
        if (ancestor.isMatched(j)) {
          break;
        }

        if (ancestor.hasBeenChecked(j) || !checkIsMatch(j, ancestor.node)) {
          ancestors = ancestorStack.iterator();
          for (int k = i - 1; k >= j; k--) {
            ancestors.next().setNotMatched(k);
          }
          allMatch = false;
          break;
        }
      }

      if (allMatch) {
        ancestors = ancestorStack.iterator();
        for (int k = i - 1; k > lastMultiLevelWildcardIndex; k--) {
          ancestor = ancestors.next();
          if (ancestor.isMatched(k)) {
            break;
          }
          ancestor.setMatched(k);
        }
        return i;
      }
    }
    return lastMultiLevelWildcardIndex;
  }

  public void reset() {
    visitorStack.clear();
    ancestorStack.clear();
    nextMatchedNode = null;
    visitorStack.push(
        new VisitorStackEntry<>(Collections.singletonList(root).iterator(), 0, 0, -1));
  }

  private void popStack() {
    visitorStack.pop();
    // The ancestor pop operation with level check supports the children of one node pushed by
    // batch.
    if (!visitorStack.isEmpty() && visitorStack.peek().level < ancestorStack.size()) {
      ancestorStack.pop();
    }
  }

  /**
   * This method is invoked to decide how to push children of given node. Invoked only when
   * patternIndex == nodes.length.
   *
   * @param node the current processed node
   * @param patternIndex the patternIndex for the given node
   * @param lastMultiLevelWildcardIndex the lastMultiLevelWildcardIndex of the given node
   */
  private void pushChildrenWhilePrefixMatch(
      N node, int patternIndex, int lastMultiLevelWildcardIndex) {
    pushAllChildren(node, patternIndex, lastMultiLevelWildcardIndex);
  }

  /**
   * This method is invoked to decide how to push children of given node. Invoked only when
   * patternIndex == nodes.length - 1.
   *
   * @param node the current processed node
   * @param patternIndex the patternIndex for the given node
   * @param lastMultiLevelWildcardIndex the lastMultiLevelWildcardIndex of the given node
   */
  private void pushChildrenWhileTail(N node, int patternIndex, int lastMultiLevelWildcardIndex) {
    if (nodes[patternIndex].equals(MULTI_LEVEL_PATH_WILDCARD)) {
      pushAllChildren(node, patternIndex, patternIndex);
    } else if (lastMultiLevelWildcardIndex != -1) {
      pushAllChildren(
          node,
          findLastMatch(node, patternIndex, lastMultiLevelWildcardIndex) + 1,
          lastMultiLevelWildcardIndex);
    } else if (isPrefixMatch) {
      pushAllChildren(node, patternIndex + 1, lastMultiLevelWildcardIndex);
    }
  }

  /**
   * This method is invoked to decide how to push children of given node. Invoked only when
   * patternIndex < nodes.length - 1.
   *
   * @param node the current processed node
   * @param patternIndex the patternIndex for the given node
   * @param lastMultiLevelWildcardIndex the lastMultiLevelWildcardIndex of the given node
   */
  private void pushChildrenWhileInternal(
      N node, int patternIndex, int lastMultiLevelWildcardIndex) {
    if (nodes[patternIndex + 1].equals(MULTI_LEVEL_PATH_WILDCARD)) {
      pushAllChildren(node, patternIndex + 1, patternIndex + 1);
    } else {
      if (lastMultiLevelWildcardIndex > -1) {
        pushAllChildren(node, patternIndex + 1, lastMultiLevelWildcardIndex);
      } else if (nodes[patternIndex + 1].contains(ONE_LEVEL_PATH_WILDCARD)) {
        pushAllChildren(node, patternIndex + 1, lastMultiLevelWildcardIndex);
      } else {
        pushSingleChild(node, patternIndex + 1, lastMultiLevelWildcardIndex);
      }
    }
  }

  /**
   * Push child for name match case.
   *
   * @param parent the parent node of target children
   * @param patternIndex the patternIndex to match children
   * @param lastMultiLevelWildcardIndex the lastMultiLevelWildcardIndex of child
   */
  protected void pushSingleChild(N parent, int patternIndex, int lastMultiLevelWildcardIndex) {
    N child = getChild(parent, nodes[patternIndex]);
    if (child != null) {
      ancestorStack.push(
          new AncestorStackEntry<>(
              parent,
              visitorStack.peek().patternIndex,
              visitorStack.peek().lastMultiLevelWildcardIndex));
      visitorStack.push(
          new VisitorStackEntry<>(
              Collections.singletonList(child).iterator(),
              patternIndex,
              ancestorStack.size(),
              lastMultiLevelWildcardIndex));
    }
  }

  /**
   * Push children for the following cases:
   *
   * <ol>
   *   <li>the pattern to match children is **, multiLevelWildcard
   *   <li>the pattern to match children contains *, oneLevelWildcard
   *   <li>there's ** before the patternIndex for children
   * </ol>
   *
   * @param parent the parent node of target children
   * @param patternIndex the patternIndex to match children
   * @param lastMultiLevelWildcardIndex the lastMultiLevelWildcardIndex of child
   */
  protected void pushAllChildren(N parent, int patternIndex, int lastMultiLevelWildcardIndex) {
    ancestorStack.push(
        new AncestorStackEntry<>(
            parent,
            visitorStack.peek().patternIndex,
            visitorStack.peek().lastMultiLevelWildcardIndex));
    visitorStack.push(
        new VisitorStackEntry<>(
            getChildrenIterator(parent),
            patternIndex,
            ancestorStack.size(),
            lastMultiLevelWildcardIndex));
  }

  protected boolean checkIsMatch(int patternIndex, N node) {
    if (nodes[patternIndex].equals(MULTI_LEVEL_PATH_WILDCARD)) {
      return true;
    } else if (nodes[patternIndex].contains(ONE_LEVEL_PATH_WILDCARD)) {
      return checkOneLevelWildcardMatch(nodes[patternIndex], node);
    } else {
      return checkNameMatch(nodes[patternIndex], node);
    }
  }

  protected boolean checkOneLevelWildcardMatch(String regex, N node) {
    return Pattern.matches(regex, node.getName());
  }

  protected boolean checkNameMatch(String targetName, N node) {
    return targetName.equals(node.getName());
  }

  protected String[] generateFullPathNodes(N node) {
    List<String> nodeNames = new ArrayList<>();
    Iterator<AncestorStackEntry<N>> iterator = ancestorStack.descendingIterator();
    while (iterator.hasNext()) {
      nodeNames.add(iterator.next().node.getName());
    }
    nodeNames.add(node.getName());
    return nodeNames.toArray(new String[0]);
  }

  /**
   * Check whether the given node is an internal node of this tree. Return true if the given node is
   * an internal node. Return false if the given node is a leaf node.
   */
  protected abstract boolean isInternalNode(N node);

  // Get a child with the given childName.
  protected abstract N getChild(N parent, String childName);

  // Get a iterator of all children.
  protected abstract Iterator<N> getChildrenIterator(N parent);

  /**
   * Internal-match means the node matches an internal node name of the given path pattern. root.sg
   * internal match root.sg.**(pattern). This method should be implemented according to concrete
   * tasks.
   *
   * <p>Return whether the subtree of given node should be processed. If return true, the traversing
   * process will keep traversing the subtree. If return false, the traversing process will skip the
   * subtree of given node.
   */
  protected abstract boolean processInternalMatchedNode(N node);

  /**
   * Full-match means the node matches the last node name of the given path pattern. root.sg.d full
   * match root.sg.**(pattern) This method should be implemented according to concrete tasks.
   *
   * <p>Return whether the subtree of given node should be processed. If return true, the traversing
   * process will keep traversing the subtree. If return false, the traversing process will skip the
   * subtree of given node.
   */
  protected abstract boolean processFullMatchedNode(N node);

  /** The method used for generating the result based on the matched node. */
  protected abstract R generateResult();

  protected static class VisitorStackEntry<N> {

    private final Iterator<N> iterator;
    private final int patternIndex;
    private final int level;
    private final int lastMultiLevelWildcardIndex;

    VisitorStackEntry(
        Iterator<N> iterator, int patternIndex, int level, int lastMultiLevelWildcardIndex) {
      this.iterator = iterator;
      this.patternIndex = patternIndex;
      this.level = level;
      this.lastMultiLevelWildcardIndex = lastMultiLevelWildcardIndex;
    }
  }

  protected static class AncestorStackEntry<N> {
    private final N node;
    private final int matchedIndex;
    /** Record the check result to reduce repeating check. */
    private final byte[] matchStatus;

    AncestorStackEntry(N node, int matchedIndex, int lastMultiLevelWildcardIndex) {
      this.node = node;
      this.matchedIndex = matchedIndex;
      matchStatus = new byte[matchedIndex - lastMultiLevelWildcardIndex + 1];
      matchStatus[0] = 1;
      matchStatus[matchStatus.length - 1] = 1;
    }

    public N getNode() {
      return node;
    }

    boolean hasBeenChecked(int index) {
      return matchStatus[matchedIndex - index] != 0;
    }

    boolean isMatched(int index) {
      return matchStatus[matchedIndex - index] == 1;
    }

    void setMatched(int index) {
      matchStatus[matchedIndex - index] = 1;
    }

    void setNotMatched(int index) {
      matchStatus[matchedIndex - index] = -1;
    }
  }
}
