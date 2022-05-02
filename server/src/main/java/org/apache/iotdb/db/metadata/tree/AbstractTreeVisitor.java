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

package org.apache.iotdb.db.metadata.tree;

import org.apache.iotdb.db.metadata.path.PartialPath;

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

  protected final N root;
  protected final String[] nodes;
  protected final boolean isPrefixMatch;

  protected final Deque<VisitorStackEntry<N>> visitorStack = new ArrayDeque<>();
  protected final Deque<N> ancestorStack = new ArrayDeque<>();

  protected N nextMatchedNode;

  protected AbstractTreeVisitor(N root, PartialPath pathPattern, boolean isPrefixMatch) {
    this.root = root;
    this.nodes = optimizePathPattern(pathPattern);
    this.isPrefixMatch = isPrefixMatch;

    visitorStack.push(
        new VisitorStackEntry<>(Collections.singletonList(root).iterator(), 0, 0, -1));
  }

  /**
   * Optimize the given path pattern. Currently, the node name used for one level match will be
   * transformed into a regex.
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
    R result = generateResult();
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
        if (processFullMatchedNode(node)) {
          return;
        }

        if (!isLeafNode(node)) {
          pushAllChildren(node, patternIndex, lastMultiLevelWildcardIndex);
        }

        if (nextMatchedNode != null) {
          return;
        }

        continue;
      }

      if (checkIsMatch(patternIndex, node)) {
        if (patternIndex == nodes.length - 1) {
          if (processFullMatchedNode(node)) {
            return;
          }

          if (!isLeafNode(node)) {
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

          if (nextMatchedNode != null) {
            return;
          }

          continue;
        }

        if (processInternalMatchedNode(node)) {
          return;
        }

        if (!isLeafNode(node)) {
          if (nodes[patternIndex + 1].equals(MULTI_LEVEL_PATH_WILDCARD)) {
            pushAllChildren(node, patternIndex + 1, patternIndex + 1);
          } else {
            if (lastMultiLevelWildcardIndex > -1) {
              pushAllChildren(node, patternIndex + 1, lastMultiLevelWildcardIndex);
            } else if (nodes[patternIndex + 1].contains(ONE_LEVEL_PATH_WILDCARD)) {
              pushAllChildren(node, patternIndex + 1, lastMultiLevelWildcardIndex);
            } else {
              pushSingleChild(
                  node, nodes[patternIndex + 1], patternIndex + 1, lastMultiLevelWildcardIndex);
            }
          }
        }

      } else {
        if (lastMultiLevelWildcardIndex == -1) {
          continue;
        }

        int lastMatchIndex = findLastMatch(node, patternIndex, lastMultiLevelWildcardIndex);

        if (processInternalMatchedNode(node)) {
          return;
        }

        if (!isLeafNode(node)) {
          pushAllChildren(node, lastMatchIndex + 1, lastMultiLevelWildcardIndex);
        }
      }

      if (nextMatchedNode != null) {
        return;
      }
    }
  }

  private int findLastMatch(N node, int patternIndex, int lastMultiLevelWildcardIndex) {
    for (int i = patternIndex - 1; i > lastMultiLevelWildcardIndex; i--) {
      if (!checkIsMatch(i, node)) {
        continue;
      }

      Iterator<N> ancestors = ancestorStack.iterator();
      boolean allMatch = true;
      for (int j = i - 1; j > lastMultiLevelWildcardIndex; j--) {
        if (!checkIsMatch(j, ancestors.next())) {
          allMatch = false;
          break;
        }
      }

      if (allMatch) {
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

  protected void pushSingleChild(
      N parent, String childName, int patternIndex, int lastMultiLevelWildcardIndex) {
    N child = getChild(parent, childName);
    if (child != null) {
      ancestorStack.push(parent);
      visitorStack.push(
          new VisitorStackEntry<>(
              Collections.singletonList(child).iterator(),
              patternIndex,
              ancestorStack.size(),
              lastMultiLevelWildcardIndex));
    }
  }

  protected void pushAllChildren(N parent, int patternIndex, int lastMultiLevelWildcardIndex) {
    ancestorStack.push(parent);
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
    Iterator<N> iterator = ancestorStack.descendingIterator();
    while (iterator.hasNext()) {
      nodeNames.add(iterator.next().getName());
    }
    nodeNames.add(node.getName());
    return nodeNames.toArray(new String[0]);
  }

  // Check whether the given node is a leaf node of this tree.
  protected abstract boolean isLeafNode(N node);

  // Get a child with the given childName.
  protected abstract N getChild(N parent, String childName);

  // Get a iterator of all children.
  protected abstract Iterator<N> getChildrenIterator(N parent);

  /**
   * Internal-match means the node matches an internal node name of the given path pattern. root.sg
   * internal match root.sg.**(pattern). This method should be implemented according to concrete
   * tasks.
   *
   * <p>If return true, the traversing process won't check the subtree with the given node as root,
   * and the result will be return immediately. If return false, the traversing process will keep
   * traversing the subtree.
   */
  protected abstract boolean processInternalMatchedNode(N node);

  /**
   * Full-match means the node matches the last node name of the given path pattern. root.sg.d full
   * match root.sg.**(pattern) This method should be implemented according to concrete tasks.
   *
   * <p>If return true, the traversing process won't check the subtree with the given node as root,
   * and the result will be return immediately. f return false, the traversing process will keep
   * traversing the subtree.
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
}
