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
package org.apache.iotdb.db.metadata.mtree.traverser;

import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.metadata.template.Template;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.utils.Pair;

import org.apache.commons.lang3.StringUtils;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.iotdb.db.conf.IoTDBConstant.MULTI_LEVEL_PATH_WILDCARD;
import static org.apache.iotdb.db.conf.IoTDBConstant.ONE_LEVEL_PATH_WILDCARD;

/**
 * This class defines the main traversal framework and declares some methods for result process
 * extension. This class could be extended to implement concrete tasks. <br>
 * Currently, the tasks are classified into two type:
 *
 * <ol>
 *   <li>counter: to count the node num or measurement num that matches the path pattern
 *   <li>collector: to collect customized results of the matched node or measurement
 * </ol>
 */
public abstract class Traverser {

  protected IMNode startNode;
  protected String[] nodes;

  // to construct full path or find mounted node on MTree when traverse into template
  protected Deque<IMNode> traverseContext;

  // if true, measurement in template should be processed
  protected boolean shouldTraverseTemplate = false;

  // default false means fullPath pattern match
  protected boolean isPrefixMatch = false;

  // if matched, Support new path pattern: 0 or more layers
  protected Pattern pattern = Pattern.compile("(\\*\\()(\\d*)(,?)(\\d*)(\\))");

  /**
   * To traverse subtree under root.sg, e.g., init Traverser(root, "root.sg.**")
   *
   * @param startNode denote which tree to traverse by passing its root
   * @param path use wildcard to specify which part to traverse
   * @throws MetadataException
   */
  public Traverser(IMNode startNode, PartialPath path) throws MetadataException {
    String[] nodes = path.getNodes();
    if (nodes.length == 0 || !nodes[0].equals(startNode.getName())) {
      throw new IllegalPathException(
          path.getFullPath(), path.getFullPath() + " doesn't start with " + startNode.getName());
    }
    this.startNode = startNode;
    this.nodes = nodes;
    this.traverseContext = new ArrayDeque<>();
  }

  /**
   * The interface to start the traversal. The node process should be defined before traversal by
   * overriding or implement concerned methods.
   */
  public void traverse() throws MetadataException {
    traverse(startNode, 0, 0, -1, -1);
  }

  /**
   * The recursive method for MTree traversal. If the node matches nodes[idx], then do some
   * operation and traverse the children with nodes[idx+1].
   *
   * @param node current node that match the targetName in given path
   * @param idx the index of targetName in given path
   * @param level the level of current node in MTree
   * @throws MetadataException some result process may throw MetadataException
   */
  protected void traverse(IMNode node, int idx, int level, int minMatchLevel, int maxMatchLevel)
      throws MetadataException {

    if (processMatchedMNode(node, idx, level, minMatchLevel)) {
      return;
    }

    if (idx >= nodes.length - 1) {
      if (minMatchLevel > 0) {
        processOneLayers(node, idx, level, minMatchLevel, maxMatchLevel);
        return;
      }
      if (maxMatchLevel > 0 || isPrefixMatch) {
        processOneLayers(node, idx, level, minMatchLevel, maxMatchLevel);
      }
      return;
    }

    if (node.isMeasurement()) {
      return;
    }

    if (minMatchLevel > 0) {
      processOneLayers(node, idx, level, minMatchLevel, maxMatchLevel);
      return;
    }
    if (maxMatchLevel > 0) {
      processOneLayers(node, idx, level, minMatchLevel, maxMatchLevel);
    }

    String targetName = nodes[idx + 1];
    Pair<Integer, Integer> matchLevelPair = processNodeName(targetName);
    if (matchLevelPair.left == 0) {
      processZeroLayer(node, idx, level, matchLevelPair);
    }
    if (matchLevelPair.left > 0 && matchLevelPair.right > 0) {
      processMinMaxLayers(node, idx, level, matchLevelPair);
    } else if (targetName.contains("*")) {
      processNameWithWildcardLayers(node, idx, level, targetName);
    } else {
      processNameLayers(node, idx, level, targetName);
    }
  }

  /**
   * 1.*(start, end) - start (included) to end (excluded) layers 2.*( , end) - 0 to end (excluded)
   * layers 3.*(start, ) - start (included) or more layers, ** stands for *(1, ) 4.*( , ) - 0 or
   * more layers, maybe we can use a shortcut (such as ***)? 5.*( n ) - exactly n layers, * stands
   * for *(1)
   *
   * @param nodeName given path
   * @return left is min layers;right is max layers
   */
  private Pair<Integer, Integer> processNodeName(String nodeName) {
    Pair<Integer, Integer> pair = new Pair<>(-1, -1);
    Matcher matcher = pattern.matcher(nodeName);
    if (matcher.find()) {
      if (StringUtils.isBlank(matcher.group(2)) && StringUtils.isBlank(matcher.group(4))) {
        pair.left = 0;
        pair.right = Integer.MAX_VALUE;
        return pair;
      } else if (StringUtils.isNotBlank(matcher.group(2))
          && StringUtils.isBlank(matcher.group(3))) {
        pair.left = Integer.parseInt(matcher.group(2));
        pair.right = Integer.parseInt(matcher.group(2));
        return pair;
      }
      if (StringUtils.isNotBlank(matcher.group(2))) {
        pair.left = Integer.parseInt(matcher.group(2));
      } else {
        pair.left = 0;
      }
      if (StringUtils.isNotBlank(matcher.group(4))) {
        pair.right = Integer.parseInt(matcher.group(4)) - 1;
      } else {
        pair.right = Integer.MAX_VALUE;
      }
    } else if (MULTI_LEVEL_PATH_WILDCARD.equals(nodeName)) {
      pair.left = 1;
      pair.right = Integer.MAX_VALUE;
    } else if (ONE_LEVEL_PATH_WILDCARD.equals(nodeName)) {
      pair.left = 1;
      pair.right = 1;
    }
    return pair;
  }

  private void processOneLayers(
      IMNode node, int idx, int level, int minMatchLevel, int maxMatchLevel)
      throws MetadataException {
    traverseContext.push(node);
    for (IMNode child : node.getChildren().values()) {
      traverse(child, idx, level + 1, minMatchLevel - 1, maxMatchLevel - 1);
    }
    traverseContext.pop();

    if (!shouldTraverseTemplate) {
      return;
    }

    if (!node.isUseTemplate()) {
      return;
    }

    Template upperTemplate = node.getUpperTemplate();
    traverseContext.push(node);
    for (IMNode child : upperTemplate.getDirectNodes()) {
      traverse(child, idx, level + 1, minMatchLevel - 1, maxMatchLevel - 1);
    }
    traverseContext.pop();
  }

  private void processNameWithWildcardLayers(IMNode node, int idx, int level, String targetName)
      throws MetadataException {
    String targetNameRegex = nodes[idx + 1].replace("*", ".*");
    traverseContext.push(node);
    for (IMNode child : node.getChildren().values()) {
      if (child.isMeasurement()) {
        String alias = child.getAsMeasurementMNode().getAlias();
        if (!Pattern.matches(targetNameRegex, child.getName())
            && !(alias != null && Pattern.matches(targetNameRegex, alias))) {
          continue;
        }
      } else {
        if (!Pattern.matches(targetNameRegex, child.getName())) {
          continue;
        }
      }
      traverse(child, idx + 1, level + 1, -1, -1);
    }
    traverseContext.pop();

    if (!shouldTraverseTemplate) {
      return;
    }

    if (!node.isUseTemplate()) {
      return;
    }

    Template upperTemplate = node.getUpperTemplate();
    IMNode directNode = upperTemplate.getDirectNode(targetName);
    if (directNode != null) {
      traverseContext.push(node);
      traverse(directNode, idx + 1, level + 1, -1, -1);
      traverseContext.pop();
    }
  }

  private void processNameLayers(IMNode node, int idx, int level, String targetName)
      throws MetadataException {
    IMNode next = node.getChild(targetName);
    if (next != null) {
      traverseContext.push(node);
      traverse(next, idx + 1, level + 1, -1, -1);
      traverseContext.pop();
    }
    if (!shouldTraverseTemplate) {
      return;
    }
    if (!node.isUseTemplate()) {
      return;
    }
    Template upperTemplate = node.getUpperTemplate();
    IMNode directNode = upperTemplate.getDirectNode(targetName);
    if (directNode != null) {
      traverseContext.push(node);
      traverse(directNode, idx + 1, level + 1, -1, -1);
      traverseContext.pop();
    }
  }

  private void processZeroLayer(
      IMNode node, int idx, int level, Pair<Integer, Integer> matchLevelPair)
      throws MetadataException {
    traverse(node, idx + 1, level, -1, matchLevelPair.right);
    matchLevelPair.left = -1;
    matchLevelPair.right--;

    if (!shouldTraverseTemplate) {
      return;
    }

    if (!node.isUseTemplate()) {
      return;
    }

    traverseContext.push(node);
    traverse(node, idx + 1, level, -1, matchLevelPair.right);
    traverseContext.pop();
  }

  private void processMinMaxLayers(
      IMNode node, int idx, int level, Pair<Integer, Integer> matchLevelPair)
      throws MetadataException {
    traverseContext.push(node);
    for (IMNode child : node.getChildren().values()) {
      traverse(child, idx + 1, level + 1, matchLevelPair.left - 1, matchLevelPair.right - 1);
    }
    traverseContext.pop();

    if (!shouldTraverseTemplate) {
      return;
    }

    if (!node.isUseTemplate()) {
      return;
    }

    Template upperTemplate = node.getUpperTemplate();
    traverseContext.push(node);
    for (IMNode child : upperTemplate.getDirectNodes()) {
      traverse(child, idx + 1, level + 1, matchLevelPair.left - 1, matchLevelPair.right - 1);
    }
    traverseContext.pop();
  }

  /**
   * process curNode that matches the targetName during traversal. there are two cases: 1. internal
   * match: root.sg internal match root.sg.**(pattern) 2. full match: root.sg.d full match
   * root.sg.**(pattern) Both of them are default abstract and should be implemented according
   * concrete tasks.
   *
   * @return whether this branch of recursive traversal should stop; if true, stop
   */
  private boolean processMatchedMNode(IMNode node, int idx, int level, int minMatchLevel)
      throws MetadataException {
    if (idx < nodes.length - 1 || minMatchLevel > 0) {
      return processInternalMatchedMNode(node, idx, level);
    } else {
      return processFullMatchedMNode(node, idx, level);
    }
  }

  /**
   * internal match: root.sg internal match root.sg.**(pattern)
   *
   * @return whether this branch of recursive traversal should stop; if true, stop
   */
  protected abstract boolean processInternalMatchedMNode(IMNode node, int idx, int level)
      throws MetadataException;

  /**
   * full match: root.sg.d full match root.sg.**(pattern)
   *
   * @return whether this branch of recursive traversal should stop; if true, stop
   */
  protected abstract boolean processFullMatchedMNode(IMNode node, int idx, int level)
      throws MetadataException;

  public void setPrefixMatch(boolean isPrefixMatch) {
    this.isPrefixMatch = isPrefixMatch;
  }

  public void setShouldTraverseTemplate(boolean shouldTraverseTemplate) {
    this.shouldTraverseTemplate = shouldTraverseTemplate;
  }

  /**
   * @param currentNode the node need to get the full path of
   * @return full path from traverse start node to the current node
   */
  protected PartialPath getCurrentPartialPath(IMNode currentNode) throws IllegalPathException {
    Iterator<IMNode> nodes = traverseContext.descendingIterator();
    StringBuilder builder =
        nodes.hasNext() ? new StringBuilder(nodes.next().getName()) : new StringBuilder();
    while (nodes.hasNext()) {
      IMNode node = nodes.next();
      if (node == currentNode) {
        break;
      }
      builder.append(TsFileConstant.PATH_SEPARATOR);
      builder.append(node.getName());
    }
    if (builder.length() != 0) {
      builder.append(TsFileConstant.PATH_SEPARATOR);
    }
    builder.append(currentNode.getName());
    return new PartialPath(builder.toString());
  }

  /** @return the storage group node in the traverse path */
  protected IMNode getStorageGroupNodeInTraversePath() {
    Iterator<IMNode> nodes = traverseContext.iterator();
    while (nodes.hasNext()) {
      IMNode node = nodes.next();
      if (node.isStorageGroup()) {
        return node;
      }
    }
    return null;
  }
}
