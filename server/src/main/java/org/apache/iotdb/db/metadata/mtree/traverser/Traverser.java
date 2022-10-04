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

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mnode.iterator.IMNodeIterator;
import org.apache.iotdb.db.metadata.mtree.store.IMTreeStore;
import org.apache.iotdb.db.metadata.template.Template;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static org.apache.iotdb.commons.conf.IoTDBConstant.MULTI_LEVEL_PATH_WILDCARD;
import static org.apache.iotdb.commons.conf.IoTDBConstant.ONE_LEVEL_PATH_WILDCARD;
import static org.apache.iotdb.commons.conf.IoTDBConstant.PATH_ROOT;
import static org.apache.iotdb.db.metadata.MetadataConstant.NON_TEMPLATE;

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

  protected IMTreeStore store;

  protected IMNode startNode;
  protected String[] nodes;
  protected int startIndex;
  protected int startLevel;
  protected boolean isPrefixStart = false;

  // to construct full path or find mounted node on MTree when traverse into template
  protected Deque<IMNode> traverseContext;

  protected boolean isInTemplate = false;

  // if true, measurement in template should be processed
  protected boolean shouldTraverseTemplate = false;
  protected Map<Integer, Template> templateMap;

  // default false means fullPath pattern match
  protected boolean isPrefixMatch = false;

  /**
   * To traverse subtree under root.sg, e.g., init Traverser(root, "root.sg.**")
   *
   * @param startNode denote which tree to traverse by passing its root
   * @param path use wildcard to specify which part to traverse
   * @throws MetadataException
   */
  public Traverser(IMNode startNode, PartialPath path, IMTreeStore store) throws MetadataException {
    String[] nodes = path.getNodes();
    if (nodes.length == 0 || !nodes[0].equals(PATH_ROOT)) {
      throw new IllegalPathException(
          path.getFullPath(), path.getFullPath() + " doesn't start with " + startNode.getName());
    }
    this.startNode = startNode;
    this.nodes = nodes;
    this.store = store;
    this.traverseContext = new ArrayDeque<>();
    initStartIndexAndLevel(path);
  }

  /**
   * The traverser may start traversing from a storageGroupMNode, which is an InternalMNode of the
   * whole MTree.
   */
  private void initStartIndexAndLevel(PartialPath path) throws MetadataException {
    IMNode parent = startNode.getParent();
    Deque<IMNode> ancestors = new ArrayDeque<>();
    ancestors.push(startNode);

    startLevel = 0;
    while (parent != null) {
      startLevel++;
      traverseContext.addLast(parent);

      ancestors.push(parent);
      parent = parent.getParent();
    }

    IMNode cur;
    // given root.a.sg, accept path starting with prefix like root.a.sg, root.*.*, root.**,
    // root.a.**, which means the prefix matches the startNode's fullPath
    for (startIndex = 0; startIndex <= startLevel && startIndex < nodes.length; startIndex++) {
      cur = ancestors.pop();
      if (nodes[startIndex].equals(MULTI_LEVEL_PATH_WILDCARD)) {
        return;
      } else if (!nodes[startIndex].equals(cur.getName())
          && !nodes[startIndex].contains(ONE_LEVEL_PATH_WILDCARD)) {
        throw new IllegalPathException(
            path.getFullPath(), path.getFullPath() + " doesn't start with " + cur.getFullPath());
      }
    }

    if (startIndex <= startLevel) {
      if (!nodes[startIndex - 1].equals(MULTI_LEVEL_PATH_WILDCARD)) {
        isPrefixStart = true;
      }
    } else {
      startIndex--;
    }
  }

  /**
   * The interface to start the traversal. The node process should be defined before traversal by
   * overriding or implement concerned methods.
   */
  public void traverse() throws MetadataException {
    if (isPrefixStart && !isPrefixMatch) {
      return;
    }
    traverse(startNode, startIndex, startLevel);
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
  protected void traverse(IMNode node, int idx, int level) throws MetadataException {

    if (processMatchedMNode(node, idx, level)) {
      return;
    }

    if (idx >= nodes.length - 1) {
      if (nodes[nodes.length - 1].equals(MULTI_LEVEL_PATH_WILDCARD) || isPrefixMatch) {
        processMultiLevelWildcard(node, idx, level);
      }
      return;
    }

    if (node.isMeasurement()) {
      return;
    }

    String targetName = nodes[idx + 1];
    if (MULTI_LEVEL_PATH_WILDCARD.equals(targetName)) {
      processMultiLevelWildcard(node, idx, level);
    } else if (targetName.contains(ONE_LEVEL_PATH_WILDCARD)) {
      processOneLevelWildcard(node, idx, level);
    } else {
      processNameMatch(node, idx, level);
    }
  }

  /**
   * process curNode that matches the targetName during traversal. there are two cases: 1. internal
   * match: root.sg internal match root.sg.**(pattern) 2. full match: root.sg.d full match
   * root.sg.**(pattern) Both of them are default abstract and should be implemented according
   * concrete tasks.
   *
   * @return whether this branch of recursive traversal should stop; if true, stop
   */
  private boolean processMatchedMNode(IMNode node, int idx, int level) throws MetadataException {
    if (idx < nodes.length - 1) {
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

  protected void processMultiLevelWildcard(IMNode node, int idx, int level)
      throws MetadataException {
    if (isInTemplate) {
      traverseContext.push(node);
      for (IMNode child : node.getChildren().values()) {
        traverse(child, idx + 1, level + 1);
      }
      traverseContext.pop();
      return;
    }

    traverseContext.push(node);
    IMNode child;
    IMNodeIterator iterator = store.getChildrenIterator(node);
    try {
      while (iterator.hasNext()) {
        child = iterator.next();
        try {
          traverse(child, idx + 1, level + 1);
        } finally {
          store.unPin(child);
        }
      }
    } finally {
      iterator.close();
    }

    traverseContext.pop();

    if (!shouldTraverseTemplate) {
      return;
    }

    if (!node.isUseTemplate()) {
      return;
    }

    Template upperTemplate = getUpperTemplate(node);
    isInTemplate = true;
    traverseContext.push(node);
    for (IMNode childInTemplate : upperTemplate.getDirectNodes()) {
      traverse(childInTemplate, idx + 1, level + 1);
    }
    traverseContext.pop();
    isInTemplate = false;
  }

  protected void processOneLevelWildcard(IMNode node, int idx, int level) throws MetadataException {
    boolean multiLevelWildcard = nodes[idx].equals(MULTI_LEVEL_PATH_WILDCARD);
    String targetNameRegex = nodes[idx + 1].replace("*", ".*");

    if (isInTemplate) {
      traverseContext.push(node);
      for (IMNode child : node.getChildren().values()) {
        if (!Pattern.matches(targetNameRegex, child.getName())) {
          continue;
        }
        traverse(child, idx + 1, level + 1);
      }
      traverseContext.pop();

      if (multiLevelWildcard) {
        traverseContext.push(node);
        for (IMNode child : node.getChildren().values()) {
          traverse(child, idx, level + 1);
        }
        traverseContext.pop();
      }
      return;
    }

    traverseContext.push(node);
    IMNode child;
    IMNodeIterator iterator = store.getChildrenIterator(node);
    try {
      while (iterator.hasNext()) {
        child = iterator.next();
        try {
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
          traverse(child, idx + 1, level + 1);
        } finally {
          store.unPin(child);
        }
      }
    } finally {
      iterator.close();
    }

    traverseContext.pop();

    if (multiLevelWildcard) {
      traverseContext.push(node);
      iterator = store.getChildrenIterator(node);
      try {
        while (iterator.hasNext()) {
          child = iterator.next();
          try {
            traverse(child, idx, level + 1);
          } finally {
            store.unPin(child);
          }
        }
      } finally {
        iterator.close();
      }
      traverseContext.pop();
    }

    if (!shouldTraverseTemplate) {
      return;
    }

    if (!node.isUseTemplate()) {
      return;
    }

    Template upperTemplate = getUpperTemplate(node);

    isInTemplate = true;
    traverseContext.push(node);
    for (IMNode childInTemplate : upperTemplate.getDirectNodes()) {
      if (!Pattern.matches(targetNameRegex, childInTemplate.getName())) {
        continue;
      }
      traverse(childInTemplate, idx + 1, level + 1);
    }
    traverseContext.pop();

    if (multiLevelWildcard) {
      traverseContext.push(node);
      for (IMNode childInTemplate : upperTemplate.getDirectNodes()) {
        traverse(childInTemplate, idx, level + 1);
      }
      traverseContext.pop();
    }
    isInTemplate = false;
  }

  @SuppressWarnings("Duplicates")
  protected void processNameMatch(IMNode node, int idx, int level) throws MetadataException {
    boolean multiLevelWildcard = nodes[idx].equals(MULTI_LEVEL_PATH_WILDCARD);
    String targetName = nodes[idx + 1];

    if (isInTemplate) {
      IMNode targetNode = node.getChild(targetName);
      if (targetNode != null) {
        traverseContext.push(node);
        traverse(targetNode, idx + 1, level + 1);
        traverseContext.pop();
      }

      if (multiLevelWildcard) {
        traverseContext.push(node);
        for (IMNode child : node.getChildren().values()) {
          traverse(child, idx, level + 1);
        }
        traverseContext.pop();
      }
      return;
    }

    IMNode next = store.getChild(node, targetName);
    if (next != null) {
      try {
        traverseContext.push(node);
        traverse(next, idx + 1, level + 1);
        traverseContext.pop();
      } finally {
        store.unPin(next);
      }
    }

    if (multiLevelWildcard) {
      traverseContext.push(node);
      IMNode child;
      IMNodeIterator iterator = store.getChildrenIterator(node);
      try {
        while (iterator.hasNext()) {
          child = iterator.next();
          try {
            traverse(child, idx, level + 1);
          } finally {
            store.unPin(child);
          }
        }
      } finally {
        iterator.close();
      }
      traverseContext.pop();
    }

    if (!shouldTraverseTemplate) {
      return;
    }

    if (!node.isUseTemplate()) {
      return;
    }

    Template upperTemplate = getUpperTemplate(node);
    isInTemplate = true;
    IMNode targetNode = upperTemplate.getDirectNode(targetName);
    if (targetNode != null) {
      traverseContext.push(node);
      traverse(targetNode, idx + 1, level + 1);
      traverseContext.pop();
    }

    if (multiLevelWildcard) {
      traverseContext.push(node);
      for (IMNode child : upperTemplate.getDirectNodes()) {
        traverse(child, idx, level + 1);
      }
      traverseContext.pop();
    }
    isInTemplate = false;
  }

  protected Template getUpperTemplate(IMNode node) {
    Iterator<IMNode> iterator = traverseContext.iterator();
    IMNode ancestor;
    if (templateMap == null) {
      // old standalone
      if (node.getSchemaTemplate() != null) {
        return node.getUpperTemplate();
      }
      while (iterator.hasNext()) {
        ancestor = iterator.next();
        if (ancestor.getSchemaTemplate() != null) {
          return ancestor.getSchemaTemplate();
        }
      }
    } else {
      // new cluster
      if (node.getSchemaTemplateId() != NON_TEMPLATE) {
        return templateMap.get(node.getSchemaTemplateId());
      }
      while (iterator.hasNext()) {
        ancestor = iterator.next();
        if (ancestor.getSchemaTemplateId() != NON_TEMPLATE) {
          return templateMap.get(ancestor.getSchemaTemplateId());
        }
      }
    }
    // if the node is usingTemplate, the upperTemplate won't be null
    return null;
  }

  public void setTemplateMap(Map<Integer, Template> templateMap) {
    this.templateMap = templateMap;
  }

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
  protected PartialPath getCurrentPartialPath(IMNode currentNode) {
    return new PartialPath(getCurrentPathNodes(currentNode));
  }

  protected String[] getCurrentPathNodes(IMNode currentNode) {
    Iterator<IMNode> nodes = traverseContext.descendingIterator();
    List<String> nodeNames = new LinkedList<>();
    if (nodes.hasNext()) {
      nodeNames.addAll(Arrays.asList(nodes.next().getPartialPath().getNodes()));
    }

    while (nodes.hasNext()) {
      nodeNames.add(nodes.next().getName());
    }

    nodeNames.add(currentNode.getName());

    return nodeNames.toArray(new String[0]);
  }

  /** @return the storage group node in the traverse path */
  protected IMNode getStorageGroupNodeInTraversePath(IMNode currentNode) {
    if (currentNode.isStorageGroup()) {
      return currentNode;
    }
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
