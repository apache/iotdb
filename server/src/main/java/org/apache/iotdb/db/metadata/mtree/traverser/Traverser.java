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
import org.apache.iotdb.commons.schema.tree.AbstractTreeVisitor;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mnode.iterator.IMNodeIterator;
import org.apache.iotdb.db.metadata.mtree.store.IMTreeStore;
import org.apache.iotdb.db.metadata.template.Template;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.apache.iotdb.commons.conf.IoTDBConstant.MULTI_LEVEL_PATH_WILDCARD;
import static org.apache.iotdb.commons.conf.IoTDBConstant.ONE_LEVEL_PATH_WILDCARD;
import static org.apache.iotdb.commons.conf.IoTDBConstant.PATH_ROOT;

/**
 * This class defines the main traversal framework and declares some methods for result process
 * extension. This class could be extended to implement concrete tasks. <br>
 * Currently, the tasks are classified into two type:
 *
 * <ol>
 *   <li>counter: to count the node num or measurement num that matches the path pattern
 *   <li>collector: to collect customized results of the matched node or measurement
 * </ol>
 *
 * root.sg12.sg1 sg1 root->sg12->
 *
 * <p>root.sg22.sg1
 */
public abstract class Traverser<R> extends AbstractTreeVisitor<IMNode, R> {

  protected IMTreeStore store;

  protected IMNode startNode;
  protected String[] nodes;
  protected int startIndex;
  protected int startLevel;
  protected boolean isPrefixStart = false;

  // to construct full path or find mounted node on MTree when traverse into template
  protected Deque<IMNode> traverseContext;

  // measurement in template should be processed only if templateMap is not null
  protected Map<Integer, Template> templateMap;

  // if true, the pre deleted measurement or pre deactivated template won't be processed
  protected boolean skipPreDeletedSchema = false;

  // default false means fullPath pattern match
  protected boolean isPrefixMatch = false;

  private List<IMNode> aboveSGNodes = new ArrayList<>();

  public Traverser() {}

  /**
   * To traverse subtree under root.sg, e.g., init Traverser(root, "root.sg.**")
   *
   * @param startNode denote which tree to traverse by passing its root
   * @param path use wildcard to specify which part to traverse
   * @throws MetadataException
   */
  public Traverser(IMNode startNode, PartialPath path, IMTreeStore store, boolean isPrefixMatch)
      throws MetadataException {
    super(path, isPrefixMatch);
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
    IMNode node = startNode.getParent();
    while (node != null) {
      aboveSGNodes.add(node);
      node = node.getParent();
    }
    initStack(aboveSGNodes.get(aboveSGNodes.size() - 1));
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
    while (hasNext()) {
      next();
    }
    // TODO: 临时在这里判断失败，释放资源

    //    if (isPrefixStart && !isPrefixMatch) {
    //      return;
    //    }
    //    traverse(startNode, startIndex, startLevel);
  }

  @Override
  protected IMNode getChild(IMNode parent, String childName) throws MetadataException {
    if (aboveSGNodes.contains(parent)) {
      int index = aboveSGNodes.indexOf(parent);
      if (index == 0) {
        return startNode;
      } else {
        return aboveSGNodes.get(index - 1);
      }
    } else {
      return store.getChild(parent, childName);
    }
    // TODO: support template
  }

  @Override
  protected void releaseNode(IMNode node) {
    store.unPin(node);
  }

  @Override
  protected Iterator<IMNode> getChildrenIterator(IMNode parent) throws MetadataException {
    if (aboveSGNodes.contains(parent)) {
      int index = aboveSGNodes.indexOf(parent);
      if (index == 0) {
        return Collections.singletonList(startNode).iterator();
      } else {
        return Collections.singletonList(aboveSGNodes.get(index - 1)).iterator();
      }
    }
    return store.getTraverserIterator(parent, templateMap, skipPreDeletedSchema);
  }

  @Override
  protected void releaseNodeIterator(Iterator<IMNode> nodeIterator) {
    ((IMNodeIterator) nodeIterator).close();
  }

  public void setTemplateMap(Map<Integer, Template> templateMap) {
    this.templateMap = templateMap;
  }

  public void setSkipPreDeletedSchema(boolean skipPreDeletedSchema) {
    this.skipPreDeletedSchema = skipPreDeletedSchema;
  }

  /** @return full path from traverse start node to the current node */
  protected PartialPath getCurrentPartialPath() {
    return new PartialPath(generateFullPathNodes());
  }

  /** @return the database node in the traverse path */
  protected IMNode getStorageGroupNodeInTraversePath(IMNode currentNode) {
    if (currentNode.isStorageGroup()) {
      return currentNode;
    }
    for (IMNode node : traverseContext) {
      if (node.isStorageGroup()) {
        return node;
      }
    }
    return null;
  }
}
