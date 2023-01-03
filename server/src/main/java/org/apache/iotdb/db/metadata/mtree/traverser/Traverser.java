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
import org.apache.iotdb.db.metadata.mtree.store.IMTreeStore;
import org.apache.iotdb.db.metadata.template.Template;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.Map;

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
public abstract class Traverser extends AbstractTreeVisitor<IMNode, IMNode> {

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

  // if true, the pre deleted measurement or pre deactivated template won't be processed
  protected boolean skipPreDeletedSchema = false;

  // default false means fullPath pattern match
  protected boolean isPrefixMatch = false;

  /**
   * To traverse subtree under root.sg, e.g., init Traverser(root, "root.sg.**")
   *
   * @param startNode denote which tree to traverse by passing its root
   * @param path use wildcard to specify which part to traverse
   * @throws MetadataException
   */
  public Traverser(IMNode startNode, PartialPath path, IMTreeStore store, boolean isPrefixMatch)
      throws MetadataException {
    super(startNode, path, isPrefixMatch);
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
  protected IMNode getChild(IMNode parent, String childName) {
    try {
      return store.getChild(parent, childName);
    } catch (MetadataException e) {
      e.printStackTrace();
      return null;
    }
    // TODO: support template
  }

  @Override
  protected Iterator<IMNode> getChildrenIterator(IMNode parent) {
    try {
      return store.getChildrenIterator(parent);
    } catch (MetadataException e) {
      e.printStackTrace();
      return null;
    }
    // TODO: support template
  }

  @Override
  protected IMNode generateResult() {
    return nextMatchedNode;
  }

  protected Template getActivatedSchemaTemplate(IMNode node) {
    // new cluster, the used template is directly recorded as template id in device mnode
    if (node.getSchemaTemplateId() != NON_TEMPLATE) {
      if (skipPreDeletedSchema && node.getAsEntityMNode().isPreDeactivateTemplate()) {
        // skip this pre deactivated template, the invoker will skip this
        return null;
      }
      return templateMap.get(node.getSchemaTemplateId());
    }
    // if the node is usingTemplate, the upperTemplate won't be null or the upperTemplateId won't be
    // NON_TEMPLATE.
    throw new IllegalStateException(
        String.format(
            "There should be a template mounted on any ancestor of the node [%s] usingTemplate.",
            node.getFullPath()));
  }

  public void setTemplateMap(Map<Integer, Template> templateMap) {
    this.templateMap = templateMap;
  }

  public void setShouldTraverseTemplate(boolean shouldTraverseTemplate) {
    this.shouldTraverseTemplate = shouldTraverseTemplate;
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
