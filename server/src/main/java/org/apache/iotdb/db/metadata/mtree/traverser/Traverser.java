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

import java.util.Iterator;
import java.util.Map;

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
 */
public abstract class Traverser<R> extends AbstractTreeVisitor<IMNode, R> {

  protected IMTreeStore store;

  protected IMNode startNode;
  protected String[] nodes;
  protected int startIndex;
  protected int startLevel;

  // measurement in template should be processed only if templateMap is not null
  protected Map<Integer, Template> templateMap;

  // if true, the pre deleted measurement or pre deactivated template won't be processed
  protected boolean skipPreDeletedSchema = false;

  // default false means fullPath pattern match
  protected boolean isPrefixMatch = false;

  protected Traverser() {}

  /**
   * To traverse subtree under root.sg, e.g., init Traverser(root, "root.sg.**")
   *
   * @param startNode denote which tree to traverse by passing its root
   * @param path use wildcard to specify which part to traverse
   * @throws MetadataException
   */
  protected Traverser(IMNode startNode, PartialPath path, IMTreeStore store, boolean isPrefixMatch)
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
    if (parent.isAboveDatabase()) {
      return parent.getChild(childName);
    } else {
      return store.getChild(parent, childName);
    }
    // TODO: support template
  }

  @Override
  protected void releaseNode(IMNode node) {
    if (!node.isAboveDatabase() && !node.isStorageGroup()) {
      store.unPin(node);
      // TODO: support template
    }
  }

  @Override
  protected Iterator<IMNode> getChildrenIterator(IMNode parent) throws MetadataException {
    if (parent.isAboveDatabase()) {
      return parent.getChildren().values().iterator();
    } else {
      return store.getTraverserIterator(parent, templateMap, skipPreDeletedSchema);
    }
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
}
