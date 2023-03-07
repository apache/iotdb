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
import org.apache.iotdb.db.metadata.mnode.iterator.MNodeIterator;
import org.apache.iotdb.db.metadata.mtree.store.IMTreeStore;
import org.apache.iotdb.db.metadata.mtree.store.ReentrantReadOnlyCachedMTreeStore;
import org.apache.iotdb.db.metadata.template.Template;
import org.apache.iotdb.db.metadata.template.TemplateMNodeGenerator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;

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
public abstract class Traverser<R> extends AbstractTreeVisitor<IMNode, R> {

  private static final Logger logger = LoggerFactory.getLogger(Traverser.class);

  protected IMTreeStore store;

  protected IMNode startNode;
  protected String[] nodes;

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
   * @param store MTree store to traverse
   * @param isPrefixMatch prefix match or not
   * @throws MetadataException path does not meet the expected rules
   */
  protected Traverser(IMNode startNode, PartialPath path, IMTreeStore store, boolean isPrefixMatch)
      throws MetadataException {
    super(startNode, path, isPrefixMatch);
    this.store = store.getWithReentrantReadLock();
    initStack();
    String[] nodes = path.getNodes();
    if (nodes.length == 0 || !nodes[0].equals(PATH_ROOT)) {
      throw new IllegalPathException(
          path.getFullPath(), path.getFullPath() + " doesn't start with " + startNode.getName());
    }
    this.startNode = startNode;
    this.nodes = nodes;
  }

  /**
   * The interface to start the traversal. The node process should be defined before traversal by
   * overriding or implement concerned methods.
   */
  public void traverse() throws MetadataException {
    while (hasNext()) {
      next();
    }
    if (!isSuccess()) {
      Throwable e = getFailure();
      logger.warn(e.getMessage(), e);
      throw new MetadataException(e.getMessage(), e);
    }
  }

  @Override
  protected IMNode getChild(IMNode parent, String childName) throws MetadataException {
    IMNode child = null;
    if (parent.isAboveDatabase()) {
      child = parent.getChild(childName);
    } else {
      if (templateMap != null
          && !templateMap.isEmpty() // this task will cover some timeseries represented by template
          && parent.getSchemaTemplateId() != NON_TEMPLATE // the device is using template
          && !(skipPreDeletedSchema
              && parent
                  .getAsEntityMNode()
                  .isPreDeactivateTemplate())) { // the template should not skip
        Template template = templateMap.get(parent.getSchemaTemplateId());
        // if null, it means the template on this device is not covered in this query, refer to the
        // mpp analyzing stage
        if (template != null) {
          child =
              TemplateMNodeGenerator.getChild(
                  templateMap.get(parent.getSchemaTemplateId()), childName);
        }
      }
    }
    if (child == null) {
      child = store.getChild(parent, childName);
    }
    return child;
  }

  @Override
  protected void releaseNode(IMNode node) {
    if (!node.isAboveDatabase() && !node.isStorageGroup()) {
      // In any case we can call store#inpin directly because the unpin method will not do anything
      // if it is an IMNode in template or in memory mode.
      store.unPin(node);
    }
  }

  @Override
  protected Iterator<IMNode> getChildrenIterator(IMNode parent) throws MetadataException {
    if (parent.isAboveDatabase()) {
      return new MNodeIterator(parent.getChildren().values().iterator());
    } else {
      return store.getTraverserIterator(parent, templateMap, skipPreDeletedSchema);
    }
  }

  @Override
  protected void releaseNodeIterator(Iterator<IMNode> nodeIterator) {
    ((IMNodeIterator) nodeIterator).close();
  }

  @Override
  public void close() {
    super.close();
    if (store instanceof ReentrantReadOnlyCachedMTreeStore) {
      // TODO update here
      ((ReentrantReadOnlyCachedMTreeStore) store).unlockRead();
    }
  }

  public void setTemplateMap(Map<Integer, Template> templateMap) {
    this.templateMap = templateMap;
  }

  public void setSkipPreDeletedSchema(boolean skipPreDeletedSchema) {
    this.skipPreDeletedSchema = skipPreDeletedSchema;
  }
}
