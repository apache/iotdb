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
import org.apache.iotdb.commons.path.fa.IFAState;
import org.apache.iotdb.commons.path.fa.IFATransition;
import org.apache.iotdb.commons.schema.node.IMNode;
import org.apache.iotdb.commons.schema.node.utils.IMNodeFactory;
import org.apache.iotdb.commons.schema.node.utils.IMNodeIterator;
import org.apache.iotdb.commons.schema.tree.AbstractTreeVisitor;
import org.apache.iotdb.db.metadata.mnode.mem.iterator.MNodeIterator;
import org.apache.iotdb.db.metadata.mnode.utils.MNodeUtils;
import org.apache.iotdb.db.metadata.mtree.store.IMTreeStore;
import org.apache.iotdb.db.metadata.mtree.store.ReentrantReadOnlyCachedMTreeStore;
import org.apache.iotdb.db.metadata.template.Template;

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
public abstract class Traverser<R, N extends IMNode<N>> extends AbstractTreeVisitor<N, R> {

  private static final Logger logger = LoggerFactory.getLogger(Traverser.class);

  protected IMTreeStore<N> store;

  protected N startNode;
  protected String[] nodes;

  // measurement in template should be processed only if templateMap is not null
  protected Map<Integer, Template> templateMap;
  protected IMNodeFactory<N> nodeFactory;

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
  protected Traverser(N startNode, PartialPath path, IMTreeStore<N> store, boolean isPrefixMatch)
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
  protected N getChild(N parent, String childName) throws MetadataException {
    N child = null;
    if (parent.isAboveDatabase()) {
      child = parent.getChild(childName);
    } else {
      if (templateMap != null
          && !templateMap.isEmpty() // this task will cover some timeseries represented by template
          && (parent.isDevice()
              && parent.getAsDeviceMNode().getSchemaTemplateId()
                  != NON_TEMPLATE) // the device is using template
          && !(skipPreDeletedSchema
              && parent
                  .getAsDeviceMNode()
                  .isPreDeactivateTemplate())) { // the template should not skip
        int templateId = parent.getAsDeviceMNode().getSchemaTemplateId();
        Template template = templateMap.get(templateId);
        // if null, it means the template on this device is not covered in this query, refer to the
        // mpp analyzing stage
        if (template != null && nodeFactory != null) {
          child = MNodeUtils.getChild(templateMap.get(templateId), childName, nodeFactory);
        }
      }
    }
    if (child == null) {
      child = store.getChild(parent, childName);
    }
    return child;
  }

  @Override
  protected void releaseNode(N node) {
    if (!node.isAboveDatabase() && !node.isDatabase()) {
      // In any case we can call store#inpin directly because the unpin method will not do anything
      // if it is an IMNode in template or in memory mode.
      store.unPin(node);
    }
  }

  @Override
  protected Iterator<N> getChildrenIterator(N parent) throws MetadataException {
    if (parent.isAboveDatabase()) {
      return new MNodeIterator<>(parent.getChildren().values().iterator());
    } else {
      return store.getTraverserIterator(parent, templateMap, skipPreDeletedSchema);
    }
  }

  @Override
  protected void releaseNodeIterator(Iterator<N> nodeIterator) {
    ((IMNodeIterator<N>) nodeIterator).close();
  }

  @Override
  public void close() {
    super.close();
    if (store instanceof ReentrantReadOnlyCachedMTreeStore) {
      // TODO update here
      ((ReentrantReadOnlyCachedMTreeStore) store).unlockRead();
    }
  }

  public void setTemplateMap(Map<Integer, Template> templateMap, IMNodeFactory<N> nodeFactory) {
    this.templateMap = templateMap;
    this.nodeFactory = nodeFactory;
  }

  public void setSkipPreDeletedSchema(boolean skipPreDeletedSchema) {
    this.skipPreDeletedSchema = skipPreDeletedSchema;
  }

  @Override
  protected IFAState tryGetNextState(
      N node, IFAState sourceState, Map<String, IFATransition> preciseMatchTransitionMap) {
    IFATransition transition;
    IFAState state;
    if (node.isMeasurement()) {
      String alias = node.getAsMeasurementMNode().getAlias();
      if (alias != null) {
        transition = preciseMatchTransitionMap.get(alias);
        if (transition != null) {
          state = patternFA.getNextState(sourceState, transition);
          if (state.isFinal()) {
            return state;
          }
        }
      }
      transition = preciseMatchTransitionMap.get(node.getName());
      if (transition != null) {
        state = patternFA.getNextState(sourceState, transition);
        if (state.isFinal()) {
          return state;
        }
      }
      return null;
    }

    transition = preciseMatchTransitionMap.get(node.getName());
    if (transition == null) {
      return null;
    }
    return patternFA.getNextState(sourceState, transition);
  }

  @Override
  protected IFAState tryGetNextState(N node, IFAState sourceState, IFATransition transition) {
    IFAState state;
    if (node.isMeasurement()) {
      String alias = node.getAsMeasurementMNode().getAlias();
      if (alias != null && transition.isMatch(alias)) {
        state = patternFA.getNextState(sourceState, transition);
        if (state.isFinal()) {
          return state;
        }
      }
      if (transition.isMatch(node.getName())) {
        state = patternFA.getNextState(sourceState, transition);
        if (state.isFinal()) {
          return state;
        }
      }
      return null;
    }

    if (transition.isMatch(node.getName())) {
      return patternFA.getNextState(sourceState, transition);
    }
    return null;
  }
}
