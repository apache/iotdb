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

package org.apache.iotdb.db.schemaengine.schemaregion.mtree.traverser;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.commons.path.fa.IFAState;
import org.apache.iotdb.commons.path.fa.IFATransition;
import org.apache.iotdb.commons.schema.node.IMNode;
import org.apache.iotdb.commons.schema.node.role.IDeviceMNode;
import org.apache.iotdb.commons.schema.node.utils.IMNodeFactory;
import org.apache.iotdb.commons.schema.node.utils.IMNodeIterator;
import org.apache.iotdb.commons.schema.tree.AbstractTreeVisitor;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.IMTreeStore;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.mem.mnode.iterator.MNodeIterator;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.ReentrantReadOnlyCachedMTreeStore;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.memory.ReleaseFlushMonitor;
import org.apache.iotdb.db.schemaengine.schemaregion.utils.MNodeUtils;
import org.apache.iotdb.db.schemaengine.template.Template;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;

import static org.apache.iotdb.commons.conf.IoTDBConstant.PATH_ROOT;
import static org.apache.iotdb.commons.schema.SchemaConstant.NON_TEMPLATE;

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

  // Measurement in template should be processed only if templateMap is not null
  protected Map<Integer, Template> templateMap;
  protected IMNodeFactory<N> nodeFactory;

  // If true, the pre deleted measurement or pre deactivated template won't be processed
  protected boolean skipPreDeletedSchema = false;

  // Default false means fullPath pattern match
  protected boolean isPrefixMatch = false;
  private IDeviceMNode<N> skipTemplateDevice;
  private ReleaseFlushMonitor.RecordNode timeRecorder;
  private final long startTime = System.currentTimeMillis();

  protected Traverser() {}

  /**
   * To traverse subtree under root.sg, e.g., init Traverser(root, "root.sg.**")
   *
   * @param startNode denote which tree to traverse by passing its root
   * @param path use wildcard to specify which part to traverse
   * @param store MTree store to traverse
   * @param isPrefixMatch prefix match or not
   * @param scope traversing scope
   * @throws MetadataException path does not meet the expected rules
   */
  protected Traverser(
      N startNode,
      PartialPath path,
      IMTreeStore<N> store,
      boolean isPrefixMatch,
      PathPatternTree scope)
      throws MetadataException {
    super(startNode, path, isPrefixMatch, scope);
    this.store = store.getWithReentrantReadLock();
    initStack();
    String[] nodes = path.getNodes();
    if (nodes.length == 0 || !nodes[0].equals(PATH_ROOT)) {
      throw new IllegalPathException(
          path.getFullPath(), path.getFullPath() + " doesn't start with " + startNode.getName());
    }
    this.startNode = startNode;
    this.nodes = nodes;
    this.timeRecorder = store.recordTraverserStatistics();
  }

  /**
   * To traverse subtree under startNode.
   *
   * @param startNode denote which tree to traverse by passing its root
   * @param fullPathTree must not contain any wildcard
   * @param store MTree store to traverse
   * @param scope traversing scope
   */
  protected Traverser(
      N startNode, PathPatternTree fullPathTree, IMTreeStore<N> store, PathPatternTree scope) {
    super(startNode, fullPathTree, scope);
    this.store = store.getWithReentrantReadLock();
    initStack();
    this.startNode = startNode;
    this.timeRecorder = store.recordTraverserStatistics();
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
    return getChild(parent, childName, parent == skipTemplateDevice);
  }

  private N getChild(N parent, String childName, boolean skipTemplateChildren)
      throws MetadataException {
    N child = null;
    if (parent.isAboveDatabase()) {
      child = parent.getChild(childName);
    } else {
      if (templateMap != null
          && !skipTemplateChildren
          && !templateMap.isEmpty() // This task will cover some timeseries represented by template
          && (parent.isDevice()
              && parent.getAsDeviceMNode().getSchemaTemplateId()
                  != NON_TEMPLATE) // The device is using template
          && !(skipPreDeletedSchema
              && parent
                  .getAsDeviceMNode()
                  .isPreDeactivateTemplate())) { // the template should not skip
        int templateId = parent.getAsDeviceMNode().getSchemaTemplateId();
        Template template = templateMap.get(templateId);
        // If null, it means the template on this device is not covered in this query, refer to the
        // mpp analyzing stage
        if (template != null && nodeFactory != null) {
          child = MNodeUtils.getChild(templateMap.get(templateId), childName, nodeFactory);
        }
      }
    }
    if (child == null) {
      child = store.getChild(parent, childName);
      if (Objects.nonNull(child)
          && skipPreDeletedSchema
          && child.isMeasurement()
          && child.getAsMeasurementMNode().isPreDeleted()) {
        child = null;
      }
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
  protected Iterator<N> getChildrenIterator(N parent, Iterator<String> childrenName)
      throws Exception {

    return new IMNodeIterator<N>() {
      private N next = null;
      private boolean skipTemplateChildren = false;

      @Override
      public boolean hasNext() {
        if (next == null) {
          while (next == null && childrenName.hasNext()) {
            try {
              next = getChild(parent, childrenName.next(), skipTemplateChildren);
            } catch (Throwable e) {
              logger.warn(e.getMessage(), e);
              throw new RuntimeException(e);
            }
          }
        }
        return next != null;
      }

      @Override
      public N next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        N result = next;
        next = null;
        return result;
      }

      @Override
      public void skipTemplateChildren() {
        skipTemplateChildren = true;
      }

      @Override
      public void close() {
        if (next != null) {
          releaseNode(next);
          next = null;
        }
      }
    };
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
    if (nodeIterator instanceof IMNodeIterator) {
      ((IMNodeIterator<N>) nodeIterator).close();
    }
  }

  @Override
  public void close() {
    super.close();
    if (store instanceof ReentrantReadOnlyCachedMTreeStore) {
      ((ReentrantReadOnlyCachedMTreeStore) store).unlockRead();
    }
    long endTime = System.currentTimeMillis();
    if (timeRecorder != null) {
      timeRecorder.setEndTime(endTime);
    }
    store.recordTraverserMetric(endTime - startTime);
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

  protected void skipTemplateChildren(IDeviceMNode<N> deviceMNode) {
    skipTemplateDevice = deviceMNode;
    Iterator<N> iterator = getCurrentChildrenIterator();
    if (iterator instanceof IMNodeIterator) {
      ((IMNodeIterator<N>) iterator).skipTemplateChildren();
    }
  }
}
