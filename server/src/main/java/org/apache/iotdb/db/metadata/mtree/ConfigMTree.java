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

package org.apache.iotdb.db.metadata.mtree;

import org.apache.iotdb.common.rpc.thrift.TSchemaNode;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.utils.ThriftConfigNodeSerDeUtils;
import org.apache.iotdb.db.exception.metadata.PathNotExistException;
import org.apache.iotdb.db.exception.metadata.StorageGroupAlreadySetException;
import org.apache.iotdb.db.exception.metadata.StorageGroupNotSetException;
import org.apache.iotdb.db.metadata.LocalSchemaProcessor;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mnode.IStorageGroupMNode;
import org.apache.iotdb.db.metadata.mnode.InternalMNode;
import org.apache.iotdb.db.metadata.mnode.StorageGroupMNode;
import org.apache.iotdb.db.metadata.mnode.iterator.IMNodeIterator;
import org.apache.iotdb.db.metadata.mtree.store.MemMTreeStore;
import org.apache.iotdb.db.metadata.mtree.traverser.collector.CollectorTraverser;
import org.apache.iotdb.db.metadata.mtree.traverser.collector.MNodeAboveSGCollector;
import org.apache.iotdb.db.metadata.mtree.traverser.collector.StorageGroupCollector;
import org.apache.iotdb.db.metadata.mtree.traverser.counter.CounterTraverser;
import org.apache.iotdb.db.metadata.mtree.traverser.counter.StorageGroupCounter;
import org.apache.iotdb.db.metadata.utils.MetaFormatUtils;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.TreeSet;

import static org.apache.iotdb.commons.conf.IoTDBConstant.ONE_LEVEL_PATH_WILDCARD;
import static org.apache.iotdb.commons.conf.IoTDBConstant.PATH_ROOT;
import static org.apache.iotdb.db.metadata.MetadataConstant.ALL_RESULT_NODES;
import static org.apache.iotdb.db.metadata.MetadataConstant.ALL_TEMPLATE;
import static org.apache.iotdb.db.metadata.MetadataConstant.INTERNAL_MNODE_TYPE;
import static org.apache.iotdb.db.metadata.MetadataConstant.NON_TEMPLATE;
import static org.apache.iotdb.db.metadata.MetadataConstant.STORAGE_GROUP_MNODE_TYPE;

// Since the ConfigMTree is all stored in memory, thus it is not restricted to manage MNode through
// MTreeStore.
public class ConfigMTree {

  private final Logger logger = LoggerFactory.getLogger(ConfigMTree.class);

  private IMNode root;
  // this store is only used for traverser invoking
  private MemMTreeStore store;

  public ConfigMTree() throws MetadataException {
    store = new MemMTreeStore(new PartialPath(PATH_ROOT), false);
    root = store.getRoot();
  }

  public void clear() {
    if (store != null) {
      store.clear();
      this.root = store.getRoot();
    }
  }

  // region Storage Group Management

  /**
   * Set storage group. Make sure check seriesPath before setting storage group
   *
   * @param path path
   */
  public void setStorageGroup(PartialPath path) throws MetadataException {
    String[] nodeNames = path.getNodes();
    MetaFormatUtils.checkStorageGroup(path.getFullPath());
    if (nodeNames.length <= 1 || !nodeNames[0].equals(root.getName())) {
      throw new IllegalPathException(path.getFullPath());
    }
    IMNode cur = root;
    int i = 1;
    // e.g., path = root.a.b.sg, create internal nodes for a, b
    while (i < nodeNames.length - 1) {
      IMNode temp = cur.getChild(nodeNames[i]);
      if (temp == null) {
        cur.addChild(nodeNames[i], new InternalMNode(cur, nodeNames[i]));
      } else if (temp.isStorageGroup()) {
        // before set storage group, check whether the storage group already exists
        throw new StorageGroupAlreadySetException(temp.getFullPath());
      }
      cur = cur.getChild(nodeNames[i]);
      i++;
    }

    // synchronize check and add, we need addChild operation be atomic.
    // only write operations on mtree will be synchronized
    synchronized (this) {
      if (cur.hasChild(nodeNames[i])) {
        // node b has child sg
        if (cur.getChild(nodeNames[i]).isStorageGroup()) {
          throw new StorageGroupAlreadySetException(path.getFullPath());
        } else {
          throw new StorageGroupAlreadySetException(path.getFullPath(), true);
        }
      } else {
        IStorageGroupMNode storageGroupMNode =
            new StorageGroupMNode(
                cur, nodeNames[i], CommonDescriptor.getInstance().getConfig().getDefaultTTLInMs());

        IMNode result = cur.addChild(nodeNames[i], storageGroupMNode);

        if (result != storageGroupMNode) {
          throw new StorageGroupAlreadySetException(path.getFullPath(), true);
        }
      }
    }
  }

  /** Delete a storage group */
  public void deleteStorageGroup(PartialPath path) throws MetadataException {
    IStorageGroupMNode storageGroupMNode = getStorageGroupNodeByStorageGroupPath(path);
    IMNode cur = storageGroupMNode.getParent();
    // Suppose current system has root.a.b.sg1, root.a.sg2, and delete root.a.b.sg1
    // delete the storage group node sg1
    cur.deleteChild(storageGroupMNode.getName());

    // delete node a while retain root.a.sg2
    while (cur.getParent() != null && cur.getChildren().size() == 0) {
      cur.getParent().deleteChild(cur.getName());
      cur = cur.getParent();
    }
  }

  /**
   * Check whether path is storage group or not
   *
   * <p>e.g., path = root.a.b.sg. if nor a and b is StorageGroupMNode and sg is a StorageGroupMNode
   * path is a storage group
   *
   * @param path path
   * @apiNote :for cluster
   */
  public boolean isStorageGroup(PartialPath path) {
    String[] nodeNames = path.getNodes();
    if (nodeNames.length <= 1 || !nodeNames[0].equals(IoTDBConstant.PATH_ROOT)) {
      return false;
    }
    IMNode cur = root;
    int i = 1;
    while (i < nodeNames.length - 1) {
      cur = cur.getChild(nodeNames[i]);
      if (cur == null || cur.isStorageGroup()) {
        return false;
      }
      i++;
    }
    cur = cur.getChild(nodeNames[i]);
    return cur != null && cur.isStorageGroup();
  }

  /** Check whether the given path contains a storage group */
  public boolean checkStorageGroupByPath(PartialPath path) {
    String[] nodes = path.getNodes();
    IMNode cur = root;
    for (int i = 1; i < nodes.length; i++) {
      cur = cur.getChild(nodes[i]);
      if (cur == null) {
        return false;
      } else if (cur.isStorageGroup()) {
        return true;
      }
    }
    return false;
  }

  /**
   * Get storage group path by path
   *
   * <p>e.g., root.sg1 is storage group, path is root.sg1.d1, return root.sg1
   *
   * @return storage group in the given path
   */
  public PartialPath getBelongedStorageGroup(PartialPath path) throws StorageGroupNotSetException {
    String[] nodes = path.getNodes();
    IMNode cur = root;
    for (int i = 1; i < nodes.length; i++) {
      cur = cur.getChild(nodes[i]);
      if (cur == null) {
        throw new StorageGroupNotSetException(path.getFullPath());
      } else if (cur.isStorageGroup()) {
        return cur.getPartialPath();
      }
    }
    throw new StorageGroupNotSetException(path.getFullPath());
  }

  /**
   * Get the storage group that given path pattern matches or belongs to.
   *
   * <p>Suppose we have (root.sg1.d1.s1, root.sg2.d2.s2), refer the following cases: 1. given path
   * "root.sg1", ("root.sg1") will be returned. 2. given path "root.*", ("root.sg1", "root.sg2")
   * will be returned. 3. given path "root.*.d1.s1", ("root.sg1", "root.sg2") will be returned.
   *
   * @param pathPattern a path pattern or a full path
   * @return a list contains all storage groups related to given path
   */
  public List<PartialPath> getBelongedStorageGroups(PartialPath pathPattern)
      throws MetadataException {
    return collectStorageGroups(pathPattern, false, true);
  }

  /**
   * Get all storage group that the given path pattern matches. If using prefix match, the path
   * pattern is used to match prefix path. All timeseries start with the matched prefix path will be
   * collected.
   *
   * @param pathPattern a path pattern or a full path
   * @param isPrefixMatch if true, the path pattern is used to match prefix path
   * @return a list contains all storage group names under given path pattern
   */
  public List<PartialPath> getMatchedStorageGroups(PartialPath pathPattern, boolean isPrefixMatch)
      throws MetadataException {
    return collectStorageGroups(pathPattern, isPrefixMatch, false);
  }

  private List<PartialPath> collectStorageGroups(
      PartialPath pathPattern, boolean isPrefixMatch, boolean collectInternal)
      throws MetadataException {
    List<PartialPath> result = new LinkedList<>();
    StorageGroupCollector<List<PartialPath>> collector =
        new StorageGroupCollector<List<PartialPath>>(root, pathPattern, store) {
          @Override
          protected void collectStorageGroup(IStorageGroupMNode node) {
            result.add(node.getPartialPath());
          }
        };
    collector.setCollectInternal(collectInternal);
    collector.setPrefixMatch(isPrefixMatch);
    collector.traverse();
    return result;
  }

  /**
   * Get all storage group names
   *
   * @return a list contains all distinct storage groups
   */
  public List<PartialPath> getAllStorageGroupPaths() {
    List<PartialPath> res = new ArrayList<>();
    Deque<IMNode> nodeStack = new ArrayDeque<>();
    nodeStack.add(root);
    while (!nodeStack.isEmpty()) {
      IMNode current = nodeStack.pop();
      if (current.isStorageGroup()) {
        res.add(current.getPartialPath());
      } else {
        nodeStack.addAll(current.getChildren().values());
      }
    }
    return res;
  }

  /**
   * Resolve the path or path pattern into StorageGroupName-FullPath pairs. Try determining the
   * storage group using the children of a mNode. If one child is a storage group node, put a
   * storageGroupName-fullPath pair into paths.
   */
  public Map<String, List<PartialPath>> groupPathByStorageGroup(PartialPath path)
      throws MetadataException {
    Map<String, List<PartialPath>> result = new HashMap<>();
    StorageGroupCollector<Map<String, String>> collector =
        new StorageGroupCollector<Map<String, String>>(root, path, store) {
          @Override
          protected void collectStorageGroup(IStorageGroupMNode node) {
            PartialPath sgPath = node.getPartialPath();
            result.put(sgPath.getFullPath(), path.alterPrefixPath(sgPath));
          }
        };
    collector.setCollectInternal(true);
    collector.traverse();
    return result;
  }

  /**
   * Get the count of storage group matching the given path. If using prefix match, the path pattern
   * is used to match prefix path. All timeseries start with the matched prefix path will be
   * counted.
   *
   * @param pathPattern a path pattern or a full path, may contain wildcard.
   * @param isPrefixMatch if true, the path pattern is used to match prefix path
   */
  public int getStorageGroupNum(PartialPath pathPattern, boolean isPrefixMatch)
      throws MetadataException {
    CounterTraverser counter = new StorageGroupCounter(root, pathPattern, store);
    counter.setPrefixMatch(isPrefixMatch);
    counter.traverse();
    return counter.getCount();
  }

  /**
   * E.g., root.sg is storage group given [root, sg], if the give path is not a storage group, throw
   * exception
   */
  public IStorageGroupMNode getStorageGroupNodeByStorageGroupPath(PartialPath storageGroupPath)
      throws MetadataException {
    String[] nodes = storageGroupPath.getNodes();
    if (nodes.length == 0 || !nodes[0].equals(root.getName())) {
      throw new IllegalPathException(storageGroupPath.getFullPath());
    }
    IMNode cur = root;
    for (int i = 1; i < nodes.length - 1; i++) {
      cur = cur.getChild(nodes[i]);
      if (cur == null) {
        throw new StorageGroupNotSetException(storageGroupPath.getFullPath());
      }
      if (cur.isStorageGroup()) {
        throw new StorageGroupAlreadySetException(cur.getFullPath());
      }
    }

    cur = cur.getChild(nodes[nodes.length - 1]);
    if (cur == null) {
      throw new StorageGroupNotSetException(storageGroupPath.getFullPath());
    }
    if (cur.isStorageGroup()) {
      return cur.getAsStorageGroupMNode();
    } else {
      throw new StorageGroupAlreadySetException(storageGroupPath.getFullPath(), true);
    }
  }

  /**
   * E.g., root.sg is storage group given [root, sg], return the MNode of root.sg given [root, sg,
   * device], return the MNode of root.sg Get storage group node, the give path don't need to be
   * storage group path.
   */
  public IStorageGroupMNode getStorageGroupNodeByPath(PartialPath path) throws MetadataException {
    String[] nodes = path.getNodes();
    if (nodes.length == 0 || !nodes[0].equals(root.getName())) {
      throw new IllegalPathException(path.getFullPath());
    }
    IMNode cur = root;
    for (int i = 1; i < nodes.length; i++) {
      cur = cur.getChild(nodes[i]);
      if (cur == null) {
        break;
      }
      if (cur.isStorageGroup()) {
        return cur.getAsStorageGroupMNode();
      }
    }
    throw new StorageGroupNotSetException(path.getFullPath());
  }

  public List<PartialPath> getInvolvedStorageGroupNodes(
      PartialPath pathPattern, boolean isPrefixMatch) throws MetadataException {
    List<PartialPath> result = new ArrayList<>();
    StorageGroupCollector<List<PartialPath>> collector =
        new StorageGroupCollector<List<PartialPath>>(root, pathPattern, store) {
          @Override
          protected void collectStorageGroup(IStorageGroupMNode node) {
            result.add(node.getPartialPath());
          }
        };
    collector.setCollectInternal(true);
    collector.setPrefixMatch(isPrefixMatch);
    collector.traverse();
    return result;
  }

  /** Get all storage group MNodes */
  public List<IStorageGroupMNode> getAllStorageGroupNodes() {
    List<IStorageGroupMNode> ret = new ArrayList<>();
    Deque<IMNode> nodeStack = new ArrayDeque<>();
    nodeStack.add(root);
    while (!nodeStack.isEmpty()) {
      IMNode current = nodeStack.pop();
      if (current.isStorageGroup()) {
        ret.add(current.getAsStorageGroupMNode());
      } else {
        nodeStack.addAll(current.getChildren().values());
      }
    }
    return ret;
  }

  /**
   * Check whether the storage group of given path exists. The given path may be a prefix path of
   * existing storage group.
   *
   * @param path a full path or a prefix path
   */
  public boolean isStorageGroupAlreadySet(PartialPath path) {
    String[] nodeNames = path.getNodes();
    IMNode cur = root;
    if (!nodeNames[0].equals(root.getName())) {
      return false;
    }
    for (int i = 1; i < nodeNames.length; i++) {
      if (!cur.hasChild(nodeNames[i])) {
        return false;
      }
      cur = cur.getChild(nodeNames[i]);
      if (cur.isStorageGroup()) {
        return true;
      }
    }
    return true;
  }

  /**
   * Check whether the storage group of given path exists. The given path may be a prefix path of
   * existing storage group. if exists will throw MetaException.
   *
   * @param path a full path or a prefix path
   */
  public void checkStorageGroupAlreadySet(PartialPath path) throws StorageGroupAlreadySetException {
    String[] nodeNames = path.getNodes();
    IMNode cur = root;
    if (!nodeNames[0].equals(root.getName())) {
      return;
    }
    for (int i = 1; i < nodeNames.length; i++) {
      if (!cur.hasChild(nodeNames[i])) {
        return;
      }
      cur = cur.getChild(nodeNames[i]);
      if (cur.isStorageGroup()) {
        throw new StorageGroupAlreadySetException(cur.getFullPath());
      }
    }
    throw new StorageGroupAlreadySetException(path.getFullPath(), true);
  }

  // endregion

  // region MTree Node Management

  public IMNode getNodeWithAutoCreate(PartialPath path) throws StorageGroupNotSetException {
    String[] nodeNames = path.getNodes();
    IMNode cur = root;
    IMNode child;
    boolean hasStorageGroup = false;
    for (int i = 1; i < nodeNames.length; i++) {
      child = cur.getChild(nodeNames[i]);
      if (child == null) {
        if (hasStorageGroup) {
          child = cur.addChild(nodeNames[i], new InternalMNode(cur, nodeNames[i]));
        } else {
          throw new StorageGroupNotSetException(path.getFullPath());
        }
      } else if (child.isStorageGroup()) {
        hasStorageGroup = true;
      }

      cur = child;
    }
    return cur;
  }

  /**
   * Get all paths of nodes in the given level matching the given path. If using prefix match, the
   * path pattern is used to match prefix path.
   */
  public Pair<List<PartialPath>, Set<PartialPath>> getNodesListInGivenLevel(
      PartialPath pathPattern,
      int nodeLevel,
      boolean isPrefixMatch,
      LocalSchemaProcessor.StorageGroupFilter filter)
      throws MetadataException {
    MNodeAboveSGCollector<List<PartialPath>> collector =
        new MNodeAboveSGCollector<List<PartialPath>>(root, pathPattern, store) {
          @Override
          protected void transferToResult(IMNode node) {
            resultSet.add(getCurrentPartialPath(node));
          }
        };
    collector.setResultSet(new LinkedList<>());
    collector.setTargetLevel(nodeLevel);
    collector.setPrefixMatch(isPrefixMatch);
    collector.setStorageGroupFilter(filter);
    collector.traverse();

    return new Pair<>(collector.getResult(), collector.getInvolvedStorageGroupMNodes());
  }

  /**
   * Get child node path in the next level of the given path pattern. This method only count in
   * nodes above storage group. Nodes below storage group, including storage group node will be
   * counted by certain MTreeBelowSG.
   *
   * <p>give pathPattern and the child nodes is those matching pathPattern.*
   *
   * <p>e.g., MTree has [root.a.sg1.d1.s1, root.b.sg1.d1.s2, root.c.sg1.d2.s1] given path = root
   * return [root.a, root.b]
   *
   * @param pathPattern The given path
   * @return All child nodes' seriesPath(s) of given seriesPath.
   */
  public Pair<Set<TSchemaNode>, Set<PartialPath>> getChildNodePathInNextLevel(
      PartialPath pathPattern) throws MetadataException {
    try {
      MNodeAboveSGCollector<Set<TSchemaNode>> collector =
          new MNodeAboveSGCollector<Set<TSchemaNode>>(
              root, pathPattern.concatNode(ONE_LEVEL_PATH_WILDCARD), store) {
            @Override
            protected void transferToResult(IMNode node) {
              resultSet.add(
                  new TSchemaNode(
                      getCurrentPartialPath(node).getFullPath(),
                      node.getMNodeType(true).getNodeType()));
            }
          };
      collector.setResultSet(new TreeSet<>());
      collector.traverse();

      return new Pair<>(collector.getResult(), collector.getInvolvedStorageGroupMNodes());
    } catch (IllegalPathException e) {
      throw new IllegalPathException(pathPattern.getFullPath());
    }
  }

  /**
   * Get child node path in the next level of the given path pattern. This method only count in
   * nodes above storage group. Nodes below storage group, including storage group node will be
   * counted by certain MTreeBelowSG.
   *
   * <p>give pathPattern and the child nodes is those matching pathPattern.*
   *
   * <p>e.g., MTree has [root.a.sg1.d1.s1, root.b.sg1.d1.s2, root.c.sg1.d2.s1] given path = root
   * return [a, b]
   *
   * @param pathPattern The given path
   * @return All child nodes' seriesPath(s) of given seriesPath.
   */
  public Pair<Set<String>, Set<PartialPath>> getChildNodeNameInNextLevel(PartialPath pathPattern)
      throws MetadataException {
    try {
      MNodeAboveSGCollector<Set<String>> collector =
          new MNodeAboveSGCollector<Set<String>>(
              root, pathPattern.concatNode(ONE_LEVEL_PATH_WILDCARD), store) {
            @Override
            protected void transferToResult(IMNode node) {
              resultSet.add(node.getName());
            }
          };
      collector.setResultSet(new TreeSet<>());
      collector.traverse();

      return new Pair<>(collector.getResult(), collector.getInvolvedStorageGroupMNodes());
    } catch (IllegalPathException e) {
      throw new IllegalPathException(pathPattern.getFullPath());
    }
  }

  // endregion

  // region Template Management

  /**
   * check whether there is template on given path and the subTree has template return true,
   * otherwise false
   */
  public void checkTemplateOnPath(PartialPath path) throws MetadataException {
    String[] nodeNames = path.getNodes();
    IMNode cur = root;
    IMNode child;

    if (cur.getSchemaTemplateId() != NON_TEMPLATE) {
      throw new MetadataException("Template already exists on " + cur.getFullPath());
    }

    for (int i = 1; i < nodeNames.length; i++) {
      child = cur.getChild(nodeNames[i]);
      if (child == null) {
        return;
      }
      cur = child;
      if (cur.getSchemaTemplateId() != NON_TEMPLATE) {
        throw new MetadataException("Template already exists on " + cur.getFullPath());
      }
      if (cur.isMeasurement()) {
        return;
      }
    }

    checkTemplateOnSubtree(cur);
  }

  // traverse  all the  descendant of the given path node
  private void checkTemplateOnSubtree(IMNode node) throws MetadataException {
    if (node.isMeasurement()) {
      return;
    }
    IMNode child;
    IMNodeIterator iterator = store.getChildrenIterator(node);
    while (iterator.hasNext()) {
      child = iterator.next();

      if (child.isMeasurement()) {
        continue;
      }
      if (child.getSchemaTemplateId() != NON_TEMPLATE) {
        throw new MetadataException("Template already exists on " + child.getFullPath());
      }
      checkTemplateOnSubtree(child);
    }
  }

  public List<String> getPathsSetOnTemplate(int templateId, boolean filterPreUnset)
      throws MetadataException {
    List<String> resSet = new ArrayList<>();
    CollectorTraverser<Set<String>> setTemplatePaths =
        new CollectorTraverser<Set<String>>(root, new PartialPath(ALL_RESULT_NODES), store) {
          @Override
          protected boolean processInternalMatchedMNode(IMNode node, int idx, int level) {
            // will never get here, implement for placeholder
            return false;
          }

          @Override
          protected boolean processFullMatchedMNode(IMNode node, int idx, int level)
              throws MetadataException {
            // shall not traverse nodes inside template
            if (!node.getPartialPath().equals(getCurrentPartialPath(node))) {
              return true;
            }

            // if node not set template, go on traversing
            if (node.getSchemaTemplateId() != NON_TEMPLATE) {
              if (filterPreUnset && node.isSchemaTemplatePreUnset()) {
                // filter the pre unset template
                return true;
              }
              // if set template, and equals to target or target for all, add to result
              if (templateId == ALL_TEMPLATE || templateId == node.getSchemaTemplateId()) {
                resSet.add(node.getFullPath());
              }
              // descendants of the node cannot set another template, exit from this branch
              return true;
            }
            return false;
          }
        };
    setTemplatePaths.traverse();
    return resSet;
  }

  /** This method returns the templateId set on paths covered by input path pattern. */
  public Map<Integer, Set<PartialPath>> getTemplateSetInfo(PartialPath pathPattern)
      throws MetadataException {
    Map<Integer, Set<PartialPath>> result = new HashMap<>();
    CollectorTraverser<List<Integer>> collector =
        new CollectorTraverser<List<Integer>>(root, pathPattern, store) {
          @Override
          protected boolean processInternalMatchedMNode(IMNode node, int idx, int level)
              throws MetadataException {
            if (node.getSchemaTemplateId() != NON_TEMPLATE) {
              // node set template
              result
                  .computeIfAbsent(node.getSchemaTemplateId(), k -> new HashSet<>())
                  .add(getCurrentPartialPath(node));
              // descendants of the node cannot set another template, exit from this branch
              return true;
            }
            return false;
          }

          @Override
          protected boolean processFullMatchedMNode(IMNode node, int idx, int level)
              throws MetadataException {
            if (node.getSchemaTemplateId() != NON_TEMPLATE) {
              // node set template
              result
                  .computeIfAbsent(node.getSchemaTemplateId(), k -> new HashSet<>())
                  .add(getCurrentPartialPath(node));
              // descendants of the node cannot set another template, exit from this branch
              return true;
            }
            return false;
          }
        };
    collector.traverse();
    return result;
  }

  public void preUnsetTemplate(int templateId, PartialPath path) throws MetadataException {
    getNodeSetTemplate(templateId, path).preUnsetSchemaTemplate();
  }

  public void rollbackUnsetTemplate(int templateId, PartialPath path) throws MetadataException {
    getNodeSetTemplate(templateId, path).rollbackUnsetSchemaTemplate();
  }

  public void unsetTemplate(int templateId, PartialPath path) throws MetadataException {
    getNodeSetTemplate(templateId, path).unsetSchemaTemplate();
  }

  private IMNode getNodeSetTemplate(int templateId, PartialPath path) throws MetadataException {
    String[] nodeNames = path.getNodes();
    IMNode cur = root;
    for (int i = 1; i < nodeNames.length; i++) {
      cur = cur.getChild(nodeNames[i]);
      if (cur == null) {
        throw new PathNotExistException(path.getFullPath());
      }
    }
    if (cur.getSchemaTemplateId() != templateId) {
      throw new MetadataException(
          String.format("Template %s is not set on path %s", templateId, path));
    }
    return cur;
  }

  // endregion

  // region Serialization and Deserialization

  public void serialize(OutputStream outputStream) throws IOException {
    serializeInternalNode((InternalMNode) this.root, outputStream);
  }

  private void serializeInternalNode(InternalMNode node, OutputStream outputStream)
      throws IOException {
    serializeChildren(node, outputStream);

    ReadWriteIOUtils.write(INTERNAL_MNODE_TYPE, outputStream);
    ReadWriteIOUtils.write(node.getName(), outputStream);
    ReadWriteIOUtils.write(node.getSchemaTemplateId(), outputStream);
    ReadWriteIOUtils.write(node.getChildren().size(), outputStream);
  }

  private void serializeChildren(InternalMNode node, OutputStream outputStream) throws IOException {
    for (IMNode child : node.getChildren().values()) {
      if (child.isStorageGroup()) {
        serializeStorageGroupNode((StorageGroupMNode) child, outputStream);
      } else {
        serializeInternalNode((InternalMNode) child, outputStream);
      }
    }
  }

  private void serializeStorageGroupNode(
      StorageGroupMNode storageGroupNode, OutputStream outputStream) throws IOException {
    serializeChildren(storageGroupNode, outputStream);

    ReadWriteIOUtils.write(STORAGE_GROUP_MNODE_TYPE, outputStream);
    ReadWriteIOUtils.write(storageGroupNode.getName(), outputStream);
    ReadWriteIOUtils.write(storageGroupNode.getSchemaTemplateId(), outputStream);
    ThriftConfigNodeSerDeUtils.serializeTStorageGroupSchema(
        storageGroupNode.getStorageGroupSchema(), outputStream);
  }

  public void deserialize(InputStream inputStream) throws IOException {
    byte type = ReadWriteIOUtils.readByte(inputStream);
    if (type != STORAGE_GROUP_MNODE_TYPE) {
      logger.error("Wrong node type. Cannot deserialize MTreeAboveSG from given buffer");
      return;
    }

    StorageGroupMNode storageGroupMNode = deserializeStorageGroupMNode(inputStream);
    InternalMNode internalMNode;

    Stack<InternalMNode> stack = new Stack<>();
    stack.push(storageGroupMNode);

    String name = storageGroupMNode.getName();
    int childNum = 0;

    while (!PATH_ROOT.equals(name)) {
      type = ReadWriteIOUtils.readByte(inputStream);
      switch (type) {
        case INTERNAL_MNODE_TYPE:
          internalMNode = deserializeInternalMNode(inputStream);
          childNum = ReadWriteIOUtils.readInt(inputStream);
          while (childNum > 0) {
            internalMNode.addChild(stack.pop());
            childNum--;
          }
          stack.push(internalMNode);
          name = internalMNode.getName();
          break;
        case STORAGE_GROUP_MNODE_TYPE:
          storageGroupMNode = deserializeStorageGroupMNode(inputStream);
          childNum = 0;
          stack.push(storageGroupMNode);
          name = storageGroupMNode.getName();
          break;
        default:
          logger.error("Unrecognized node type. Cannot deserialize MTreeAboveSG from given buffer");
          return;
      }
    }
    this.root = stack.peek();
  }

  private InternalMNode deserializeInternalMNode(InputStream inputStream) throws IOException {
    InternalMNode internalMNode = new InternalMNode(null, ReadWriteIOUtils.readString(inputStream));
    internalMNode.setSchemaTemplateId(ReadWriteIOUtils.readInt(inputStream));
    return internalMNode;
  }

  private StorageGroupMNode deserializeStorageGroupMNode(InputStream inputStream)
      throws IOException {
    StorageGroupMNode storageGroupMNode =
        new StorageGroupMNode(null, ReadWriteIOUtils.readString(inputStream));
    storageGroupMNode.setSchemaTemplateId(ReadWriteIOUtils.readInt(inputStream));
    storageGroupMNode.setStorageGroupSchema(
        ThriftConfigNodeSerDeUtils.deserializeTStorageGroupSchema(inputStream));
    return storageGroupMNode;
  }

  // endregion
}
