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

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.MNodeTypeMismatchException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.metadata.PathAlreadyExistException;
import org.apache.iotdb.db.exception.metadata.StorageGroupAlreadySetException;
import org.apache.iotdb.db.exception.metadata.StorageGroupNotSetException;
import org.apache.iotdb.db.metadata.MetadataConstant;
import org.apache.iotdb.db.metadata.SchemaEngine;
import org.apache.iotdb.db.metadata.SchemaRegion;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mnode.IStorageGroupMNode;
import org.apache.iotdb.db.metadata.mnode.InternalMNode;
import org.apache.iotdb.db.metadata.mnode.StorageGroupMNode;
import org.apache.iotdb.db.metadata.mtree.store.IMTreeStore;
import org.apache.iotdb.db.metadata.mtree.store.MemMTreeStore;
import org.apache.iotdb.db.metadata.mtree.traverser.collector.MNodeAboveSGCollector;
import org.apache.iotdb.db.metadata.mtree.traverser.collector.StorageGroupCollector;
import org.apache.iotdb.db.metadata.mtree.traverser.counter.CounterTraverser;
import org.apache.iotdb.db.metadata.mtree.traverser.counter.MNodeAboveSGLevelCounter;
import org.apache.iotdb.db.metadata.mtree.traverser.counter.StorageGroupCounter;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.metadata.template.Template;
import org.apache.iotdb.db.metadata.utils.MetaFormatUtils;
import org.apache.iotdb.tsfile.utils.Pair;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static org.apache.iotdb.commons.conf.IoTDBConstant.ONE_LEVEL_PATH_WILDCARD;
import static org.apache.iotdb.commons.conf.IoTDBConstant.PATH_ROOT;

public class MTreeAboveSG {

  private static final Logger logger = LoggerFactory.getLogger(MTreeAboveSG.class);
  public static final Gson GSON = new GsonBuilder().setPrettyPrinting().create();

  private IMNode root;
  private IMTreeStore store;

  public MTreeAboveSG() {
    root = new InternalMNode(null, PATH_ROOT);
    store = new MemMTreeStore(root);
  }

  public void clear() {
    for (IStorageGroupMNode storageGroupMNode : getAllStorageGroupNodes()) {
      storageGroupMNode.getSchemaRegion().clear();
    }
    if (store != null) {
      store.clear();
      this.root = store.getRoot();
    }
  }

  /**
   * Set storage group. Make sure check seriesPath before setting storage group
   *
   * @param path path
   */
  public void setStorageGroup(PartialPath path) throws MetadataException {
    setStorageGroup(path, false);
  }

  public void setStorageGroup(PartialPath path, boolean isRecover) throws MetadataException {
    String[] nodeNames = path.getNodes();
    MetaFormatUtils.checkStorageGroup(path.getFullPath());
    if (nodeNames.length <= 1 || !nodeNames[0].equals(root.getName())) {
      throw new IllegalPathException(path.getFullPath());
    }
    IMNode cur = root;
    Template upperTemplate = cur.getSchemaTemplate();
    int i = 1;
    // e.g., path = root.a.b.sg, create internal nodes for a, b
    while (i < nodeNames.length - 1) {
      IMNode temp = cur.getChild(nodeNames[i]);
      if (temp == null) {
        if (cur.isUseTemplate() && upperTemplate.hasSchema(nodeNames[i])) {
          throw new PathAlreadyExistException(
              cur.getPartialPath().concatNode(nodeNames[i]).getFullPath());
        }
        cur.addChild(nodeNames[i], new InternalMNode(cur, nodeNames[i]));
      } else if (temp.isStorageGroup()) {
        // before set storage group, check whether the storage group already exists
        throw new StorageGroupAlreadySetException(temp.getFullPath());
      }
      cur = cur.getChild(nodeNames[i]);
      upperTemplate = cur.getSchemaTemplate() == null ? upperTemplate : cur.getSchemaTemplate();
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
        if (cur.isUseTemplate() && upperTemplate.hasSchema(nodeNames[i])) {
          throw new PathAlreadyExistException(
              cur.getPartialPath().concatNode(nodeNames[i]).getFullPath());
        }

        if (isRecover) {
          // recover SchemaRegion
          SchemaRegion schemaRegion = new SchemaRegion();
          IStorageGroupMNode storageGroupMNode = schemaRegion.initForRecover(path);
          cur.addChild(nodeNames[i], storageGroupMNode);
          schemaRegion.recover();
        } else {
          IStorageGroupMNode storageGroupMNode =
              new StorageGroupMNode(
                  cur, nodeNames[i], IoTDBDescriptor.getInstance().getConfig().getDefaultTTL());
          // init SchemaRegion
          SchemaRegion schemaRegion = new SchemaRegion();
          schemaRegion.initNewSchemaRegion(storageGroupMNode);
          storageGroupMNode.setSchemaRegion(schemaRegion);
          IMNode result = cur.addChild(nodeNames[i], storageGroupMNode);

          if (result == storageGroupMNode) {
            return;
          }

          // another thread executed addChild before adding the prepared storageGroupMNode to MTree
          schemaRegion.deleteStorageGroup();
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
    storageGroupMNode.getSchemaRegion().deleteStorageGroup();

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
   * E.g., root.sg is storage group given [root, sg], return the MNode of root.sg given [root, sg,
   * device], throw exception Get storage group node, if the give path is not a storage group, throw
   * exception
   */
  public IStorageGroupMNode getStorageGroupNodeByStorageGroupPath(PartialPath path)
      throws MetadataException {
    IStorageGroupMNode node = getStorageGroupNodeByPath(path);
    if (!node.getPartialPath().equals(path)) {
      throw new MNodeTypeMismatchException(
          path.getFullPath(), MetadataConstant.STORAGE_GROUP_MNODE_TYPE);
    }

    return node;
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

  public List<IStorageGroupMNode> getInvolvedStorageGroupNodes(
      PartialPath pathPattern, boolean isPrefixMatch) throws MetadataException {
    List<IStorageGroupMNode> result = new ArrayList<>();
    StorageGroupCollector<List<IStorageGroupMNode>> collector =
        new StorageGroupCollector<List<IStorageGroupMNode>>(root, pathPattern, store) {
          @Override
          protected void collectStorageGroup(IStorageGroupMNode node) {
            result.add(node);
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
   * Get the count of nodes in the given level matching the given path. If using prefix match, the
   * path pattern is used to match prefix path. All timeseries start with the matched prefix path
   * will be counted.
   */
  public Pair<Integer, Set<IStorageGroupMNode>> getNodesCountInGivenLevel(
      PartialPath pathPattern, int level, boolean isPrefixMatch) throws MetadataException {
    MNodeAboveSGLevelCounter counter =
        new MNodeAboveSGLevelCounter(root, pathPattern, store, level);
    counter.setPrefixMatch(isPrefixMatch);
    counter.traverse();
    return new Pair<>(counter.getCount(), counter.getInvolvedStorageGroupMNodes());
  }

  /** Get all paths from root to the given level */
  public Pair<List<PartialPath>, Set<IStorageGroupMNode>> getNodesListInGivenLevel(
      PartialPath pathPattern, int nodeLevel, SchemaEngine.StorageGroupFilter filter)
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
  public Pair<Set<String>, Set<IStorageGroupMNode>> getChildNodePathInNextLevel(
      PartialPath pathPattern) throws MetadataException {
    try {
      MNodeAboveSGCollector<Set<String>> collector =
          new MNodeAboveSGCollector<Set<String>>(
              root, pathPattern.concatNode(ONE_LEVEL_PATH_WILDCARD), store) {
            @Override
            protected void transferToResult(IMNode node) {
              resultSet.add(getCurrentPartialPath(node).getFullPath());
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
  public Pair<Set<String>, Set<IStorageGroupMNode>> getChildNodeNameInNextLevel(
      PartialPath pathPattern) throws MetadataException {
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

  @Override
  public String toString() {
    JsonObject jsonObject = new JsonObject();
    jsonObject.add(root.getName(), mNodeToJSON(root));
    return jsonToString(jsonObject);
  }

  private JsonObject mNodeToJSON(IMNode node) {
    JsonObject jsonObject = new JsonObject();
    if (node.getChildren().size() > 0) {
      for (IMNode child : node.getChildren().values()) {
        if (child.isStorageGroup()) {
          jsonObject.add(
              child.getName(),
              child
                  .getAsStorageGroupMNode()
                  .getSchemaRegion()
                  .getMetadataInJson()
                  .get(child.getFullPath()));
        } else {
          jsonObject.add(child.getName(), mNodeToJSON(child));
        }
      }
    }
    return jsonObject;
  }

  private static String jsonToString(JsonObject jsonObject) {
    return GSON.toJson(jsonObject);
  }
}
