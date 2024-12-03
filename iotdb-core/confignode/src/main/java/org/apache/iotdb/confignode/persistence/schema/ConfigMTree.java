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

package org.apache.iotdb.confignode.persistence.schema;

import org.apache.iotdb.common.rpc.thrift.TSchemaNode;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.commons.schema.node.role.IDatabaseMNode;
import org.apache.iotdb.commons.schema.node.utils.IMNodeFactory;
import org.apache.iotdb.commons.schema.node.utils.IMNodeIterator;
import org.apache.iotdb.commons.schema.table.TableNodeStatus;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnSchema;
import org.apache.iotdb.commons.utils.PathUtils;
import org.apache.iotdb.commons.utils.ThriftConfigNodeSerDeUtils;
import org.apache.iotdb.confignode.persistence.schema.mnode.IConfigMNode;
import org.apache.iotdb.confignode.persistence.schema.mnode.factory.ConfigMNodeFactory;
import org.apache.iotdb.confignode.persistence.schema.mnode.impl.ConfigTableNode;
import org.apache.iotdb.db.exception.metadata.DatabaseAlreadySetException;
import org.apache.iotdb.db.exception.metadata.DatabaseConflictException;
import org.apache.iotdb.db.exception.metadata.DatabaseModelException;
import org.apache.iotdb.db.exception.metadata.DatabaseNotSetException;
import org.apache.iotdb.db.exception.metadata.PathAlreadyExistException;
import org.apache.iotdb.db.exception.metadata.PathNotExistException;
import org.apache.iotdb.db.exception.metadata.table.ColumnNotExistsException;
import org.apache.iotdb.db.exception.metadata.table.TableAlreadyExistsException;
import org.apache.iotdb.db.exception.metadata.table.TableNotExistsException;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.traverser.collector.DatabaseCollector;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.traverser.collector.MNodeAboveDBCollector;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.traverser.collector.MNodeCollector;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.traverser.counter.DatabaseCounter;
import org.apache.iotdb.db.schemaengine.schemaregion.utils.MetaFormatUtils;

import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.Stack;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static org.apache.iotdb.commons.conf.IoTDBConstant.ONE_LEVEL_PATH_WILDCARD;
import static org.apache.iotdb.commons.conf.IoTDBConstant.PATH_ROOT;
import static org.apache.iotdb.commons.schema.SchemaConstant.ALL_MATCH_SCOPE;
import static org.apache.iotdb.commons.schema.SchemaConstant.ALL_RESULT_NODES;
import static org.apache.iotdb.commons.schema.SchemaConstant.ALL_TEMPLATE;
import static org.apache.iotdb.commons.schema.SchemaConstant.INTERNAL_MNODE_TYPE;
import static org.apache.iotdb.commons.schema.SchemaConstant.NON_TEMPLATE;
import static org.apache.iotdb.commons.schema.SchemaConstant.ROOT;
import static org.apache.iotdb.commons.schema.SchemaConstant.STORAGE_GROUP_MNODE_TYPE;
import static org.apache.iotdb.commons.schema.SchemaConstant.TABLE_MNODE_TYPE;

// Since the ConfigMTree is all stored in memory, thus it is not restricted to manage MNode through
// MTreeStore.
public class ConfigMTree {

  private final Logger logger = LoggerFactory.getLogger(ConfigMTree.class);

  private IConfigMNode root;
  // this store is only used for traverser invoking
  private final ConfigMTreeStore store;

  private final IMNodeFactory<IConfigMNode> nodeFactory = ConfigMNodeFactory.getInstance();

  public ConfigMTree() throws MetadataException {
    store = new ConfigMTreeStore(nodeFactory);
    root = store.getRoot();
  }

  public void clear() {
    if (store != null) {
      store.clear();
      this.root = store.getRoot();
    }
  }

  // region database Management

  /**
   * CREATE DATABASE. Make sure check seriesPath before setting database
   *
   * @param path path
   */
  public void setStorageGroup(final PartialPath path) throws MetadataException {
    final String[] nodeNames = path.getNodes();
    MetaFormatUtils.checkDatabase(path.getFullPath());
    if (nodeNames.length <= 1 || !nodeNames[0].equals(root.getName())) {
      throw new IllegalPathException(path.getFullPath());
    }
    IConfigMNode cur = root;
    int i = 1;
    // e.g., path = root.a.b.sg, create internal nodes for a, b
    while (i < nodeNames.length - 1) {
      final IConfigMNode temp = store.getChild(cur, nodeNames[i]);
      if (temp == null) {
        store.addChild(cur, nodeNames[i], nodeFactory.createInternalMNode(cur, nodeNames[i]));
      } else if (temp.isDatabase()) {
        // before create database, check whether the database already exists
        throw new DatabaseConflictException(temp.getFullPath(), false);
      }
      cur = store.getChild(cur, nodeNames[i]);
      i++;
    }

    // synchronize check and add, we need addChild operation be atomic.
    // only write operations on mtree will be synchronized
    synchronized (this) {
      if (store.hasChild(cur, nodeNames[i])) {
        // node b has child sg
        throw store.getChild(cur, nodeNames[i]).isDatabase()
            ? new DatabaseAlreadySetException(path.getFullPath())
            : new DatabaseConflictException(path.getFullPath(), true);
      } else {
        final IDatabaseMNode<IConfigMNode> databaseMNode =
            nodeFactory.createDatabaseMNode(cur, nodeNames[i]);

        final IConfigMNode result = store.addChild(cur, nodeNames[i], databaseMNode.getAsMNode());

        if (result != databaseMNode) {
          throw new DatabaseConflictException(path.getFullPath(), true);
        }
      }
    }
  }

  /** Delete a database */
  public void deleteDatabase(PartialPath path) throws MetadataException {
    IDatabaseMNode<IConfigMNode> databaseMNode = getDatabaseNodeByDatabasePath(path);
    IConfigMNode cur = databaseMNode.getParent();
    // Suppose current system has root.a.b.sg1, root.a.sg2, and delete root.a.b.sg1
    // delete the database node sg1
    store.deleteChild(cur, databaseMNode.getName());

    // delete node a while retain root.a.sg2
    while (cur.getParent() != null && cur.getChildren().isEmpty()) {
      cur.getParent().deleteChild(cur.getName());
      cur = cur.getParent();
    }
  }

  /**
   * Get the database that given path pattern matches or belongs to.
   *
   * <p>Suppose we have (root.sg1.d1.s1, root.sg2.d2.s2), refer the following cases: 1. given path
   * "root.sg1", ("root.sg1") will be returned. 2. given path "root.*", ("root.sg1", "root.sg2")
   * will be returned. 3. given path "root.*.d1.s1", ("root.sg1", "root.sg2") will be returned.
   *
   * @param pathPattern a path pattern or a full path
   * @return a list contains all databases related to given path
   */
  public List<PartialPath> getBelongedDatabases(PartialPath pathPattern) throws MetadataException {
    return collectDatabases(pathPattern, ALL_MATCH_SCOPE, false, true);
  }

  /**
   * Get all database that the given path pattern matches. If using prefix match, the path pattern
   * is used to match prefix path. All timeseries start with the matched prefix path will be
   * collected.
   *
   * @param pathPattern a path pattern or a full path
   * @param scope traversing scope
   * @param isPrefixMatch if true, the path pattern is used to match prefix path
   * @return a list contains all database names under given path pattern
   */
  public List<PartialPath> getMatchedDatabases(
      PartialPath pathPattern, PathPatternTree scope, boolean isPrefixMatch)
      throws MetadataException {
    return collectDatabases(pathPattern, scope, isPrefixMatch, false);
  }

  private List<PartialPath> collectDatabases(
      PartialPath pathPattern,
      PathPatternTree scope,
      boolean isPrefixMatch,
      boolean collectInternal)
      throws MetadataException {
    List<PartialPath> result = new LinkedList<>();
    try (DatabaseCollector<?, ?> collector =
        new DatabaseCollector<List<PartialPath>, IConfigMNode>(
            root, pathPattern, store, isPrefixMatch, scope) {

          @Override
          protected void collectDatabase(IDatabaseMNode<IConfigMNode> node) {
            result.add(node.getPartialPath());
          }
        }) {
      collector.setCollectInternal(collectInternal);
      collector.traverse();
    }
    return result;
  }

  /**
   * Get all database names
   *
   * @return a list contains all distinct databases
   */
  public List<PartialPath> getAllDatabasePaths(final Boolean isTableModel) {
    final List<PartialPath> res = new ArrayList<>();
    final Deque<IConfigMNode> nodeStack = new ArrayDeque<>();
    nodeStack.add(root);
    while (!nodeStack.isEmpty()) {
      final IConfigMNode current = nodeStack.pop();
      if (current.isDatabase()) {
        if (Boolean.TRUE.equals(isTableModel) && !current.getDatabaseSchema().isIsTableModel()
            || Boolean.FALSE.equals(isTableModel) && current.getDatabaseSchema().isIsTableModel()) {
          continue;
        }
        res.add(current.getPartialPath());
      } else {
        nodeStack.addAll(current.getChildren().values());
      }
    }
    return res;
  }

  /**
   * Get the count of database matching the given path. If using prefix match, the path pattern is
   * used to match prefix path. All timeseries start with the matched prefix path will be counted.
   *
   * @param pathPattern a path pattern or a full path, may contain wildcard.
   * @param scope traversing scope
   * @param isPrefixMatch if true, the path pattern is used to match prefix path
   */
  public int getDatabaseNum(
      final PartialPath pathPattern, final PathPatternTree scope, final boolean isPrefixMatch)
      throws MetadataException {
    try (final DatabaseCounter<IConfigMNode> counter =
        new DatabaseCounter<>(root, pathPattern, store, isPrefixMatch, scope)) {
      return (int) counter.count();
    }
  }

  /**
   * E.g., root.sg is database given [root, sg], if the give path is not a database, throw exception
   */
  public IDatabaseMNode<IConfigMNode> getDatabaseNodeByDatabasePath(final PartialPath databasePath)
      throws MetadataException {
    final String[] nodes = databasePath.getNodes();
    if (nodes.length == 0 || !nodes[0].equals(root.getName())) {
      throw new IllegalPathException(databasePath.getFullPath());
    }
    IConfigMNode cur = root;
    for (int i = 1; i < nodes.length - 1; i++) {
      cur = cur.getChild(nodes[i]);
      if (cur == null) {
        throw new DatabaseNotSetException(databasePath.getFullPath());
      }
      if (cur.isDatabase()) {
        throw new DatabaseConflictException(cur.getFullPath(), false);
      }
    }

    cur = cur.getChild(nodes[nodes.length - 1]);
    if (cur == null) {
      throw new DatabaseNotSetException(databasePath.getFullPath());
    }
    if (cur.isDatabase()) {
      return cur.getAsDatabaseMNode();
    } else {
      throw new DatabaseConflictException(databasePath.getFullPath(), true);
    }
  }

  /**
   * E.g., root.sg is database given [root, sg], return the MNode of root.sg given [root, sg,
   * device], return the MNode of root.sg Get database node, the give path don't need to be database
   * path.
   */
  public IDatabaseMNode<IConfigMNode> getDatabaseNodeByPath(final PartialPath path)
      throws MetadataException {
    final String[] nodes = path.getNodes();
    if (nodes.length == 0 || !nodes[0].equals(root.getName())) {
      throw new IllegalPathException(path.getFullPath());
    }
    IConfigMNode cur = root;
    for (int i = 1; i < nodes.length; i++) {
      cur = cur.getChild(nodes[i]);
      if (cur == null) {
        break;
      }
      if (cur.isDatabase()) {
        return cur.getAsDatabaseMNode();
      }
    }
    throw new DatabaseNotSetException(path.getFullPath());
  }

  /**
   * Check whether the database of given path exists. The given path may be a prefix path of
   * existing database.
   *
   * @param path a full path or a prefix path
   */
  public boolean isDatabaseAlreadySet(PartialPath path) {
    String[] nodeNames = path.getNodes();
    IConfigMNode cur = root;
    if (!nodeNames[0].equals(root.getName())) {
      return false;
    }
    for (int i = 1; i < nodeNames.length; i++) {
      if (!store.hasChild(cur, nodeNames[i])) {
        return false;
      }
      cur = store.getChild(cur, nodeNames[i]);
      if (cur.isDatabase()) {
        return true;
      }
    }
    return true;
  }

  /**
   * Check whether the database of given path exists. The given path may be a prefix path of
   * existing database. if exists will throw MetaException.
   *
   * @param path a full path or a prefix path
   */
  public void checkDatabaseAlreadySet(final PartialPath path)
      throws DatabaseAlreadySetException, DatabaseConflictException {
    final String[] nodeNames = path.getNodes();
    IConfigMNode cur = root;
    if (!nodeNames[0].equals(root.getName())) {
      return;
    }
    for (int i = 1; i < nodeNames.length; i++) {
      if (!store.hasChild(cur, nodeNames[i])) {
        return;
      }
      cur = store.getChild(cur, nodeNames[i]);
      if (cur.isDatabase()) {
        throw new DatabaseAlreadySetException(cur.getFullPath());
      }
    }
    throw new DatabaseConflictException(path.getFullPath(), true);
  }

  // endregion

  // region MTree Node Management

  public IConfigMNode getNodeWithAutoCreate(PartialPath path) throws DatabaseNotSetException {
    String[] nodeNames = path.getNodes();
    IConfigMNode cur = root;
    IConfigMNode child;
    boolean hasStorageGroup = false;
    for (int i = 1; i < nodeNames.length; i++) {
      child = store.getChild(cur, nodeNames[i]);
      if (child == null) {
        if (hasStorageGroup) {
          child =
              store.addChild(cur, nodeNames[i], nodeFactory.createInternalMNode(cur, nodeNames[i]));
        } else {
          throw new DatabaseNotSetException(path.getFullPath());
        }
      } else if (child.isDatabase()) {
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
      PartialPath pathPattern, int nodeLevel, boolean isPrefixMatch, PathPatternTree scope)
      throws MetadataException {
    List<PartialPath> result = new LinkedList<>();
    try (MNodeAboveDBCollector<Void, IConfigMNode> collector =
        new MNodeAboveDBCollector<Void, IConfigMNode>(
            root, pathPattern, store, isPrefixMatch, scope) {
          @Override
          protected Void collectMNode(IConfigMNode node) {
            result.add(getPartialPathFromRootToNode(node));
            return null;
          }
        }) {

      collector.setTargetLevel(nodeLevel);
      collector.traverse();
      return new Pair<>(result, collector.getInvolvedDatabaseMNodes());
    }
  }

  /**
   * Get child node path in the next level of the given path pattern. This method only count in
   * nodes above database. Nodes below database, including database node will be counted by certain
   * MTreeBelowSG.
   *
   * <p>give pathPattern and the child nodes is those matching pathPattern.*
   *
   * <p>e.g., MTree has [root.a.sg1.d1.s1, root.b.sg1.d1.s2, root.c.sg1.d2.s1] given path = root
   * return [root.a, root.b]
   *
   * @param pathPattern The given path
   * @param scope traversing scope
   * @return All child nodes' seriesPath(s) of given seriesPath.
   */
  public Pair<Set<TSchemaNode>, Set<PartialPath>> getChildNodePathInNextLevel(
      PartialPath pathPattern, PathPatternTree scope) throws MetadataException {
    Set<TSchemaNode> result = new TreeSet<>();
    try (MNodeAboveDBCollector<Void, IConfigMNode> collector =
        new MNodeAboveDBCollector<Void, IConfigMNode>(
            root, pathPattern.concatNode(ONE_LEVEL_PATH_WILDCARD), store, false, scope) {
          @Override
          protected Void collectMNode(IConfigMNode node) {
            result.add(
                new TSchemaNode(
                    getPartialPathFromRootToNode(node).getFullPath(),
                    node.getMNodeType().getNodeType()));
            return null;
          }
        }) {
      collector.traverse();
      return new Pair<>(result, collector.getInvolvedDatabaseMNodes());
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
  public void checkTemplateOnPath(final PartialPath path) throws MetadataException {
    String[] nodeNames = path.getNodes();
    IConfigMNode cur = root;
    IConfigMNode child;

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
      if (cur.isDatabase() && cur.getDatabaseSchema().isIsTableModel()) {
        throw new DatabaseModelException(cur.getFullPath(), true);
      }
    }

    checkTemplateOnSubtree(cur);
  }

  // traverse all the descendant of the given path node
  private void checkTemplateOnSubtree(final IConfigMNode node) throws MetadataException {
    IConfigMNode child;
    IMNodeIterator<IConfigMNode> iterator = store.getChildrenIterator(node);
    while (iterator.hasNext()) {
      child = iterator.next();

      // Skip table model databases
      if (child.isDatabase() && child.getDatabaseSchema().isIsTableModel()) {
        continue;
      }
      if (child.getSchemaTemplateId() != NON_TEMPLATE) {
        throw new MetadataException("Template already exists on " + child.getFullPath());
      }
      checkTemplateOnSubtree(child);
    }
  }

  public List<String> getPathsSetOnTemplate(
      final int templateId, final PathPatternTree scope, final boolean filterPreUnset)
      throws MetadataException {
    final List<String> resSet = new ArrayList<>();
    try (final MNodeCollector<Void, IConfigMNode> collector =
        new MNodeCollector<Void, IConfigMNode>(
            root, new PartialPath(ALL_RESULT_NODES), store, false, scope) {
          @Override
          protected boolean acceptFullMatchedNode(final IConfigMNode node) {
            if (super.acceptFullMatchedNode(node)) {
              // if node not set template, go on traversing
              if (node.getSchemaTemplateId() != NON_TEMPLATE) {
                if (filterPreUnset && node.isSchemaTemplatePreUnset()) {
                  // filter the pre unset template
                  return false;
                }
                // if set template, and equals to target or target for all, add to result
                return templateId == ALL_TEMPLATE || templateId == node.getSchemaTemplateId();
              }
            }
            return false;
          }

          @Override
          protected Void collectMNode(final IConfigMNode node) {
            resSet.add(node.getFullPath());
            return null;
          }

          @Override
          protected boolean shouldVisitSubtreeOfFullMatchedNode(final IConfigMNode node) {
            // descendants of the node cannot set another template, exit from this branch
            return node.getSchemaTemplateId() == NON_TEMPLATE
                && !(node.isDatabase() && node.getDatabaseSchema().isIsTableModel())
                && super.shouldVisitSubtreeOfFullMatchedNode(node);
          }

          @Override
          protected boolean shouldVisitSubtreeOfInternalMatchedNode(final IConfigMNode node) {
            // descendants of the node cannot set another template, exit from this branch
            return node.getSchemaTemplateId() == NON_TEMPLATE
                && !(node.isDatabase() && node.getDatabaseSchema().isIsTableModel())
                && super.shouldVisitSubtreeOfInternalMatchedNode(node);
          }
        }) {
      collector.traverse();
    }
    return resSet;
  }

  /** This method returns the templateId set on paths covered by input path pattern. */
  public Map<Integer, Set<PartialPath>> getTemplateSetInfo(final PartialPath pathPattern)
      throws MetadataException {
    final Map<Integer, Set<PartialPath>> result = new HashMap<>();
    try (final MNodeCollector<Void, IConfigMNode> collector =
        new MNodeCollector<Void, IConfigMNode>(root, pathPattern, store, false, ALL_MATCH_SCOPE) {
          @Override
          protected boolean acceptFullMatchedNode(final IConfigMNode node) {
            return node.getSchemaTemplateId() != NON_TEMPLATE || super.acceptFullMatchedNode(node);
          }

          @Override
          protected boolean acceptInternalMatchedNode(final IConfigMNode node) {
            return node.getSchemaTemplateId() != NON_TEMPLATE
                || super.acceptInternalMatchedNode(node);
          }

          @Override
          protected Void collectMNode(final IConfigMNode node) {
            if (node.getSchemaTemplateId() != NON_TEMPLATE && !node.isSchemaTemplatePreUnset()) {
              result
                  .computeIfAbsent(node.getSchemaTemplateId(), k -> new HashSet<>())
                  .add(getPartialPathFromRootToNode(node));
            }
            return null;
          }

          @Override
          protected boolean shouldVisitSubtreeOfFullMatchedNode(final IConfigMNode node) {
            // descendants of the node cannot set another template, exit from this branch
            return node.getSchemaTemplateId() == NON_TEMPLATE
                && !(node.isDatabase() && node.getDatabaseSchema().isIsTableModel())
                && super.shouldVisitSubtreeOfFullMatchedNode(node);
          }

          @Override
          protected boolean shouldVisitSubtreeOfInternalMatchedNode(final IConfigMNode node) {
            // descendants of the node cannot set another template, exit from this branch
            return node.getSchemaTemplateId() == NON_TEMPLATE
                && !(node.isDatabase() && node.getDatabaseSchema().isIsTableModel())
                && super.shouldVisitSubtreeOfInternalMatchedNode(node);
          }
        }) {
      collector.traverse();
    }
    return result;
  }

  public void setTemplate(final int templateId, final PartialPath templateSetPath)
      throws MetadataException {
    getNodeWithAutoCreate(templateSetPath).setSchemaTemplateId(templateId);
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

  private IConfigMNode getNodeSetTemplate(int templateId, PartialPath path)
      throws MetadataException {
    String[] nodeNames = path.getNodes();
    IConfigMNode cur = root;
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

  // region table management

  public void preCreateTable(final PartialPath database, final TsTable table)
      throws MetadataException {
    final IConfigMNode databaseNode = getDatabaseNodeByDatabasePath(database).getAsMNode();
    if (!databaseNode.getDatabaseSchema().isIsTableModel()) {
      throw new DatabaseModelException(database.getFullPath(), false);
    }
    final IConfigMNode node = databaseNode.getChild(table.getTableName());
    if (node == null) {
      final ConfigTableNode tableNode =
          (ConfigTableNode)
              databaseNode.addChild(
                  table.getTableName(), new ConfigTableNode(databaseNode, table.getTableName()));
      tableNode.setTable(table);
      tableNode.setStatus(TableNodeStatus.PRE_CREATE);
    } else if (node instanceof ConfigTableNode) {
      throw new TableAlreadyExistsException(
          database.getFullPath().substring(ROOT.length() + 1), table.getTableName());
    } else {
      throw new PathAlreadyExistException(database.concatNode(table.getTableName()).getFullPath());
    }
  }

  public void rollbackCreateTable(final PartialPath database, final String tableName)
      throws MetadataException {
    final IConfigMNode databaseNode = getDatabaseNodeByDatabasePath(database).getAsMNode();
    databaseNode.deleteChild(tableName);
  }

  public void commitCreateTable(final PartialPath database, final String tableName)
      throws MetadataException {
    final IConfigMNode databaseNode = getDatabaseNodeByDatabasePath(database).getAsMNode();
    if (!databaseNode.hasChild(tableName)) {
      throw new TableNotExistsException(
          database.getFullPath().substring(ROOT.length() + 1), tableName);
    }
    final ConfigTableNode tableNode = (ConfigTableNode) databaseNode.getChild(tableName);
    if (!tableNode.getStatus().equals(TableNodeStatus.PRE_CREATE)) {
      throw new IllegalStateException();
    }
    tableNode.setStatus(TableNodeStatus.USING);
  }

  public void preDeleteTable(final PartialPath database, final String tableName)
      throws MetadataException {
    final IConfigMNode databaseNode = getDatabaseNodeByDatabasePath(database).getAsMNode();
    if (!databaseNode.hasChild(tableName)) {
      throw new TableNotExistsException(
          database.getFullPath().substring(ROOT.length() + 1), tableName);
    }
    final ConfigTableNode tableNode = (ConfigTableNode) databaseNode.getChild(tableName);
    if (tableNode.getStatus().equals(TableNodeStatus.PRE_CREATE)) {
      throw new IllegalStateException();
    }
    tableNode.setStatus(TableNodeStatus.PRE_DELETE);
  }

  public void dropTable(final PartialPath database, final String tableName)
      throws MetadataException {
    final IConfigMNode databaseNode = getDatabaseNodeByDatabasePath(database).getAsMNode();
    store.deleteChild(databaseNode, tableName);
  }

  public List<TsTable> getAllUsingTablesUnderSpecificDatabase(final PartialPath databasePath)
      throws MetadataException {
    final IConfigMNode databaseNode = getDatabaseNodeByDatabasePath(databasePath).getAsMNode();
    return databaseNode.getChildren().values().stream()
        .filter(
            child ->
                child instanceof ConfigTableNode
                    && ((ConfigTableNode) child).getStatus().equals(TableNodeStatus.USING))
        .map(child -> ((ConfigTableNode) child).getTable())
        .collect(Collectors.toList());
  }

  public List<Pair<TsTable, TableNodeStatus>> getAllTablesUnderSpecificDatabase(
      final PartialPath databasePath) throws MetadataException {
    final IConfigMNode databaseNode = getDatabaseNodeByDatabasePath(databasePath).getAsMNode();
    return databaseNode.getChildren().values().stream()
        .filter(ConfigTableNode.class::isInstance)
        .map(
            child ->
                new Pair<>(
                    ((ConfigTableNode) child).getTable(), ((ConfigTableNode) child).getStatus()))
        .collect(Collectors.toList());
  }

  public Map<String, TsTable> getSpecificTablesUnderSpecificDatabase(
      final PartialPath databasePath, final Set<String> tables) throws MetadataException {
    final IConfigMNode databaseNode = getDatabaseNodeByDatabasePath(databasePath).getAsMNode();
    final Map<String, TsTable> result = new HashMap<>();
    tables.forEach(
        table -> {
          final IConfigMNode child = databaseNode.getChildren().get(table);
          if (child instanceof ConfigTableNode
              && ((ConfigTableNode) child).getStatus().equals(TableNodeStatus.USING)) {
            result.put(table, ((ConfigTableNode) child).getTable());
          } else {
            result.put(table, null);
          }
        });
    return result;
  }

  public Map<String, List<TsTable>> getAllUsingTables() {
    return getAllDatabasePaths(true).stream()
        .collect(
            Collectors.toMap(
                PartialPath::getFullPath,
                databasePath -> {
                  try {
                    return getAllUsingTablesUnderSpecificDatabase(databasePath);
                  } catch (final MetadataException ignore) {
                    // Database path must exist because the "getAllDatabasePaths()" is called in
                    // databaseReadWriteLock.readLock().
                  }
                  return Collections.emptyList();
                }));
  }

  public Map<String, List<TsTable>> getAllPreCreateTables() throws MetadataException {
    final Map<String, List<TsTable>> result = new HashMap<>();
    final List<PartialPath> databaseList = getAllDatabasePaths(true);
    for (PartialPath databasePath : databaseList) {
      final String database = databasePath.getFullPath().substring(ROOT.length() + 1);
      final IConfigMNode databaseNode = getDatabaseNodeByDatabasePath(databasePath).getAsMNode();
      for (final IConfigMNode child : databaseNode.getChildren().values()) {
        if (child instanceof ConfigTableNode) {
          final ConfigTableNode tableNode = (ConfigTableNode) child;
          if (!tableNode.getStatus().equals(TableNodeStatus.PRE_CREATE)) {
            continue;
          }
          result.computeIfAbsent(database, k -> new ArrayList<>()).add(tableNode.getTable());
        }
      }
    }
    return result;
  }

  public void addTableColumn(
      final PartialPath database,
      final String tableName,
      final List<TsTableColumnSchema> columnSchemaList)
      throws MetadataException {
    final TsTable table = getTable(database, tableName);
    columnSchemaList.forEach(table::addColumnSchema);
  }

  public void rollbackAddTableColumn(
      final PartialPath database,
      final String tableName,
      final List<TsTableColumnSchema> columnSchemaList)
      throws MetadataException {
    final TsTable table = getTable(database, tableName);
    columnSchemaList.forEach(o -> table.removeColumnSchema(o.getColumnName()));
  }

  public void renameTableColumn(
      final PartialPath database,
      final String tableName,
      final String oldName,
      final String newName)
      throws MetadataException {
    getTable(database, tableName).renameColumnSchema(oldName, newName);
  }

  public void setTableProperties(
      final PartialPath database, final String tableName, final Map<String, String> tableProperties)
      throws MetadataException {
    final IConfigMNode databaseNode = getDatabaseNodeByDatabasePath(database).getAsMNode();
    if (!databaseNode.hasChild(tableName)) {
      throw new TableNotExistsException(
          database.getFullPath().substring(ROOT.length() + 1), tableName);
    }
    final TsTable table = ((ConfigTableNode) databaseNode.getChild(tableName)).getTable();
    tableProperties.forEach(
        (k, v) -> {
          if (Objects.nonNull(v)) {
            table.addProp(k, v);
          } else if (k.equals(TsTable.TTL_PROPERTY)
              && databaseNode.getDatabaseSchema().isSetTTL()) {
            table.addProp(k, String.valueOf(databaseNode.getDatabaseSchema().getTTL()));
          } else {
            table.removeProp(k);
          }
        });
  }

  // Return true if removed column is an attribute column
  // false if measurement column
  public boolean preDeleteColumn(
      final PartialPath database, final String tableName, final String columnName)
      throws MetadataException, SemanticException {
    final ConfigTableNode node = getTableNode(database, tableName);
    final TsTableColumnSchema columnSchema = node.getTable().getColumnSchema(columnName);
    if (Objects.isNull(columnSchema)) {
      throw new ColumnNotExistsException(
          PathUtils.unQualifyDatabaseName(database.getFullPath()), tableName, columnName);
    }
    if (columnSchema.getColumnCategory() == TsTableColumnCategory.ID
        || columnSchema.getColumnCategory() == TsTableColumnCategory.TIME) {
      throw new SemanticException("Dropping id or time column is not supported.");
    }

    node.addPreDeletedColumn(columnName);
    return columnSchema.getColumnCategory() == TsTableColumnCategory.ATTRIBUTE;
  }

  public void commitDeleteColumn(
      final PartialPath database, final String tableName, final String columnName)
      throws MetadataException {
    final ConfigTableNode node = getTableNode(database, tableName);
    final TsTable table = getTable(database, tableName);
    if (Objects.nonNull(table.getColumnSchema(columnName))) {
      table.removeColumnSchema(columnName);
      node.removePreDeletedColumn(columnName);
    }
  }

  public TsTable getUsingTableSchema(final PartialPath database, final String tableName)
      throws MetadataException {
    final ConfigTableNode node = getTableNode(database, tableName);
    if (node.getPreDeletedColumns().isEmpty()) {
      return node.getTable();
    }
    final TsTable newTable = TsTable.deserialize(ByteBuffer.wrap(node.getTable().serialize()));
    node.getPreDeletedColumns().forEach(newTable::removeColumnSchema);
    return newTable;
  }

  public Pair<TsTable, Set<String>> getTableSchemaDetails(
      final PartialPath database, final String tableName) throws MetadataException {
    final ConfigTableNode node = getTableNode(database, tableName);
    return new Pair<>(node.getTable(), node.getPreDeletedColumns());
  }

  private TsTable getTable(final PartialPath database, final String tableName)
      throws MetadataException {
    return getTableNode(database, tableName).getTable();
  }

  public Optional<TsTable> getTableIfExists(final PartialPath database, final String tableName)
      throws MetadataException {
    final IConfigMNode databaseNode = getDatabaseNodeByDatabasePath(database).getAsMNode();
    if (!databaseNode.hasChild(tableName)) {
      return Optional.empty();
    }
    final ConfigTableNode tableNode = (ConfigTableNode) databaseNode.getChild(tableName);
    return Optional.of(tableNode.getTable());
  }

  private ConfigTableNode getTableNode(final PartialPath database, final String tableName)
      throws MetadataException {
    final IConfigMNode databaseNode = getDatabaseNodeByDatabasePath(database).getAsMNode();
    if (!databaseNode.hasChild(tableName)) {
      throw new TableNotExistsException(
          database.getFullPath().substring(ROOT.length() + 1), tableName);
    }
    return ((ConfigTableNode) databaseNode.getChild(tableName));
  }

  // endregion

  // region Serialization and Deserialization

  public void serialize(final OutputStream outputStream) throws IOException {
    serializeConfigBasicMNode(this.root, outputStream);
  }

  private void serializeConfigBasicMNode(final IConfigMNode node, final OutputStream outputStream)
      throws IOException {
    serializeChildren(node, outputStream);

    ReadWriteIOUtils.write(INTERNAL_MNODE_TYPE, outputStream);
    ReadWriteIOUtils.write(node.getName(), outputStream);
    ReadWriteIOUtils.write(node.getSchemaTemplateId(), outputStream);
    ReadWriteIOUtils.write(node.getChildren().size(), outputStream);
  }

  private void serializeChildren(final IConfigMNode node, final OutputStream outputStream)
      throws IOException {
    for (final IConfigMNode child : node.getChildren().values()) {
      if (child.isDatabase()) {
        serializeDatabaseNode(child.getAsDatabaseMNode(), outputStream);
      } else if (child instanceof ConfigTableNode) {
        serializeTableNode((ConfigTableNode) child, outputStream);
      } else {
        serializeConfigBasicMNode(child, outputStream);
      }
    }
  }

  private void serializeDatabaseNode(
      final IDatabaseMNode<IConfigMNode> storageGroupNode, final OutputStream outputStream)
      throws IOException {
    serializeChildren(storageGroupNode.getAsMNode(), outputStream);

    ReadWriteIOUtils.write(STORAGE_GROUP_MNODE_TYPE, outputStream);
    ReadWriteIOUtils.write(storageGroupNode.getName(), outputStream);
    ReadWriteIOUtils.write(storageGroupNode.getAsMNode().getSchemaTemplateId(), outputStream);
    ThriftConfigNodeSerDeUtils.serializeTDatabaseSchema(
        storageGroupNode.getAsMNode().getDatabaseSchema(), outputStream);
  }

  private void serializeTableNode(final ConfigTableNode tableNode, final OutputStream outputStream)
      throws IOException {
    ReadWriteIOUtils.write(TABLE_MNODE_TYPE, outputStream);
    ReadWriteIOUtils.write(tableNode.getName(), outputStream);
    tableNode.getTable().serialize(outputStream);
    tableNode.getStatus().serialize(outputStream);
    final Set<String> preDeletedColumns = tableNode.getPreDeletedColumns();
    ReadWriteIOUtils.write(preDeletedColumns.size(), outputStream);
    for (final String column : preDeletedColumns) {
      ReadWriteIOUtils.write(column, outputStream);
    }
  }

  public void deserialize(final InputStream inputStream) throws IOException {
    byte type = ReadWriteIOUtils.readByte(inputStream);

    String name;
    int childNum;
    Stack<Pair<IConfigMNode, Boolean>> stack = new Stack<>();
    IConfigMNode databaseMNode;
    IConfigMNode internalMNode;
    IConfigMNode tableNode;

    if (type == STORAGE_GROUP_MNODE_TYPE) {
      databaseMNode = deserializeDatabaseMNode(inputStream);
      name = databaseMNode.getName();
      stack.push(new Pair<>(databaseMNode, true));
    } else if (type == TABLE_MNODE_TYPE) {
      tableNode = deserializeTableMNode(inputStream);
      name = tableNode.getName();
      stack.push(new Pair<>(tableNode, false));
    } else {
      // Currently internal mNode will not be the leaf node and thus will not be deserialized here
      // This is just in case
      internalMNode = deserializeInternalMNode(inputStream);
      ReadWriteIOUtils.readInt(inputStream);
      name = internalMNode.getName();
      stack.push(new Pair<>(internalMNode, false));
    }

    while (!PATH_ROOT.equals(name)) {
      type = ReadWriteIOUtils.readByte(inputStream);
      switch (type) {
        case INTERNAL_MNODE_TYPE:
          internalMNode = deserializeInternalMNode(inputStream);
          childNum = ReadWriteIOUtils.readInt(inputStream);
          boolean hasDB = false;
          while (childNum > 0) {
            hasDB = stack.peek().right;
            internalMNode.addChild(stack.pop().left);
            childNum--;
          }
          stack.push(new Pair<>(internalMNode, hasDB));
          name = internalMNode.getName();
          break;
        case STORAGE_GROUP_MNODE_TYPE:
          databaseMNode = deserializeDatabaseMNode(inputStream).getAsMNode();
          while (!stack.isEmpty() && Boolean.FALSE.equals(stack.peek().right)) {
            databaseMNode.addChild(stack.pop().left);
          }
          stack.push(new Pair<>(databaseMNode, true));
          name = databaseMNode.getName();
          break;
        case TABLE_MNODE_TYPE:
          tableNode = deserializeTableMNode(inputStream).getAsMNode();
          stack.push(new Pair<>(tableNode, false));
          name = tableNode.getName();
          break;
        default:
          logger.error("Unrecognized node type {} when recovering the mTree.", type);
          return;
      }
    }
    this.root = stack.peek().left;
  }

  private IConfigMNode deserializeInternalMNode(InputStream inputStream) throws IOException {
    IConfigMNode basicMNode =
        nodeFactory.createInternalMNode(null, ReadWriteIOUtils.readString(inputStream));
    basicMNode.setSchemaTemplateId(ReadWriteIOUtils.readInt(inputStream));
    return basicMNode;
  }

  private IConfigMNode deserializeDatabaseMNode(InputStream inputStream) throws IOException {
    IDatabaseMNode<IConfigMNode> databaseMNode =
        nodeFactory.createDatabaseMNode(null, ReadWriteIOUtils.readString(inputStream));
    databaseMNode.getAsMNode().setSchemaTemplateId(ReadWriteIOUtils.readInt(inputStream));
    databaseMNode
        .getAsMNode()
        .setDatabaseSchema(ThriftConfigNodeSerDeUtils.deserializeTDatabaseSchema(inputStream));
    return databaseMNode.getAsMNode();
  }

  private IConfigMNode deserializeTableMNode(final InputStream inputStream) throws IOException {
    final ConfigTableNode tableNode =
        new ConfigTableNode(null, ReadWriteIOUtils.readString(inputStream));
    tableNode.setTable(TsTable.deserialize(inputStream));
    tableNode.setStatus(TableNodeStatus.deserialize(inputStream));
    final int size = ReadWriteIOUtils.readInt(inputStream);
    for (int i = 0; i < size; ++i) {
      tableNode.addPreDeletedColumn(ReadWriteIOUtils.readString(inputStream));
    }
    return tableNode;
  }

  // endregion
}
