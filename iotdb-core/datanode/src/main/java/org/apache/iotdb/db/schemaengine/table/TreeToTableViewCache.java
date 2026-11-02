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
package org.apache.iotdb.db.schemaengine.table;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.MetadataLeaseFencedException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternUtil;
import org.apache.iotdb.commons.schema.node.utils.IMNodeFactory;
import org.apache.iotdb.commons.schema.table.TreeViewSchema;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.db.i18n.DataNodeSchemaMessages;
import org.apache.iotdb.db.schemaengine.lease.MetadataLeaseManager;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.mem.mnode.IMemMNode;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.loader.MNodeFactoryLoader;
import org.apache.iotdb.db.schemaengine.schemaregion.view.TreeToTableViewReference;

import org.apache.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.apache.iotdb.commons.conf.IoTDBConstant.MULTI_LEVEL_PATH_WILDCARD;
import static org.apache.iotdb.commons.conf.IoTDBConstant.PATH_ROOT;

/**
 * The index of the tree to table views on DataNode, keyed by the fixed prefix of each view's tree
 * path pattern.
 *
 * <p>Only DataNodeTableCache writes this index, and it does so while holding its own write lock, so
 * every write here also holds the write lock of this cache and the lock order is always
 * DataNodeTableCache's write lock and then this cache's write lock. A reader only takes this
 * cache's read lock, so the two locks can never form a cycle.
 */
public class TreeToTableViewCache implements ITreeToTableViewCache {

  public static final Logger LOGGER = LoggerFactory.getLogger(TreeToTableViewCache.class);

  private final IMNodeFactory<IMemMNode> nodeFactory =
      MNodeFactoryLoader.getInstance().getMemMNodeIMNodeFactory();
  // This index describes path prefixes, not the database/device roles in a SchemaRegion. clear()
  // replaces it with a fresh one.
  private IMemMNode rootNode = nodeFactory.createInternalMNode(null, PATH_ROOT);

  private final Map<IMemMNode, Set<TreeToTableViewReference>> viewReferences =
      new IdentityHashMap<>();
  private final Map<Pair<String, String>, PartialPath> viewPaths = new HashMap<>();
  private final ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();

  /**
   * Whether any view is registered. A cluster without any view answers "no view covers this path"
   * without taking the lock or walking the trie. It is written only under the write lock, and read
   * without any lock.
   */
  private volatile boolean hasView = false;

  private TreeToTableViewCache() {}

  @Override
  public void clear() {
    readWriteLock.writeLock().lock();
    try {
      // A node is only reachable from its parent, so a fresh root drops the whole trie at once.
      rootNode = nodeFactory.createInternalMNode(null, PATH_ROOT);
      viewReferences.clear();
      viewPaths.clear();
      hasView = false;
    } finally {
      readWriteLock.writeLock().unlock();
    }
  }

  @Override
  public void init(final Map<String, List<TsTable>> initialTables) {
    readWriteLock.writeLock().lock();
    try {
      // The index is rebuilt from scratch, so the entry of a view which is gone does not survive.
      clear();
      initialTables.forEach(
          (database, tables) -> tables.forEach(table -> registerView(database, table)));
    } finally {
      readWriteLock.writeLock().unlock();
    }
  }

  /**
   * Registers the view at the path of its tree path pattern. The caller has already picked the
   * tables which belong in the index, see DataNodeTableCache#collectInitialViews.
   */
  private void registerView(final String database, final TsTable table) {
    final Optional<String> treePath = table.getPropValue(TreeViewSchema.TREE_PATH_PATTERN);
    if (treePath.isEmpty()) {
      return;
    }
    try {
      addView(
          database,
          table.getTableName(),
          TreeViewSchema.isRestrict(table),
          table.getTagNum(),
          new PartialPath(treePath.get()));
    } catch (final IllegalPathException e) {
      LOGGER.warn(
          DataNodeSchemaMessages
              .LOG_FAILED_TO_REGISTER_THE_TREE_TO_TABLE_VIEW_ARG_ARG_DURING_INIT_ILLEGAL_PATH_359A451E,
          database,
          table.getTableName(),
          e);
    }
  }

  /**
   * Register a view at a fixed prefix path.
   *
   * @parm Partial path, eg: root.db.dev01.**;
   * @return true if a new reference was registered; false if it was already present
   */
  @Override
  public boolean addView(
      final String database,
      final String tableName,
      final boolean isRestrict,
      final int numOfTags,
      final PartialPath path)
      throws IllegalPathException {
    int prefixLength = checkPartialPath(path);
    final String[] nodes = path.getNodes();
    readWriteLock.writeLock().lock();
    try {
      final PartialPath registeredPath = viewPaths.get(new Pair<>(database, tableName));
      if (Objects.nonNull(registeredPath) && !Objects.equals(registeredPath, path)) {
        // A view is registered at exactly one path, so the entry left at the previous path has to
        // go, or removing this view by name later would miss it.
        removeView(database, tableName);
      }
      viewPaths.put(new Pair<>(database, tableName), path);

      IMemMNode current = rootNode;
      for (int i = 1; i < prefixLength; i++) {
        IMemMNode child = current.getChild(nodes[i]);
        if (child == null) {
          child = current.addChild(nodes[i], nodeFactory.createInternalMNode(current, nodes[i]));
        }
        current = child;
      }

      final TreeToTableViewReference reference =
          new TreeToTableViewReference(database, tableName, isRestrict, numOfTags);
      final Set<TreeToTableViewReference> references =
          viewReferences.computeIfAbsent(current, node -> new HashSet<>());
      final boolean added = !references.remove(reference);
      references.add(reference);
      // whether the reference is new or not, this path is covered by a view now
      hasView = true;
      return added;
    } finally {
      readWriteLock.writeLock().unlock();
    }
  }

  /**
   * Removes a view from the trie.
   *
   * @return true if the view was registered and has been removed; false otherwise
   */
  @Override
  public boolean removeView(String database, String tableName) {
    readWriteLock.writeLock().lock();
    try {
      return removeViewLocked(database, tableName);
    } finally {
      readWriteLock.writeLock().unlock();
    }
  }

  /**
   * Removes every view of the database. A database of the same name may be created again with views
   * which have nothing to do with the dropped ones, so the entries of the old database have to go
   * together with it.
   */
  @Override
  public void removeViews(final String database) {
    readWriteLock.writeLock().lock();
    try {
      // viewPaths is the record of what is registered, so the views which the table cache does not
      // hold any more are removed here as well.
      final List<String> tableNames = new ArrayList<>();
      for (final Pair<String, String> view : viewPaths.keySet()) {
        if (Objects.equals(database, view.getLeft())) {
          tableNames.add(view.getRight());
        }
      }
      tableNames.forEach(tableName -> removeViewLocked(database, tableName));
    } finally {
      readWriteLock.writeLock().unlock();
    }
  }

  /** The body of {@link #removeView}, which the caller runs while holding the write lock. */
  private boolean removeViewLocked(String database, String tableName) {
    final RegisteredView registered = getRegisteredView(database, tableName);
    if (Objects.isNull(registered)) {
      return false;
    }

    registered.references.remove(registered.reference);
    if (registered.references.isEmpty()) {
      viewReferences.remove(registered.node);
      pruneNodesWithoutView(registered.node);
      hasView = !viewReferences.isEmpty();
    }
    viewPaths.remove(new Pair<>(database, tableName));
    return true;
  }

  @Override
  public boolean alterViewName(String database, String newName, String oldName) {
    readWriteLock.writeLock().lock();
    try {
      final RegisteredView registered = getRegisteredView(database, oldName);
      if (Objects.isNull(registered)) {
        LOGGER.warn(
            DataNodeSchemaMessages.LOG_TREE_TO_TABLE_VIEW_ARG_ARG_IS_NOT_REGISTERED_BBD78902,
            database,
            oldName);
        return false;
      }

      final TreeToTableViewReference oldReference = registered.reference;
      registered.references.remove(oldReference);
      registered.references.add(
          new TreeToTableViewReference(
              database, newName, oldReference.isRestrict(), oldReference.getNumOfTags()));
      // A rename does not move the view, so the path of the old name follows the new name.
      final PartialPath path = viewPaths.remove(new Pair<>(database, oldName));
      viewPaths.put(new Pair<>(database, newName), path);
      return true;
    } finally {
      readWriteLock.writeLock().unlock();
    }
  }

  @Override
  public boolean alterViewProperties(
      final String database, final String tableName, final Map<String, String> properties) {
    readWriteLock.writeLock().lock();
    try {
      final RegisteredView registered = getRegisteredView(database, tableName);
      if (Objects.isNull(registered)) {
        LOGGER.warn(
            DataNodeSchemaMessages.LOG_TREE_TO_TABLE_VIEW_ARG_ARG_IS_NOT_REGISTERED_BBD78902,
            database,
            tableName);
        return false;
      }

      final TreeToTableViewReference oldReference = registered.reference;
      final String restrict = properties.get(TreeViewSchema.RESTRICT);
      final String numOfTags = properties.get(TreeToTableViewReference.NUM_OF_TAGS);
      registered.references.remove(oldReference);
      registered.references.add(
          new TreeToTableViewReference(
              database,
              tableName,
              Objects.isNull(restrict) ? oldReference.isRestrict() : Boolean.parseBoolean(restrict),
              Objects.isNull(numOfTags)
                  ? oldReference.getNumOfTags()
                  : Integer.parseInt(numOfTags)));
      return true;
    } finally {
      readWriteLock.writeLock().unlock();
    }
  }

  /**
   * The place a registered view lives: the trie node it sits at, the references of that node, and
   * the reference of the view itself.
   */
  private static class RegisteredView {
    IMemMNode node;
    Set<TreeToTableViewReference> references;
    TreeToTableViewReference reference;

    private RegisteredView(
        final IMemMNode node,
        final Set<TreeToTableViewReference> references,
        final TreeToTableViewReference reference) {
      this.node = node;
      this.references = references;
      this.reference = reference;
    }
  }

  /**
   * The registered view of these names, or null when no view of them is registered. The node may
   * also exist as an intermediate node of another view's path, without any view of its own.
   */
  private RegisteredView getRegisteredView(final String database, final String tableName) {
    final IMemMNode node = getViewNode(database, tableName);
    final Set<TreeToTableViewReference> references =
        Objects.isNull(node) ? null : viewReferences.get(node);
    if (Objects.isNull(references)) {
      return null;
    }
    final TreeToTableViewReference reference = findReference(references, database, tableName);
    return Objects.isNull(reference) ? null : new RegisteredView(node, references, reference);
  }

  /** The trie node the view of these names is registered at, or null if it is not registered. */
  private IMemMNode getViewNode(final String database, final String tableName) {
    final PartialPath path = viewPaths.get(new Pair<>(database, tableName));
    if (Objects.isNull(path)) {
      return null;
    }
    final String[] nodes = path.getNodes();
    return findNode(nodes, nodes.length - 1);
  }

  /** The reference of the view of these names in {@code references}, or null if it holds none. */
  private static TreeToTableViewReference findReference(
      final Set<TreeToTableViewReference> references,
      final String database,
      final String tableName) {
    for (final TreeToTableViewReference reference : references) {
      if (Objects.equals(database, reference.getDatabaseName())
          && Objects.equals(tableName, reference.getTableName())) {
        return reference;
      }
    }
    return null;
  }

  /**
   * @param path the device path, eg, root.db01.plant01.dev01
   * @return the views covering the device path, empty if there is none
   */
  @Override
  public Set<TreeToTableViewReference> getTreeToTableViews(PartialPath path) {
    // A fenced DataNode must not answer from its possibly stale index, not even with "no view
    // covers this path": the caller has to know that the cache is unavailable rather than empty.
    failIfMetadataLeaseFenced();
    if (!hasView) {
      // the answer for every path when no view is registered at all
      return Collections.emptySet();
    }
    final String[] nodes = path.getNodes();
    readWriteLock.readLock().lock();
    try {
      Set<TreeToTableViewReference> result = new HashSet<>();
      IMemMNode current = rootNode;
      int currentLevel = 1;
      while (current != null && currentLevel < (nodes.length + 1)) {
        // maybe has the qualified nodes, check and collect
        if (viewReferences.containsKey(current)) {
          checkAndCollectView(viewReferences.get(current), nodes.length - currentLevel, result);
        }
        if (currentLevel == nodes.length) {
          break;
        }
        current = current.getChild(nodes[currentLevel]);
        currentLevel++;
      }
      return result;
    } finally {
      readWriteLock.readLock().unlock();
    }
  }

  @Override
  public boolean hasView() {
    return hasView;
  }

  /**
   * The path a view is registered at, so that a caller which knows only the names of the view can
   * still locate and remove it.
   *
   * @return the path the view is registered at, or null if no view of these names is registered
   */
  @Override
  public @Nullable PartialPath getViewPath(final String database, final String tableName) {
    readWriteLock.readLock().lock();
    try {
      return viewPaths.get(new Pair<>(database, tableName));
    } finally {
      readWriteLock.readLock().unlock();
    }
  }

  /**
   * Walks the fixed prefix of a checked path, which is the path without the leading "root" and the
   * trailing multi level wildcard, eg, nodes of root.db.dev01.** give the node of root.db.dev01.
   *
   * @return the node the prefix ends at, or null if the trie does not hold that prefix
   */
  private IMemMNode findNode(final String[] nodes, final int prefixLength) {
    IMemMNode current = rootNode;
    for (int i = 1; i < prefixLength && current != null; i++) {
      current = current.getChild(nodes[i]);
    }
    return current;
  }

  /**
   * Drops the nodes which have neither a child nor a view of their own any more, starting at {@code
   * node} and walking up to the first node that is still needed. Removing a view is rare, so the
   * walk is affordable.
   */
  private void pruneNodesWithoutView(final IMemMNode node) {
    IMemMNode current = node;
    while (current != rootNode) {
      final IMemMNode parent = current.getParent();
      if (parent == null
          || !current.getChildren().isEmpty()
          || viewReferences.containsKey(current)) {
        return;
      }
      parent.deleteChild(current.getName());
      current = parent;
    }
  }

  void failIfMetadataLeaseFenced() {
    MetadataLeaseManager.getInstance()
        .failIfMetadataLeaseFenced(
            MetadataLeaseFencedException.LeaseFencedRetryPolicy.RETRY_UNTIL_SUCCESS);
  }

  private static final class TreeToTableViewCacheHolder {
    private static final TreeToTableViewCache INSTANCE = new TreeToTableViewCache();

    private TreeToTableViewCacheHolder() {}
  }

  public static ITreeToTableViewCache getInstance() {
    return TreeToTableViewCacheHolder.INSTANCE;
  }

  // the path has to be like root.db.dev01.**;
  private int checkPartialPath(PartialPath path) throws IllegalPathException {
    final String[] nodes = path.getNodes();
    if (nodes.length == 0 || !PATH_ROOT.equals(nodes[0])) {
      throw new IllegalPathException(path.getFullPath());
    }
    if (!MULTI_LEVEL_PATH_WILDCARD.equals(nodes[nodes.length - 1])) {
      throw new IllegalPathException(path.getFullPath());
    }
    final int prefixLength = nodes.length - 1;
    // Validate before modifying the trie. Wildcards inside the fixed prefix are not supported.
    for (int i = 1; i < prefixLength; i++) {
      if (nodes[i] == null || nodes[i].isEmpty() || PathPatternUtil.hasWildcard(nodes[i])) {
        throw new IllegalPathException(path.getFullPath());
      }
    }
    return prefixLength;
  }

  private void checkAndCollectView(
      Set<TreeToTableViewReference> treeToTableViewReferences,
      int remainingLevel,
      Set<TreeToTableViewReference> result) {
    treeToTableViewReferences.forEach(
        view -> {
          int numOfTags = view.getNumOfTags();
          boolean match =
              view.isRestrict() ? numOfTags == remainingLevel : numOfTags >= remainingLevel;
          if (match) {
            result.add(view);
          }
        });
  }
}
