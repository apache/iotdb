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
package org.apache.iotdb.db.metadata.mtree.store.disk.schemafile;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.metadata.schemafile.SchemaFileNotExists;
import org.apache.iotdb.db.metadata.MetadataConstant;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mnode.InternalMNode;
import org.apache.iotdb.db.metadata.mnode.MNodeUtils;
import org.apache.iotdb.db.metadata.mnode.StorageGroupEntityMNode;
import org.apache.iotdb.db.metadata.mnode.StorageGroupMNode;
import org.apache.iotdb.db.metadata.utils.MetaUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * SFManager(Schema File Manager) enables MTreeStore operates MNodes on disk ignoring specific file.
 *
 * <p>This class mainly implements these functions:
 * <li>Scan target directory, recover MTree above storage group
 * <li>Extract storage group, choose corresponding file and operate on it
 * <li>Maintain nodes above storage group on MTree
 */
public class SFManager implements ISchemaFileManager {

  public static String fileDirs =
      IoTDBDescriptor.getInstance().getConfig().getSchemaDir()
          + File.separator
          + MetadataConstant.SCHEMA_FILE_DIR;

  // key for the path from root to sgNode
  Map<String, ISchemaFile> schemaFiles;
  IMNode root;

  private static class SFManagerHolder {
    private SFManagerHolder() {}

    private static final SFManager Instance = new SFManager();
  }

  public static SFManager getInstance() {
    return SFManagerHolder.Instance;
  }

  protected SFManager() {
    schemaFiles = new HashMap<>();
    root = new InternalMNode(null, MetadataConstant.ROOT);
  }

  // region Interfaces
  @Override
  public IMNode init() throws MetadataException, IOException {
    clearSchemaFiles();
    return MockSFManager.cloneMNode(root);
  }

  @Override
  public IMNode initWithLoad() throws MetadataException, IOException {
    loadSchemaFiles();
    return getUpperMTree();
  }

  @Override
  public void writeMNode(IMNode node) throws MetadataException, IOException {
    // write a node above and not equals storage group
    if (getStorageGroupNode(node) == null) {
      // TODO: hotfix for decoupling upper tree nodes from input or output nodes
      SchemaFile.setNodeAddress(appendUpperTree(node.getPartialPath().getNodes()), 0L);
      SchemaFile.setNodeAddress(node, 0L);
      for (IMNode child : node.getChildren().values()) {
        // FIXME: it may wipe out existed page content !
        // with non-sgChild, append Internal directly
        // sgChild with cache, continue directly
        // sgChild with no cache, init if load null. load method will load from disk but not create
        if (child.isStorageGroup()) {
          if (!schemaFiles.containsKey(child.getFullPath())
              && loadSchemaFileInst(child.getFullPath()) == null) {
            initNewSchemaFile(
                child.getFullPath(), child.getAsStorageGroupMNode().getDataTTL(), child.isEntity());
            appendStorageGroupNode(child);
          }
        } else {
          SchemaFile.setNodeAddress(appendUpperTree(child.getPartialPath().getNodes()), 0L);
          SchemaFile.setNodeAddress(child, 0L);
        }
      }
    } else {
      IMNode sgNode = getStorageGroupNode(node);
      String sgName = sgNode.getFullPath();

      // if SG has no file in map, load from disk; if no file on disk, init one
      if (!schemaFiles.containsKey(sgName)) {
        if (loadSchemaFileInst(sgName) == null) {
          initNewSchemaFile(
              sgName, sgNode.getAsStorageGroupMNode().getDataTTL(), sgNode.isEntity());
        }
        appendStorageGroupNode(sgNode);
      }

      // FIXME: convert StorageGroupNode to StorageGroupEntityNode or otherwise, if necessary
      IMNode sgNodeOnUpperTree = getNodeOnUpperTree(sgNode.getPartialPath().getNodes());
      if (sgNode.isEntity()) {
        MNodeUtils.setToEntity(sgNodeOnUpperTree);
      } else {
        if (sgNodeOnUpperTree.isEntity()) {
          MNodeUtils.setToInternal(sgNodeOnUpperTree.getAsEntityMNode());
        }
      }

      schemaFiles.get(sgName).writeMNode(node);
    }
  }

  @Override
  public void deleteMNode(IMNode node) throws MetadataException, IOException {
    if (node.isStorageGroup()) {
      // delete entire corresponding file
      removeSchemaFile(node.getFullPath());
      pruneStorageGroupNode(node);
    } else {
      IMNode sgNode = getStorageGroupNode(node);
      if (sgNode == null) {
        // delete node above storage group
        removeDescendantFiles(node);
        node.getParent().deleteChild(node.getName());
      } else {
        // delete inside a schema file
        loadAndUpdateUpperTree(node);
        schemaFiles.get(getStorageGroupNode(node).getFullPath()).delete(node);
      }
    }
  }

  @Override
  public void close() throws MetadataException, IOException {
    for (ISchemaFile file : schemaFiles.values()) {
      file.close();
    }
    schemaFiles.clear();
    root = new InternalMNode(null, MetadataConstant.ROOT);
  }

  /**
   * Close corresponding schema file of the sgNode. <br>
   * Notice: This method expires upper tree from MEM.
   */
  void close(IMNode sgNode) throws MetadataException, IOException {
    if (!sgNode.isStorageGroup()) {
      throw new MetadataException(
          String.format(
              "Node [%s] is not a storage group node, cannot close schema file either.",
              sgNode.getFullPath()));
    }

    if (schemaFiles.containsKey(sgNode.getFullPath())) {
      schemaFiles.get(sgNode.getFullPath()).close();
      schemaFiles.remove(sgNode.getFullPath());
    }
    pruneStorageGroupNode(sgNode);
  }

  @Override
  public void sync() throws MetadataException, IOException {
    for (ISchemaFile file : schemaFiles.values()) {
      file.sync();
    }
  }

  public void sync(String sgName) throws MetadataException, IOException {
    if (schemaFiles.containsKey(sgName)) {
      schemaFiles.get(sgName).sync();
    }
  }

  @Override
  public void clear() throws MetadataException, IOException {
    for (ISchemaFile file : schemaFiles.values()) {
      file.clear();
    }
  }

  @Override
  public IMNode getChildNode(IMNode parent, String childName)
      throws MetadataException, IOException {
    if (getStorageGroupNode(parent) == null) {
      return MockSFManager.cloneMNode(
          getNodeOnUpperTree(parent.getPartialPath().getNodes()).getChild(childName));
    }

    loadAndUpdateUpperTree(parent);
    try {
      IMNode res =
          schemaFiles
              .get(getStorageGroupNode(parent).getFullPath())
              .getChildNode(parent, childName);
      return MockSFManager.cloneMNode(res);
    } catch (MetadataException e) {
      // TODO 1: handle get child by alias with a smarter way
      // TODO 2: it will hide exceptions even not for non-child case, which is not reasonable
      Iterator<IMNode> allChildren = getChildren(parent);
      while (allChildren.hasNext()) {
        IMNode cur = allChildren.next();
        if (cur.isMeasurement()
            && cur.getAsMeasurementMNode().getAlias() != null
            && cur.getAsMeasurementMNode().getAlias().equals(childName)) {
          return MockSFManager.cloneMNode(cur);
        }
      }
      return null;
    }
  }

  @Override
  public Iterator<IMNode> getChildren(IMNode parent) throws MetadataException, IOException {
    if (getStorageGroupNode(parent) == null) {
      List<IMNode> resSet = new ArrayList<>();
      for (IMNode resNode :
          getNodeOnUpperTree(parent.getPartialPath().getNodes()).getChildren().values()) {
        resSet.add(MockSFManager.cloneMNode(resNode));
      }
      // return
      // getNodeOnUpperTree(parent.getPartialPath().getNodes()).getChildren().values().iterator();
      return resSet.iterator();
    }

    try {
      loadAndUpdateUpperTree(parent);
      return schemaFiles.get(getStorageGroupNode(parent).getFullPath()).getChildren(parent);
    } catch (MetadataException e) {
      // throw wrapped exception since class above SFManager shall not perceive page or segment
      // inside schema file
      // TODO: same problem with getChildNode method
      return Collections.emptyIterator();
    }
  }

  // endregion

  private void loadAndUpdateUpperTree(IMNode parent) throws MetadataException, IOException {
    IMNode sgNode = getStorageGroupNode(parent);
    String sgName = sgNode.getFullPath();
    if (!schemaFiles.containsKey(sgName)) {
      if (loadSchemaFileInst(sgName) == null) {
        throw new SchemaFileNotExists(
            sgName + IoTDBConstant.PATH_SEPARATOR + MetadataConstant.SCHEMA_FILE_SUFFIX);
      }
      appendStorageGroupNode(sgNode);
    }
  }

  // return a copy of upper tree
  public IMNode getUpperMTree() {
    IMNode cur, rCur, rNode;
    IMNode rRoot = new InternalMNode(null, MetadataConstant.ROOT);
    Deque<IMNode> stack = new ArrayDeque<>();
    stack.push(root);
    stack.push(rRoot);
    while (stack.size() != 0) {
      rCur = stack.pop();
      cur = stack.pop();
      for (IMNode node : cur.getChildren().values()) {
        if (node.isStorageGroup()) {
          // on an upper tree, storage group node has no child
          rNode =
              node.isEntity()
                  ? new StorageGroupEntityMNode(
                      rCur, node.getName(), node.getAsStorageGroupMNode().getDataTTL())
                  : new StorageGroupMNode(
                      rCur, node.getName(), node.getAsStorageGroupMNode().getDataTTL());
        } else {
          rNode = new InternalMNode(rCur, node.getName());
          stack.push(node);
          stack.push(rNode);
        }
        rCur.addChild(rNode);
      }
    }
    return rRoot;
  }

  private void loadSchemaFiles() throws MetadataException, IOException {
    File dir = new File(fileDirs);
    if (!dir.isDirectory()) {
      if (dir.exists()) {
        throw new MetadataException("Invalid path for Schema File directory.");
      }
      if (!dir.mkdirs()) {
        throw new MetadataException(
            String.format("Schema file directory [%s] failed to initiate.", dir.getAbsolutePath()));
      }
    }

    File[] files = dir.listFiles();
    assert files != null;
    for (File f : files) {
      String[] fileName = MetaUtils.splitPathToDetachedPath(f.getName());
      if (fileName[fileName.length - 1].equals(MetadataConstant.SCHEMA_FILE_SUFFIX)) {
        restoreStorageGroup(Arrays.copyOfRange(fileName, 0, fileName.length - 1));
      }
    }
  }

  private void clearSchemaFiles() throws MetadataException, IOException {
    File dir = new File(fileDirs);
    if (!dir.isDirectory()) {
      if (dir.exists()) {
        throw new MetadataException("Invalid path for Schema File directory.");
      }
      if (!dir.mkdirs()) {
        throw new MetadataException(
            String.format("Schema file directory [%s] failed to initiate.", dir.getAbsolutePath()));
      }
    }
    File[] files = dir.listFiles();
    assert files != null;
    for (File f : files) {
      if (!f.delete()) {
        throw new MetadataException(
            String.format("Schema file [%s] failed to delete.", f.getAbsolutePath()));
      }
    }
  }

  private ISchemaFile loadSchemaFileInst(String sgName) throws MetadataException, IOException {
    try {
      ISchemaFile file = SchemaFile.loadSchemaFile(sgName);
      schemaFiles.put(sgName, file);
      return file;
    } catch (SchemaFileNotExists e) {
      return null;
    }
  }

  private ISchemaFile initNewSchemaFile(String sgName, long dataTTL, boolean isEntity)
      throws MetadataException, IOException {
    schemaFiles.put(sgName, SchemaFile.initSchemaFile(sgName, dataTTL, isEntity));
    return schemaFiles.get(sgName);
  }

  private void removeSchemaFile(String sgName) throws MetadataException, IOException {
    if (schemaFiles.containsKey(sgName)) {
      schemaFiles.get(sgName).close();
      schemaFiles.remove(sgName);
    }
    File file = new File(getFilePath(sgName));
    Files.delete(Paths.get(file.toURI()));
  }

  private void restoreStorageGroup(String[] nodes) throws MetadataException, IOException {
    String sgName = String.join(IoTDBConstant.PATH_SEPARATOR + "", nodes);
    ISchemaFile fileInst = SchemaFile.loadSchemaFile(sgName);
    appendStorageGroupNode(
        nodes, fileInst.init().getAsStorageGroupMNode().getDataTTL(), fileInst.init().isEntity());
    schemaFiles.put(sgName, fileInst);
  }

  private IMNode getStorageGroupNode(IMNode node) throws MetadataException {
    IMNode cur = node;
    while (cur != null && !cur.isStorageGroup()) {
      cur = cur.getParent();
    }
    // return null if above storage group
    return cur;
  }

  /**
   * Append corresponding storage node into upper tree, which is member of the class
   *
   * @param sgNode It is a node from MTree rather than the tree inside this class
   */
  private void appendStorageGroupNode(IMNode sgNode) throws MetadataException {
    appendStorageGroupNode(
        sgNode.getPartialPath().getNodes(),
        sgNode.getAsStorageGroupMNode().getDataTTL(),
        sgNode.isEntity());
  }

  private void appendStorageGroupNode(String[] nodes, long dataTTL, boolean isEntity)
      throws MetadataException {
    if (!nodes[0].equals(root.getName())) {
      throw new MetadataException(
          "Schema File with invalid name: "
              + String.join(IoTDBConstant.PATH_SEPARATOR + "", nodes));
    }

    IMNode cur = root;

    for (int i = 1; i < nodes.length - 1; i++) {
      if (!cur.hasChild(nodes[i])) {
        cur.addChild(new InternalMNode(cur, nodes[i]));
      }
      cur = cur.getChild(nodes[i]);
      if (cur.isStorageGroup()) {
        throw new MetadataException(
            String.format(
                "Path [%s] cannot be a prefix and a storage group at same time.",
                cur.getFullPath()));
      }
    }
    cur.addChild(
        isEntity
            ? new StorageGroupEntityMNode(cur, nodes[nodes.length - 1], dataTTL)
            : new StorageGroupMNode(cur, nodes[nodes.length - 1], dataTTL));
    SchemaFile.setNodeAddress(cur.getChild(nodes[nodes.length - 1]), 0L);
  }

  /**
   * Delete target storage group node, and remove all non-child node as well
   *
   * @param sgNode target storage group node from outer MTree.
   */
  private void pruneStorageGroupNode(IMNode sgNode) throws MetadataException {
    String[] nodes = sgNode.getPartialPath().getNodes();
    IMNode cur = root;
    for (int i = 1; i < nodes.length; i++) {
      if (!cur.hasChild(nodes[i])) {
        throw new MetadataException(
            String.format("Path does not exists for schema files.", sgNode.getFullPath()));
      }
      cur = cur.getChild(nodes[i]);
    }
    IMNode par = cur.getParent();
    par.deleteChild(cur.getName());
  }

  private IMNode appendUpperTree(String[] pathNodes) throws MetadataException {
    IMNode cur = root;
    if (!cur.getName().equals(pathNodes[0])) {
      throw new MetadataException("Path [%s] has incorrect root.");
    }
    for (int i = 1; i < pathNodes.length; i++) {
      if (!cur.hasChild(pathNodes[i])) {
        if (cur.isStorageGroup()) {
          throw new MetadataException(
              String.format(
                  "Path [%s] cannot be a prefix and a storage group at same time.",
                  cur.getFullPath()));
        }
        cur.addChild(new InternalMNode(cur, pathNodes[i]));
      }
      cur = cur.getChild(pathNodes[i]);
    }
    return cur;
  }

  private void removeDescendantFiles(IMNode node) throws MetadataException, IOException {
    // parameter node shall not be a storage group
    // remove all descendant schema files, remains upper none child node  unmodified.
    Deque<IMNode> stack = new ArrayDeque<>();
    stack.push(node);
    while (stack.size() != 0) {
      IMNode cur = stack.pop();
      if (cur.isStorageGroup()) {
        String sgName = cur.getFullPath();
        removeSchemaFile(sgName);
      } else {
        stack.addAll(cur.getChildren().values());
      }
    }
  }

  private IMNode getNodeOnUpperTree(String[] pathNodes) throws MetadataException {
    IMNode cur = root;
    if (!cur.getName().equals(pathNodes[0])) {
      throw new MetadataException("Path [%s] has incorrect root.");
    }
    for (int i = 1; i < pathNodes.length; i++) {
      if (!cur.hasChild(pathNodes[i])) {
        return null;
      }
      cur = cur.getChild(pathNodes[i]);
    }
    return cur;
  }

  private String getFilePath(String sgName) {
    return fileDirs
        + File.separator
        + sgName
        + IoTDBConstant.PATH_SEPARATOR
        + MetadataConstant.SCHEMA_FILE_SUFFIX;
  }
}
