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
package org.apache.iotdb.db.metadata;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.apache.iotdb.db.exception.PathErrorException;

/**
 * "PTree" is the shorthand for "Property Tree". One {@code PTree} consists several {@code PNode}
 */
public class PTree implements Serializable {

  private static final long serialVersionUID = 2642766399323283900L;
  private static final String PTREE_NOT_EXIST = "PTree seriesPath not exist. ";
  private PNode root;
  private MTree mTree;
  private String name;
  private String space = "    ";

  PTree(String name, MTree mTree) {
    this.setRoot(new PNode(name, null, false));
    this.setName(name);
    this.setmTree(mTree);
  }

  /**
   * Add a seriesPath to current PTree
   *
   * @return The count of new added {@code PNode} TODO: unused
   * @throws PathErrorException
   */
  int addPath(String path) throws PathErrorException {
    int addCount = 0;
    if (getRoot() == null) {
      throw new PathErrorException("Root Node is null, Please initialize root first");
    }
    String[] nodes = MetaUtils.getNodeNames(path, "\\.");
    if (nodes.length <= 1 || !nodes[0].equals(getRoot().getName())) {
      throw new PathErrorException("Input seriesPath not exist. Path: " + path);
    }

    PNode cur = getRoot();
    int i;
    for (i = 1; i < nodes.length - 1; i++) {
      if (!cur.hasChild(nodes[i])) {
        cur.addChild(nodes[i], new PNode(nodes[i], cur, false));
        addCount++;
      }
      cur = cur.getChild(nodes[i]);
    }
    if (cur.hasChild(nodes[i])) {
      throw new PathErrorException("Path already exists. Path: " + path);
    } else {
      PNode node = new PNode(nodes[i], cur, true);
      cur.addChild(node.getName(), node);
      addCount++;
    }
    return addCount;
  }

  /**
   * Remove a seriesPath from current PTree
   *
   * @throws PathErrorException
   */
  void deletePath(String path) throws PathErrorException {
    String[] nodes = MetaUtils.getNodeNames(path, "\\.");
    if (nodes.length == 0 || !nodes[0].equals(getRoot().getName())) {
      throw new PathErrorException("Path not correct. Path:" + path);
    }
    PNode cur = getRoot();
    for (int i = 1; i < nodes.length; i++) {
      if (!cur.hasChild(nodes[i])) {
        throw new PathErrorException(
            "Path not correct. Node[" + cur.getName() + "] doesn't have child named:" + nodes[i]);
      }
      cur = cur.getChild(nodes[i]);
    }
    cur.getParent().deleteChild(cur.getName());
  }

  /**
   * Link a {@code MNode} to a {@code PNode} in current PTree
   *
   * @throws PathErrorException
   */
  void linkMNode(String pTreePath, String mTreePath) throws PathErrorException {
    List<String> paths = mTree.getAllPathInList(mTreePath);
    String[] nodes = MetaUtils.getNodeNames(pTreePath, "\\.");
    PNode leaf = getLeaf(getRoot(), nodes, 0);
    for (String p : paths) {
      leaf.linkMPath(p);
    }
  }

  /**
   * Unlink a {@code MNode} from a {@code PNode} in current PTree
   *
   * @throws PathErrorException
   */
  void unlinkMNode(String pTreePath, String mTreePath) throws PathErrorException {
    List<String> paths = mTree.getAllPathInList(mTreePath);
    String[] nodes = MetaUtils.getNodeNames(pTreePath, "\\.");
    PNode leaf = getLeaf(getRoot(), nodes, 0);
    for (String p : paths) {
      leaf.unlinkMPath(p);
    }
  }

  private PNode getLeaf(PNode node, String[] nodes, int idx) throws PathErrorException {
    if (idx >= nodes.length) {
      throw new PathErrorException(PTREE_NOT_EXIST);
    }
    if (node.isLeaf()) {
      if (idx != nodes.length - 1 || !nodes[idx].equals(node.getName())) {
        throw new PathErrorException(PTREE_NOT_EXIST);
      }
      return node;
    } else {
      if (idx >= nodes.length - 1 || !node.hasChild(nodes[idx + 1])) {
        throw new PathErrorException(PTREE_NOT_EXIST);
      }
      return getLeaf(node.getChild(nodes[idx + 1]), nodes, idx + 1);
    }
  }

  /**
   *
   * @param path
   *            a seriesPath in current {@code PTree} Get all linked seriesPath in MTree according to the given
   *            seriesPath in PTree
   * @return Paths will be separated by the {@code MNode.dataFileName} in the HashMap
   * @throws PathErrorException
   */
  HashMap<String, List<String>> getAllLinkedPath(String path)
      throws PathErrorException {
    String[] nodes = MetaUtils.getNodeNames(path, "\\.");
    PNode leaf = getLeaf(getRoot(), nodes, 0);
    HashMap<String, List<String>> res = new HashMap<>();

    for (String MPath : leaf.getLinkedMTreePathMap().keySet()) {
      HashMap<String, List<String>> tr = getmTree().getAllPath(MPath);
      mergePathRes(res, tr);
    }
    return res;
  }

  private void mergePathRes(HashMap<String, List<String>> res,
      HashMap<String, List<String>> tr) {
    for (String key : tr.keySet()) {
      if (!res.containsKey(key)) {
        res.put(key, new ArrayList<>());
      }
      for (String p : tr.get(key)) {
        if (!res.get(key).contains(p)) {
          res.get(key).add(p);
        }
      }
    }
  }

  @Override
  public String toString() {
    return pNodeToString(getRoot(), 0);
  }

  private String pNodeToString(PNode node, int tab) {
    StringBuilder s = new StringBuilder();
    for (int i = 0; i < tab; i++) {
      s.append(space);
    }
    s.append(node.getName());
    if (!node.isLeaf() && node.getChildren().size() > 0) {
      s.append(":{\n");
      int first = 0;
      for (PNode child : node.getChildren().values()) {
        if (first == 0) {
          first = 1;
        } else {
          s.append(",\n");
        }
        s.append(pNodeToString(child, tab + 1));
      }
      s.append("\n");
      for (int i = 0; i < tab; i++) {
        s.append(space);
      }
      s.append("}");
    } else if (node.isLeaf()) {
      s.append(":{\n");
      String[] linkedPaths = node.getLinkedMTreePathMap().values().stream().map(
          Object::toString).toArray(String[]::new);
      for (int i = 0; i < linkedPaths.length; i++) {
        if (i != linkedPaths.length - 1) {
          s.append(getTabs(tab + 1)).append(linkedPaths[i]).append(",\n");
        } else {
          s.append(getTabs(tab + 1)).append(linkedPaths[i]).append("\n");
        }
      }
      s.append(getTabs(tab)).append("}");
    }
    return s.toString();
  }

  private String getTabs(int count) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < count; i++) {
      sb.append(space);
    }
    return sb.toString();
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  private MTree getmTree() {
    return mTree;
  }

  private void setmTree(MTree mTree) {
    this.mTree = mTree;
  }

  public PNode getRoot() {
    return root;
  }

  public void setRoot(PNode root) {
    this.root = root;
  }
}
