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

package org.apache.iotdb.db.mpp.common.schematree;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.qp.constant.SQLConstant;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

public class PathPatternTree {

  private PathPatternNode root;

  @Deprecated private List<PartialPath> pathList;

  public PathPatternTree() {
    this.root = new PathPatternNode(SQLConstant.ROOT);
    this.pathList = new ArrayList<>();
  }

  public PathPatternNode getRoot() {
    return root;
  }

  public void setRoot(PathPatternNode root) {
    this.root = root;
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////
  // Operations for time series paths
  /////////////////////////////////////////////////////////////////////////////////////////////////

  /** @param fullPath */
  public void appendFullPath(PartialPath fullPath) {
    appendBranchWithoutPrune(root, fullPath.getNodes(), 0);
  }

  /**
   * @param devicePath
   * @param measurement
   */
  public void appendFullPath(PartialPath devicePath, String measurement) {
    int deviceNodeLength = devicePath.getNodeLength();
    String[] pathNodes = new String[deviceNodeLength + 1];
    System.arraycopy(devicePath.getNodes(), 0, pathNodes, 0, deviceNodeLength);
    pathNodes[deviceNodeLength] = measurement;

    appendBranchWithoutPrune(root, pathNodes, 0);
  }

  /** @param pathPattern */
  public void appendPathPattern(PartialPath pathPattern) {
    boolean isExist = false;
    for (PartialPath path : pathList) {
      if (path.matchFullPath(pathPattern)) {
        // path already exists in pathList
        isExist = true;
        break;
      }
    }
    if (!isExist) {
      // remove duplicate path in pathList
      pathList.removeAll(
          pathList.stream().filter(pathPattern::matchFullPath).collect(Collectors.toList()));
      pathList.add(pathPattern);
    }
  }

  /** */
  @Deprecated
  public void constructTree() {
    for (PartialPath path : pathList) {
      appendBranchWithoutPrune(root, path.getNodes(), 0);
    }
    pathList.clear();
  }

  private void appendBranchWithoutPrune(PathPatternNode curNode, String[] pathNodes, int pos) {
    if (pos == pathNodes.length - 1) {
      return;
    }

    PathPatternNode nextNode = curNode.getChildren(pathNodes[pos + 1]);

    if (nextNode != null) {
      appendBranchWithoutPrune(nextNode, pathNodes, pos + 1);
    } else {
      constructBranch(curNode, pathNodes, pos + 1);
    }
  }

  private void constructBranch(PathPatternNode curNode, String[] pathNodes, int pos) {
    for (int i = pos; i < pathNodes.length; i++) {
      PathPatternNode newNode = new PathPatternNode(pathNodes[i]);
      curNode.addChild(newNode);
      curNode = newNode;
    }
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////
  // Operations for time series paths
  /////////////////////////////////////////////////////////////////////////////////////////////////

  public boolean isEmpty() {
    return (root.getChildren() == null || root.getChildren().isEmpty())
        && (pathList == null || pathList.isEmpty());
  }

  /** @return */
  public List<String> findAllDevicePaths() {
    List<String> nodes = new ArrayList<>();
    List<String> pathPatternList = new ArrayList<>();
    findAllDevicePaths(root, nodes, pathPatternList);
    return pathPatternList;
  }

  /** @return */
  public List<PartialPath> splitToPathList() {
    List<PartialPath> result = new ArrayList<>();
    Deque<String> ancestors = new ArrayDeque<>();
    searchFullPath(root, ancestors, result);
    return result;
  }

  /** @return */
  public PathPatternTree findOverlappedPattern(PartialPath pattern) {
    PathPatternTree patternTree = new PathPatternTree();
    for (PartialPath pathPattern : findOverlappedPaths(pattern)) {
      patternTree.appendPathPattern(pathPattern);
    }
    return patternTree;
  }

  private void findAllDevicePaths(
      PathPatternNode curNode, List<String> nodes, List<String> pathPatternList) {
    nodes.add(curNode.getName());
    if (curNode.isLeaf()) {
      if (!curNode.getName().equals("**")) {
        pathPatternList.add(parseNodesToString(nodes.subList(0, nodes.size() - 1)));
      } else {
        pathPatternList.add(parseNodesToString(nodes));
      }
      nodes.remove(nodes.size() - 1);
      return;
    }
    if (curNode.isWildcard()) {
      pathPatternList.add(parseNodesToString(nodes));
      nodes.remove(nodes.size() - 1);
      return;
    }
    for (PathPatternNode childNode : curNode.getChildren().values()) {
      findAllDevicePaths(childNode, nodes, pathPatternList);
    }
    nodes.remove(nodes.size() - 1);
  }

  private String parseNodesToString(List<String> nodes) {
    StringBuilder fullPathBuilder = new StringBuilder(nodes.get(0));
    for (int i = 1; i < nodes.size(); i++) {
      fullPathBuilder.append(TsFileConstant.PATH_SEPARATOR).append(nodes.get(i));
    }
    return fullPathBuilder.toString();
  }

  private void searchFullPath(
      PathPatternNode node, Deque<String> ancestors, List<PartialPath> fullPaths) {
    if (node.isLeaf()) {
      fullPaths.add(constructFullPath(node, ancestors));
      return;
    }

    ancestors.push(node.getName());
    for (PathPatternNode child : node.getChildren().values()) {
      searchFullPath(child, ancestors, fullPaths);
    }
    ancestors.pop();
  }

  private PartialPath constructFullPath(PathPatternNode node, Deque<String> ancestors) {
    Iterator<String> iterator = ancestors.descendingIterator();
    List<String> nodeList = new ArrayList<>(ancestors.size() + 1);
    while (iterator.hasNext()) {
      nodeList.add(iterator.next());
    }
    nodeList.add(node.getName());
    return new PartialPath(nodeList.toArray(new String[0]));
  }

  private List<PartialPath> findOverlappedPaths(PartialPath pattern) {
    if (pathList.isEmpty()) {
      pathList = splitToPathList();
    }

    List<PartialPath> results = new ArrayList<>();
    for (PartialPath path : pathList) {
      if (pattern.overlapWith(path)) {
        results.add(path);
      }
    }
    return results;
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////
  // serialize & deserialize
  /////////////////////////////////////////////////////////////////////////////////////////////////

  public void serialize(PublicBAOS outputStream) throws IOException {
    constructTree();
    root.serialize(outputStream);
  }

  public void serialize(DataOutputStream stream) throws IOException {
    constructTree();
    root.serialize(stream);
  }

  public void serialize(ByteBuffer buffer) {
    constructTree();
    root.serialize(buffer);
  }

  public static PathPatternTree deserialize(ByteBuffer buffer) {
    PathPatternNode root = deserializeNode(buffer);
    PathPatternTree deserializedPatternTree = new PathPatternTree();
    deserializedPatternTree.setRoot(root);
    return deserializedPatternTree;
  }

  private static PathPatternNode deserializeNode(ByteBuffer buffer) {
    PathPatternNode node = new PathPatternNode(ReadWriteIOUtils.readString(buffer));
    int childrenSize = ReadWriteIOUtils.readInt(buffer);
    while (childrenSize > 0) {
      PathPatternNode tmpNode = deserializeNode(buffer);
      node.addChild(tmpNode);
      childrenSize--;
    }
    return node;
  }

  @TestOnly
  public boolean equalWith(PathPatternTree that) {
    if (this == that) {
      return true;
    }
    if (that == null || getClass() != that.getClass()) {
      return false;
    }
    return this.getRoot().equalWith(that.getRoot());
  }
}
