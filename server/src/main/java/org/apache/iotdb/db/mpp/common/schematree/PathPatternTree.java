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

import org.apache.iotdb.commons.conf.IoTDBConstant;
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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class PathPatternTree {

  private PathPatternNode root;

  private List<PartialPath> pathPatternList;

  public PathPatternTree() {
    this.root = new PathPatternNode(SQLConstant.ROOT);
    this.pathPatternList = new ArrayList<>();
  }

  public PathPatternNode getRoot() {
    return root;
  }

  public void setRoot(PathPatternNode root) {
    this.root = root;
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////
  // Operations for constructing tree
  /////////////////////////////////////////////////////////////////////////////////////////////////

  /** Append a fullPath (without wildcards) as a branch on the tree. */
  public void appendFullPath(PartialPath fullPath) {
    appendBranchWithoutPrune(root, fullPath.getNodes(), 0);
  }

  /** Append a fullPath consisting of device and measurement as a branch on the tree. */
  public void appendFullPath(PartialPath devicePath, String measurement) {
    int deviceNodeLength = devicePath.getNodeLength();
    String[] pathNodes = new String[deviceNodeLength + 1];
    System.arraycopy(devicePath.getNodes(), 0, pathNodes, 0, deviceNodeLength);
    pathNodes[deviceNodeLength] = measurement;

    appendBranchWithoutPrune(root, pathNodes, 0);
  }

  /** Add a pathPattern (may contain wildcards) to pathPatternList. */
  public void appendPathPattern(PartialPath pathPattern) {
    boolean isExist = false;
    for (PartialPath path : pathPatternList) {
      if (path.include(pathPattern)) {
        // path already exists in pathPatternList
        isExist = true;
        break;
      }
    }
    if (!isExist) {
      // remove duplicate path in pathPatternList
      pathPatternList.removeAll(
          pathPatternList.stream().filter(pathPattern::include).collect(Collectors.toList()));
      pathPatternList.add(pathPattern);
    }
  }

  /** Construct tree according to the pathPatternList. */
  public void constructTree() {
    for (PartialPath path : pathPatternList) {
      appendBranchWithoutPrune(root, path.getNodes(), 0);
    }
    pathPatternList.clear();
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
  // Operations for querying tree
  /////////////////////////////////////////////////////////////////////////////////////////////////

  public boolean isEmpty() {
    return (root.getChildren() == null || root.getChildren().isEmpty())
        && (pathPatternList == null || pathPatternList.isEmpty());
  }

  public List<String> getAllDevicePatterns() {
    List<String> nodes = new ArrayList<>();
    Set<String> results = new HashSet<>();
    searchDevicePattern(root, nodes, results);
    return new ArrayList<>(results);
  }

  private void searchDevicePattern(
      PathPatternNode curNode, List<String> nodes, Set<String> results) {
    nodes.add(curNode.getName());
    if (curNode.isLeaf()) {
      if (!curNode.getName().equals(IoTDBConstant.MULTI_LEVEL_PATH_WILDCARD)) {
        results.add(
            nodes.size() == 1 ? "" : convertNodesToString(nodes.subList(0, nodes.size() - 1)));
      } else {
        results.add(convertNodesToString(nodes));
      }
      nodes.remove(nodes.size() - 1);
      return;
    }
    if (curNode.isWildcard()) {
      results.add(convertNodesToString(nodes));
      nodes.remove(nodes.size() - 1);
      return;
    }
    for (PathPatternNode childNode : curNode.getChildren().values()) {
      searchDevicePattern(childNode, nodes, results);
    }
    nodes.remove(nodes.size() - 1);
  }

  public List<PartialPath> getAllPathPatterns() {
    List<PartialPath> result = new ArrayList<>();
    Deque<String> ancestors = new ArrayDeque<>();
    searchPathPattern(root, ancestors, result);
    return result;
  }

  private void searchPathPattern(
      PathPatternNode node, Deque<String> ancestors, List<PartialPath> fullPaths) {
    if (node.isLeaf()) {
      fullPaths.add(convertNodesToPartialPath(node, ancestors));
      return;
    }

    ancestors.push(node.getName());
    for (PathPatternNode child : node.getChildren().values()) {
      searchPathPattern(child, ancestors, fullPaths);
    }
    ancestors.pop();
  }

  public List<PartialPath> getOverlappedPathPatterns(PartialPath pattern) {
    if (pathPatternList.isEmpty()) {
      pathPatternList = getAllPathPatterns();
    }

    List<PartialPath> results = new ArrayList<>();
    for (PartialPath path : pathPatternList) {
      if (pattern.overlapWith(path)) {
        results.add(path);
      }
    }
    return results;
  }

  private String convertNodesToString(List<String> nodes) {
    StringBuilder fullPathBuilder = new StringBuilder(nodes.get(0));
    for (int i = 1; i < nodes.size(); i++) {
      fullPathBuilder.append(TsFileConstant.PATH_SEPARATOR).append(nodes.get(i));
    }
    return fullPathBuilder.toString();
  }

  private PartialPath convertNodesToPartialPath(PathPatternNode node, Deque<String> ancestors) {
    Iterator<String> iterator = ancestors.descendingIterator();
    List<String> nodeList = new ArrayList<>(ancestors.size() + 1);
    while (iterator.hasNext()) {
      nodeList.add(iterator.next());
    }
    nodeList.add(node.getName());
    return new PartialPath(nodeList.toArray(new String[0]));
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
