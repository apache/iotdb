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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class PathPatternTree {

  private PathPatternNode root;

  private List<PartialPath> pathList;

  public PathPatternTree(PathPatternNode root) {
    this.root = root;
    this.pathList = new ArrayList<>();
  }

  public PathPatternTree(PartialPath devicePath, String[] measurements) {
    this.root = new PathPatternNode(SQLConstant.ROOT);
    this.pathList = new ArrayList<>();
    appendPaths(devicePath, Arrays.asList(measurements));
  }

  public PathPatternTree(PartialPath devicePath, List<String> measurements) {
    this.root = new PathPatternNode(SQLConstant.ROOT);
    this.pathList = new ArrayList<>();
    appendPaths(devicePath, measurements);
  }

  public PathPatternTree(Map<PartialPath, List<String>> deviceToMeasurementsMap) {
    this.root = new PathPatternNode(SQLConstant.ROOT);
    this.pathList = new ArrayList<>();
    for (Map.Entry<PartialPath, List<String>> entry : deviceToMeasurementsMap.entrySet()) {
      appendPaths(entry.getKey(), entry.getValue());
    }
  }

  public PathPatternTree(List<PartialPath> pathList) {
    this.root = new PathPatternNode(SQLConstant.ROOT);
    this.pathList = new ArrayList<>();
    for (PartialPath path : pathList) {
      appendPath(path);
    }
  }

  public PathPatternTree(PartialPath fullPath) {
    this.root = new PathPatternNode(SQLConstant.ROOT);
    this.pathList = new ArrayList<>();
    appendPath(fullPath);
  }

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

  /** @return all device path patterns in the path pattern tree. */
  public List<String> findAllDevicePaths() {
    if (root.getChildren().isEmpty()) {
      constructTree();
    }
    List<String> nodes = new ArrayList<>();
    List<String> pathPatternList = new ArrayList<>();
    findAllDevicePaths(root, nodes, pathPatternList);
    return pathPatternList;
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

  // append path to pathList
  public void appendPath(PartialPath newPath) {
    boolean isExist = false;
    for (PartialPath path : pathList) {
      if (path.matchFullPath(newPath)) {
        // path already exists in pathList
        isExist = true;
        break;
      }
    }
    if (!isExist) {
      // remove duplicate path in pathList
      pathList.removeAll(
          pathList.stream().filter(newPath::matchFullPath).collect(Collectors.toList()));
      pathList.add(newPath);
    }
  }

  public void appendPaths(List<PartialPath> paths) {
    for (PartialPath path : paths) {
      appendPath(path);
    }
  }

  public void appendPaths(PartialPath device, List<String> measurementNameList) {
    for (String measurementName : measurementNameList) {
      appendPath(device.concatNode(measurementName));
    }
  }

  // construct tree according to pathList
  public void constructTree() {
    for (PartialPath path : pathList) {
      searchAndConstruct(root, path.getNodes(), 0);
    }
    pathList.clear();
  }

  private void searchAndConstruct(PathPatternNode curNode, String[] pathNodes, int pos) {
    if (pos == pathNodes.length - 1) {
      return;
    }

    PathPatternNode nextNode = curNode.getChildren(pathNodes[pos + 1]);

    if (nextNode != null) {
      searchAndConstruct(nextNode, pathNodes, pos + 1);
    } else {
      appendTree(curNode, pathNodes, pos + 1);
    }
  }

  private void appendTree(PathPatternNode curNode, String[] pathNodes, int pos) {
    for (int i = pos; i < pathNodes.length; i++) {
      PathPatternNode newNode = new PathPatternNode(pathNodes[i]);
      curNode.addChild(newNode);
      curNode = newNode;
    }
  }

  public void serialize(PublicBAOS outputStream) throws IOException {
    constructTree();
    root.serialize(outputStream);
  }

  public void serialize(ByteBuffer buffer) {
    constructTree();
    root.serialize(buffer);
  }

  public static PathPatternTree deserialize(ByteBuffer buffer) {
    PathPatternNode root = deserializeNode(buffer);
    return new PathPatternTree(root);
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

  public List<PartialPath> splitToPathList() {
    List<PartialPath> result = new ArrayList<>();
    Deque<String> ancestors = new ArrayDeque<>();
    searchFullPath(root, ancestors, result);
    return result;
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

  public PathPatternTree findOverlappedPattern(PartialPath pattern) {
    return new PathPatternTree(findOverlappedPaths(pattern));
  }

  public List<PartialPath> findOverlappedPaths(PartialPath pattern) {
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

  public boolean isEmpty() {
    return (root.getChildren() == null || root.getChildren().isEmpty())
        && (pathList == null || pathList.isEmpty());
  }
}
