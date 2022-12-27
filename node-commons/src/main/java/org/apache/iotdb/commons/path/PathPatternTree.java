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

package org.apache.iotdb.commons.path;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.path.PathPatternNode.VoidSerializer;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.utils.PublicBAOS;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

public class PathPatternTree {

  private PathPatternNode<Void, VoidSerializer> root;

  private List<PartialPath> pathPatternList;

  public PathPatternTree() {
    this.root = new PathPatternNode<>(IoTDBConstant.PATH_ROOT, VoidSerializer.getInstance());
    this.pathPatternList = new LinkedList<>();
  }

  public PathPatternNode<Void, VoidSerializer> getRoot() {
    return root;
  }

  public void setRoot(PathPatternNode<Void, VoidSerializer> root) {
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
      pathPatternList.removeIf(pathPattern::include);
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

  private void appendBranchWithoutPrune(
      PathPatternNode<Void, VoidSerializer> curNode, String[] pathNodes, int pos) {
    if (pos == pathNodes.length - 1) {
      curNode.markPathPattern(true);
      return;
    }

    PathPatternNode<Void, VoidSerializer> nextNode = curNode.getChildren(pathNodes[pos + 1]);

    if (nextNode != null) {
      appendBranchWithoutPrune(nextNode, pathNodes, pos + 1);
    } else {
      constructBranch(curNode, pathNodes, pos + 1);
    }
  }

  private void constructBranch(
      PathPatternNode<Void, VoidSerializer> curNode, String[] pathNodes, int pos) {
    for (int i = pos; i < pathNodes.length; i++) {
      PathPatternNode<Void, VoidSerializer> newNode =
          new PathPatternNode<>(pathNodes[i], VoidSerializer.getInstance());
      curNode.addChild(newNode);
      curNode = newNode;
    }
    curNode.markPathPattern(true);
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
      PathPatternNode<Void, VoidSerializer> curNode, List<String> nodes, Set<String> results) {
    nodes.add(curNode.getName());
    if (curNode.isPathPattern()) {
      if (!curNode.getName().equals(IoTDBConstant.MULTI_LEVEL_PATH_WILDCARD)) {
        results.add(
            nodes.size() == 1 ? "" : convertNodesToString(nodes.subList(0, nodes.size() - 1)));
      } else {
        // the device of root.sg.d.** is root.sg.d and root.sg.d.**
        if (nodes.size() > 2) {
          results.add(convertNodesToString(nodes.subList(0, nodes.size() - 1)));
        }
        results.add(convertNodesToString(nodes));
      }
      if (curNode.isLeaf()) {
        nodes.remove(nodes.size() - 1);
        return;
      }
    }
    if (curNode.isWildcard()) {
      results.add(convertNodesToString(nodes));
      nodes.remove(nodes.size() - 1);
      return;
    }
    for (PathPatternNode<Void, VoidSerializer> childNode : curNode.getChildren().values()) {
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
      PathPatternNode<Void, VoidSerializer> node,
      Deque<String> ancestors,
      List<PartialPath> fullPaths) {
    if (node.isPathPattern()) {
      fullPaths.add(convertNodesToPartialPath(node, ancestors));
      if (node.isLeaf()) {
        return;
      }
    }

    ancestors.push(node.getName());
    for (PathPatternNode<Void, VoidSerializer> child : node.getChildren().values()) {
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

  private PartialPath convertNodesToPartialPath(
      PathPatternNode<Void, VoidSerializer> node, Deque<String> ancestors) {
    Iterator<String> iterator = ancestors.descendingIterator();
    List<String> nodeList = new ArrayList<>(ancestors.size() + 1);
    while (iterator.hasNext()) {
      nodeList.add(iterator.next());
    }
    nodeList.add(node.getName());
    return new PartialPath(nodeList.toArray(new String[0]));
  }

  public boolean isOverlapWith(PathPatternTree patternTree) {
    // todo improve this implementation
    for (PartialPath pathPattern : getAllPathPatterns()) {
      if (!patternTree.getOverlappedPathPatterns(pathPattern).isEmpty()) {
        return true;
      }
    }
    return false;
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
    PathPatternNode<Void, VoidSerializer> root =
        PathPatternNode.deserializeNode(buffer, VoidSerializer.getInstance());
    PathPatternTree deserializedPatternTree = new PathPatternTree();
    deserializedPatternTree.setRoot(root);
    return deserializedPatternTree;
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
