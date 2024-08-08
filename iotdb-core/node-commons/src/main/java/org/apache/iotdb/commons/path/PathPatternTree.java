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

import org.apache.tsfile.common.constant.TsFileConstant;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.IDeviceID.Factory;
import org.apache.tsfile.utils.PublicBAOS;

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
import java.util.Objects;
import java.util.Set;

public class PathPatternTree {

  private PathPatternNode<Void, VoidSerializer> root;

  private List<PartialPath> pathPatternList;

  // set the default value to TRUE to ensure correctness
  private boolean useWildcard = true;
  private boolean containWildcard = false;
  private boolean containFullPath = false;

  public PathPatternTree(boolean useWildcard) {
    this();
    this.useWildcard = useWildcard;
  }

  public PathPatternTree() {
    this.root = new PathPatternNode<>(IoTDBConstant.PATH_ROOT, VoidSerializer.getInstance());
    this.pathPatternList = new LinkedList<>();
  }

  public boolean isContainWildcard() {
    return containWildcard;
  }

  public boolean isContainFullPath() {
    return containFullPath;
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
    if (useWildcard) {
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
    } else {
      appendBranchWithoutPrune(root, pathPattern.getNodes(), 0);
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
      processNodeName(pathNodes[i]);
      curNode = newNode;
    }
    curNode.markPathPattern(true);
  }

  private void processNodeName(String nodeName) {
    if (!containWildcard) {
      containWildcard = PathPatternUtil.hasWildcard(nodeName);
    }
    if (!containFullPath) {
      containFullPath = !PathPatternUtil.hasWildcard(nodeName);
    }
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////
  // Operations for querying tree
  /////////////////////////////////////////////////////////////////////////////////////////////////

  public boolean isEmpty() {
    return (root.getChildren() == null || root.getChildren().isEmpty())
        && (pathPatternList == null || pathPatternList.isEmpty());
  }

  public List<IDeviceID> getAllDevicePatterns() {
    List<String> nodes = new ArrayList<>();
    Set<List<String>> resultNodesSet = new HashSet<>();
    searchDevicePath(root, nodes, resultNodesSet);

    Set<IDeviceID> resultPaths = new HashSet<>();
    for (List<String> resultNodes : resultNodesSet) {
      if (resultNodes != null && !resultNodes.isEmpty()) {
        resultPaths.add(Factory.DEFAULT_FACTORY.create(resultNodes.toArray(new String[0])));
      }
    }

    return new ArrayList<>(resultPaths);
  }

  public List<PartialPath> getAllDevicePaths() {
    List<String> nodes = new ArrayList<>();
    Set<List<String>> resultNodesSet = new HashSet<>();
    searchDevicePath(root, nodes, resultNodesSet);

    Set<PartialPath> resultPaths = new HashSet<>();
    for (List<String> resultNodes : resultNodesSet) {
      resultPaths.add(new PartialPath(resultNodes.toArray(new String[0])));
    }
    return new ArrayList<>(resultPaths);
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
    for (PathPatternNode<Void, VoidSerializer> childNode : curNode.getChildren().values()) {
      searchDevicePattern(childNode, nodes, results);
    }
    nodes.remove(nodes.size() - 1);
  }

  private void searchDevicePath(
      PathPatternNode<Void, VoidSerializer> curNode,
      List<String> nodes,
      Set<List<String>> resultNodesSet) {
    nodes.add(curNode.getName());
    if (curNode.isPathPattern()) {
      if (!curNode.getName().equals(IoTDBConstant.MULTI_LEVEL_PATH_WILDCARD)) {
        resultNodesSet.add(
            nodes.size() == 1
                ? new ArrayList<>()
                : new ArrayList<>(nodes.subList(0, nodes.size() - 1)));
      } else {
        // the device of root.sg.d.** is root.sg.d and root.sg.d.**
        if (nodes.size() > 2) {
          resultNodesSet.add(new ArrayList<>(nodes.subList(0, nodes.size() - 1)));
        }
        resultNodesSet.add(new ArrayList<>(nodes));
      }
      if (curNode.isLeaf()) {
        nodes.remove(nodes.size() - 1);
        return;
      }
    }
    for (PathPatternNode<Void, VoidSerializer> childNode : curNode.getChildren().values()) {
      searchDevicePath(childNode, nodes, resultNodesSet);
    }
    nodes.remove(nodes.size() - 1);
  }

  public List<PartialPath> getAllPathPatterns() {
    List<PartialPath> result = new ArrayList<>();
    Deque<String> ancestors = new ArrayDeque<>();
    searchPathPattern(root, ancestors, result, false);
    return result;
  }

  public List<MeasurementPath> getAllPathPatterns(boolean asMeasurementPath) {
    List<MeasurementPath> result = new ArrayList<>();
    Deque<String> ancestors = new ArrayDeque<>();
    searchPathPattern(root, ancestors, result, asMeasurementPath);
    return result;
  }

  private <T extends PartialPath> void searchPathPattern(
      PathPatternNode<Void, VoidSerializer> node,
      Deque<String> ancestors,
      List<T> fullPaths,
      boolean asMeasurementPath) {
    if (node.isPathPattern()) {
      fullPaths.add((T) convertNodesToPartialPath(node, ancestors, asMeasurementPath));
      if (node.isLeaf()) {
        return;
      }
    }

    ancestors.push(node.getName());
    for (PathPatternNode<Void, VoidSerializer> child : node.getChildren().values()) {
      searchPathPattern(child, ancestors, fullPaths, asMeasurementPath);
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
      PathPatternNode<Void, VoidSerializer> node,
      Deque<String> ancestors,
      boolean asMeasurementPath) {
    Iterator<String> iterator = ancestors.descendingIterator();
    List<String> nodeList = new ArrayList<>(ancestors.size() + 1);
    while (iterator.hasNext()) {
      nodeList.add(iterator.next());
    }
    nodeList.add(node.getName());
    if (asMeasurementPath) {
      return new MeasurementPath(nodeList.toArray(new String[0]));
    } else {
      return new PartialPath(nodeList.toArray(new String[0]));
    }
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

  public PathPatternTree intersectWithFullPathPrefixTree(PathPatternTree fullPathPrefixTree) {
    PathPatternTree result = new PathPatternTree(containWildcard);
    List<PartialPath> partialPathList = null;

    for (PartialPath fullPathOrPrefix : fullPathPrefixTree.getAllPathPatterns()) {
      PathPatternNode<Void, VoidSerializer> curNode = root;
      PathPatternNode<Void, VoidSerializer> tarNode = result.root;
      String[] nodes = fullPathOrPrefix.nodes;
      boolean done = false;
      if (fullPathOrPrefix.endWithMultiLevelWildcard()) {
        // if prefix match, directly construct result tree
        for (int i = 1; i < nodes.length - 1; i++) {
          done = true;
          List<PathPatternNode<Void, VoidSerializer>> tmp = curNode.getMatchChildren(nodes[i]);
          if (tmp.size() == 1 && tmp.get(0).getName().equals(nodes[i])) {
            curNode = tmp.get(0);
            if (tarNode.getChildren(nodes[i]) == null) {
              tarNode.addChild(new PathPatternNode<>(nodes[i], VoidSerializer.getInstance()));
            }
            tarNode = tarNode.getChildren(nodes[i]);
          } else {
            done = false;
            break;
          }
        }
      }
      if (done) {
        for (PathPatternNode<Void, VoidSerializer> node : curNode.getChildren().values()) {
          tarNode.addChild(node);
        }
      } else {
        // this branch is to construct intersection one by one
        if (partialPathList == null) {
          partialPathList = getAllPathPatterns();
        }
        for (PartialPath pathPattern : partialPathList) {
          if (fullPathOrPrefix.endWithMultiLevelWildcard()) {
            // prefix
            for (PartialPath temp : pathPattern.intersectWithPrefixPattern(fullPathOrPrefix)) {
              result.appendPathPattern(temp);
            }
          } else {
            // full path
            if (pathPattern.matchFullPath(fullPathOrPrefix)) {
              result.appendPathPattern(fullPathOrPrefix);
            }
          }
        }
      }
    }
    result.constructTree();
    return result;
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

  public ByteBuffer serialize() throws IOException {
    PublicBAOS baos = new PublicBAOS();
    serialize(baos);
    ByteBuffer serializedPatternTree = ByteBuffer.allocate(baos.size());
    serializedPatternTree.put(baos.getBuf(), 0, baos.size());
    serializedPatternTree.flip();
    return serializedPatternTree;
  }

  public static PathPatternTree deserialize(ByteBuffer buffer) {
    PathPatternTree deserializedPatternTree = new PathPatternTree();
    PathPatternNode<Void, VoidSerializer> root =
        PathPatternNode.deserializeNode(
            buffer, VoidSerializer.getInstance(), deserializedPatternTree::processNodeName);
    deserializedPatternTree.setRoot(root);
    return deserializedPatternTree;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    PathPatternTree that = (PathPatternTree) o;
    return Objects.equals(root, that.root);
  }

  @Override
  public int hashCode() {
    return Objects.hash(root);
  }
}
