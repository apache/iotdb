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

import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.qp.constant.SQLConstant;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class PathPatternTree {

  private PathPatternNode root;

  private final List<PartialPath> pathList;

  /**
   * Since IoTDB v0.13, all DDL and DML use patternMatch as default. Before IoTDB v0.13, all DDL and
   * DML use prefixMatch.
   */
  protected boolean isPrefixMatchPath;

  public PathPatternTree(PartialPath deivcePath, String[] measurements) {
    this.root = new PathPatternNode(SQLConstant.ROOT);
    this.pathList = new ArrayList<>();
    appendPaths(deivcePath, Arrays.asList(measurements));
  }

  public PathPatternTree(Map<PartialPath, List<String>> deviceToMeasurementsMap) {
    this.root = new PathPatternNode(SQLConstant.ROOT);
    this.pathList = new ArrayList<>();
    for (Map.Entry<PartialPath, List<String>> entry : deviceToMeasurementsMap.entrySet()) {
      appendPaths(entry.getKey(), entry.getValue());
    }
  };

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

  public boolean isPrefixMatchPath() {
    return isPrefixMatchPath;
  }

  public void setPrefixMatchPath(boolean prefixMatchPath) {
    isPrefixMatchPath = prefixMatchPath;
  }

  /** @return all path patterns in the path pattern tree. */
  public List<String> findAllPaths() {
    List<String> nodes = new ArrayList<>();
    List<String> pathPatternList = new ArrayList<>();
    findAllPaths(root, nodes, pathPatternList);
    return pathPatternList;
  }

  private void findAllPaths(
      PathPatternNode curNode, List<String> nodes, List<String> pathPatternList) {
    nodes.add(curNode.getName());
    if (curNode.isLeaf()) {
      pathPatternList.add(parseNodesToString(nodes));
    }
    for (PathPatternNode childNode : curNode.getChildren().values()) {
      findAllPaths(childNode, nodes, pathPatternList);
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

  public void appendPaths(PartialPath device, List<String> measurementNameList) {
    try {
      for (String measurementName : measurementNameList) {
        appendPath(new PartialPath(device.getFullPath(), measurementName));
      }
    } catch (IllegalPathException e) {
      e.printStackTrace();
    }
  }

  // construct tree according to pathList
  public void constructTree() {
    for (PartialPath path : pathList) {
      searchAndConstruct(root, path.getNodes(), 0);
    }
    pathList.clear();
  }

  public void searchAndConstruct(PathPatternNode curNode, String[] pathNodes, int pos) {
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

  public void serialize(ByteBuffer buffer) throws IOException {
    constructTree();
    ReadWriteIOUtils.write(isPrefixMatchPath, buffer);
    root.serialize(buffer);
  }

  public void deserialize(ByteBuffer buffer) throws IOException {
    this.isPrefixMatchPath = ReadWriteIOUtils.readBool(buffer);
    this.root = deserializeNode(buffer);
  }

  private PathPatternNode deserializeNode(ByteBuffer buffer) {
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
