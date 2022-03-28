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
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.qp.constant.SQLConstant;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class PathPatternTree {

  private PathPatternNode root;

  private final List<PartialPath> pathList;

  /**
   * Since IoTDB v0.13, all DDL and DML use patternMatch as default. Before IoTDB v0.13, all DDL and
   * DML use prefixMatch.
   */
  protected boolean isPrefixMatchPath;

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

    boolean isExist = false;
    PathPatternNode nextNode = null;
    for (PathPatternNode childNode : curNode.getChildren()) {
      if (!Objects.equals(childNode.getName(), pathNodes[pos + 1])) {
        isExist = true;
        nextNode = childNode;
        break;
      }
    }

    if (isExist) {
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
    // TODO
  }

  public void deserialize(ByteBuffer buffer) throws IOException {
    // TODO
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
