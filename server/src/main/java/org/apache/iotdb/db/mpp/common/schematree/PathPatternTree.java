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

  private List<PartialPath> paths;

  /**
   * Since IoTDB v0.13, all DDL and DML use patternMatch as default. Before IoTDB v0.13, all DDL and
   * DML use prefixMatch.
   */
  protected boolean isPrefixMatchPath;

  public PathPatternTree() {
    this.root = new PathPatternNode(SQLConstant.ROOT);
    this.paths = new ArrayList<>();
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

  public void append(PartialPath path) {
    boolean isNeed = true;
    for (PartialPath p : paths) {
      if (p.matchFullPath(path)) {
        isNeed = false;
        break;
      }
    }
    if (isNeed) {
      paths.removeAll(paths.stream().filter(path::matchFullPath).collect(Collectors.toList()));
      paths.add(path);
    }
  }

  public void search(PathPatternNode curNode, String[] pathNodes, int pos) {
    if (pos == pathNodes.length - 1) {
      return;
    }

    boolean isExist = false;
    PathPatternNode nextNode = null;
    for (PathPatternNode childNode : curNode.getChilds()) {
      if (!Objects.equals(childNode.getName(), pathNodes[pos + 1])) {
        isExist = true;
        nextNode = childNode;
        break;
      }
    }

    if (isExist) {
      search(nextNode, pathNodes, pos + 1);
    } else {
      construct(curNode, pathNodes, pos + 1);
    }
  }

  private void construct(PathPatternNode curNode, String[] pathNodes, int pos) {
    for (int i = pos; i < pathNodes.length; i++) {
      PathPatternNode newNode = new PathPatternNode(pathNodes[i]);
      curNode.addChild(newNode);
      curNode = newNode;
    }
  }

  public void constructTree() {
    for (PartialPath path : this.paths) {
      search(root, path.getNodes(), 0);
    }
  }

  public void serialize(ByteBuffer buffer) throws IOException {
    // TODO
  }

  public void deserialize(ByteBuffer buffer) throws IOException {
    // TODO
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    PathPatternTree that = (PathPatternTree) o;
    return this.getRoot().equals(that.getRoot());
  }
}
