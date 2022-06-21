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
import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class PathPatternNode {

  private final String name;
  private final Map<String, PathPatternNode> children;

  public PathPatternNode(String name) {
    this.name = name;
    this.children = new HashMap<>();
  }

  public String getName() {
    return name;
  }

  public PathPatternNode getChildren(String nodeName) {
    return children.getOrDefault(nodeName, null);
  }

  public Map<String, PathPatternNode> getChildren() {
    return children;
  }

  public void addChild(PathPatternNode tmpNode) {
    children.put(tmpNode.getName(), tmpNode);
  }

  public boolean isLeaf() {
    return children.isEmpty();
  }

  public boolean isWildcard() {
    return name.equals("*") || name.equals("**");
  }

  @TestOnly
  public boolean equalWith(PathPatternNode that) {
    if (this == that) {
      return true;
    }
    if (that == null || getClass() != that.getClass()) {
      return false;
    }
    if (!Objects.equals(that.getName(), this.getName())) {
      return false;
    }
    if (that.isLeaf() && this.isLeaf()) {
      return true;
    }
    if (that.getChildren().size() != this.getChildren().size()) {
      return false;
    }
    for (Map.Entry<String, PathPatternNode> entry : this.getChildren().entrySet()) {
      String nodeName = entry.getKey();
      if (that.getChildren(nodeName) == null
          || !that.getChildren(nodeName).equalWith(this.getChildren(nodeName))) {
        return false;
      }
    }
    return true;
  }

  public void serialize(ByteBuffer buffer) {
    ReadWriteIOUtils.write(name, buffer);
    ReadWriteIOUtils.write(children.size(), buffer);
    serializeChildren(buffer);
  }

  void serializeChildren(ByteBuffer buffer) {
    for (PathPatternNode childNode : children.values()) {
      childNode.serialize(buffer);
    }
  }

  public void serialize(PublicBAOS outputStream) throws IOException {
    ReadWriteIOUtils.write(name, outputStream);
    ReadWriteIOUtils.write(children.size(), outputStream);
    serializeChildren(outputStream);
  }

  public void serialize(DataOutputStream outputStream) throws IOException {
    ReadWriteIOUtils.write(name, outputStream);
    ReadWriteIOUtils.write(children.size(), outputStream);
    serializeChildren(outputStream);
  }

  void serializeChildren(PublicBAOS outputStream) throws IOException {
    for (PathPatternNode childNode : children.values()) {
      childNode.serialize(outputStream);
    }
  }

  void serializeChildren(DataOutputStream outputStream) throws IOException {
    for (PathPatternNode childNode : children.values()) {
      childNode.serialize(outputStream);
    }
  }
}
