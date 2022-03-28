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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class PathPatternNode {

  private String name;
  private List<PathPatternNode> children;

  public PathPatternNode(String name) {
    this.name = name;
    this.children = new ArrayList<>();
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public List<PathPatternNode> getChildren() {
    return children;
  }

  public void setChildren(List<PathPatternNode> children) {
    this.children = children;
  }

  public void addChild(PathPatternNode newNode) {
    this.children.add(newNode);
  }

  public boolean isLeaf() {
    return children.isEmpty();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    PathPatternNode that = (PathPatternNode) o;
    if (!Objects.equals(that.getName(), this.getName())) {
      return false;
    }
    if (that.isLeaf() && this.isLeaf()) {
      return true;
    }
    if (that.getChildren().size() != this.getChildren().size()) {
      return false;
    }
    for (int i = 0; i < this.getChildren().size(); i++) {
      if (!that.getChildren().get(i).equals(this.getChildren().get(i))) {
        return false;
      }
    }
    return true;
  }
}
