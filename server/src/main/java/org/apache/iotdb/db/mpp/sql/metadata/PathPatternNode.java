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

package org.apache.iotdb.db.mpp.sql.metadata;

import java.util.ArrayList;
import java.util.List;

public class PathPatternNode {

  private String pathPattern;

  private boolean isSelected;
  private boolean isValueFiltered;
  private boolean isNullFiltered;

  private List<PathPatternNode> childs = new ArrayList<>();

  public PathPatternNode(String pathPattern) {
    this.pathPattern = pathPattern;
  }

  public String getPathPattern() {
    return pathPattern;
  }

  public void setPathPattern(String pathPattern) {
    this.pathPattern = pathPattern;
  }

  public boolean isSelected() {
    return isSelected;
  }

  public void setSelected(boolean selected) {
    isSelected = selected;
  }

  public boolean isValueFiltered() {
    return isValueFiltered;
  }

  public void setValueFiltered(boolean valueFiltered) {
    isValueFiltered = valueFiltered;
  }

  public boolean isNullFiltered() {
    return isNullFiltered;
  }

  public void setNullFiltered(boolean nullFiltered) {
    isNullFiltered = nullFiltered;
  }

  public List<PathPatternNode> getChilds() {
    return childs;
  }

  public void setChilds(List<PathPatternNode> childs) {
    this.childs = childs;
  }

  public void addChild(PathPatternNode node) {
    this.childs.add(node);
  }

  public boolean isLeaf() {
    return childs.isEmpty();
  }
}
