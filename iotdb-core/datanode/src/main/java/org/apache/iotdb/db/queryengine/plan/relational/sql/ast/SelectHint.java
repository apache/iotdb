/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package org.apache.iotdb.db.queryengine.plan.relational.sql.ast;

import com.google.common.collect.ImmutableList;
import org.apache.tsfile.utils.RamUsageEstimator;

import java.util.List;
import java.util.Objects;

public class SelectHint extends Node {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(SelectHint.class);

  private final List<Node> hintItems;

  public SelectHint(List<Node> hintItems) {
    super(null);
    this.hintItems = ImmutableList.copyOf(hintItems);
  }

  public List<Node> getHintItems() {
    return hintItems;
  }

  @Override
  public List<? extends Node> getChildren() {
    return hintItems;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitSelectHint(this, context);
  }

  @Override
  public int hashCode() {
    return Objects.hash(hintItems);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    SelectHint other = (SelectHint) obj;
    return Objects.equals(this.hintItems, other.hintItems);
  }

  @Override
  public String toString() {
    if (hintItems == null || hintItems.isEmpty()) {
      return "";
    }

    StringBuilder sb = new StringBuilder();
    sb.append("/*+ ");

    for (int i = 0; i < hintItems.size(); i++) {
      if (i > 0) {
        sb.append(" ");
      }
      sb.append(hintItems.get(i).toString());
    }

    sb.append(" */");
    return sb.toString();
  }

  @Override
  public long ramBytesUsed() {
    long size = INSTANCE_SIZE;
    size += AstMemoryEstimationHelper.getEstimatedSizeOfNodeLocation(getLocationInternal());
    size += AstMemoryEstimationHelper.getEstimatedSizeOfNodeList(hintItems);
    return size;
  }
}
