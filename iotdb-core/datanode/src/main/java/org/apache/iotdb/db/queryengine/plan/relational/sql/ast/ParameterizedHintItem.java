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

/** Represents a parameterized hint, e.g., "LEADER(table1)" or "FOLLOWER(table2)". */
public class ParameterizedHintItem extends Node {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(ParameterizedHintItem.class);

  private final String hintName;
  private final List<String> parameters;

  public ParameterizedHintItem(String hintName, List<String> parameters) {
    super(null);
    this.hintName = hintName.toUpperCase();
    this.parameters = ImmutableList.copyOf(parameters);
  }

  public String getHintName() {
    return hintName;
  }

  public List<String> getParameters() {
    return parameters;
  }

  @Override
  public List<? extends Node> getChildren() {
    return ImmutableList.of();
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitParameterizedHintItem(this, context);
  }

  @Override
  public int hashCode() {
    return Objects.hash(hintName, parameters);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    ParameterizedHintItem other = (ParameterizedHintItem) obj;
    return Objects.equals(this.hintName, other.hintName)
        && Objects.equals(this.parameters, other.parameters);
  }

  @Override
  public String toString() {
    return hintName + "(" + String.join(", ", parameters) + ")";
  }

  @Override
  public long ramBytesUsed() {
    long size = INSTANCE_SIZE;
    size += AstMemoryEstimationHelper.getEstimatedSizeOfNodeLocation(getLocationInternal());
    size += AstMemoryEstimationHelper.getEstimatedSizeOfStringList(parameters);
    size += RamUsageEstimator.sizeOf(hintName);
    return size;
  }
}
