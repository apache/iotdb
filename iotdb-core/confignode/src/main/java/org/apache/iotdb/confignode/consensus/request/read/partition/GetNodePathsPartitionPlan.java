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

package org.apache.iotdb.confignode.consensus.request.read.partition;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;
import org.apache.iotdb.confignode.consensus.request.read.ConfigPhysicalReadPlan;

import java.util.Objects;

public class GetNodePathsPartitionPlan extends ConfigPhysicalReadPlan {
  private PartialPath partialPath;
  private PathPatternTree scope;
  private int level = -1;

  public GetNodePathsPartitionPlan() {
    super(ConfigPhysicalPlanType.GetNodePathsPartition);
  }

  public PathPatternTree getScope() {
    return scope;
  }

  public void setScope(final PathPatternTree scope) {
    this.scope = scope;
  }

  public PartialPath getPartialPath() {
    return partialPath;
  }

  public void setPartialPath(final PartialPath partialPath) {
    this.partialPath = partialPath;
  }

  public int getLevel() {
    return level;
  }

  public void setLevel(final int level) {
    this.level = level;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final GetNodePathsPartitionPlan that = (GetNodePathsPartitionPlan) o;
    return level == that.level && Objects.equals(partialPath, that.partialPath);
  }

  @Override
  public int hashCode() {
    return Objects.hash(partialPath, level);
  }
}
