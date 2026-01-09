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

package org.apache.iotdb.db.queryengine.plan.relational.sql.ast;

import com.google.common.collect.ImmutableList;
import org.apache.tsfile.utils.RamUsageEstimator;

import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;

public class ShowExternalService extends Statement {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(ShowExternalService.class);

  // -1 means show services on all DNs
  private int dataNodeId;

  public ShowExternalService(NodeLocation location, int dataNodeId) {
    super(location);
    this.dataNodeId = dataNodeId;
  }

  public int getDataNodeId() {
    return dataNodeId;
  }

  @Override
  public List<? extends Node> getChildren() {
    return ImmutableList.of();
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitShowExternalService(this, context);
  }

  @Override
  public int hashCode() {
    return 0;
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof ShowExternalService)) {
      return false;
    }
    return dataNodeId == ((ShowExternalService) obj).getDataNodeId();
  }

  @Override
  public String toString() {
    return toStringHelper(this).toString();
  }

  @Override
  public long ramBytesUsed() {
    long size = INSTANCE_SIZE;
    size += AstMemoryEstimationHelper.getEstimatedSizeOfNodeLocation(getLocationInternal());
    size += Integer.SIZE;
    return size;
  }
}
