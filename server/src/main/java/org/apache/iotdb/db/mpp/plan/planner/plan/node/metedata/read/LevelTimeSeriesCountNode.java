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

package org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.read;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.mpp.common.header.HeaderConstant;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;

public class LevelTimeSeriesCountNode extends SchemaQueryScanNode {
  private final int level;

  public LevelTimeSeriesCountNode(
      PlanNodeId id, PartialPath partialPath, boolean isPrefixPath, int level) {
    super(id, partialPath, isPrefixPath);
    this.level = level;
  }

  public int getLevel() {
    return level;
  }

  @Override
  public PlanNode clone() {
    return new LevelTimeSeriesCountNode(getPlanNodeId(), path, isPrefixPath, level);
  }

  @Override
  public List<String> getOutputColumnNames() {
    return HeaderConstant.countLevelTimeSeriesHeader.getRespColumns();
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.LEVEL_TIME_SERIES_COUNT.serialize(byteBuffer);
    ReadWriteIOUtils.write(path.getFullPath(), byteBuffer);
    ReadWriteIOUtils.write(isPrefixPath, byteBuffer);
    ReadWriteIOUtils.write(level, byteBuffer);
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.LEVEL_TIME_SERIES_COUNT.serialize(stream);
    ReadWriteIOUtils.write(path.getFullPath(), stream);
    ReadWriteIOUtils.write(isPrefixPath, stream);
    ReadWriteIOUtils.write(level, stream);
  }

  public static PlanNode deserialize(ByteBuffer buffer) {
    String fullPath = ReadWriteIOUtils.readString(buffer);
    PartialPath path;
    try {
      path = new PartialPath(fullPath);
    } catch (IllegalPathException e) {
      throw new IllegalArgumentException("Cannot deserialize DevicesSchemaScanNode", e);
    }
    boolean isPrefixPath = ReadWriteIOUtils.readBool(buffer);
    int level = ReadWriteIOUtils.readInt(buffer);
    PlanNodeId planNodeId = PlanNodeId.deserialize(buffer);
    return new LevelTimeSeriesCountNode(planNodeId, path, isPrefixPath, level);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    LevelTimeSeriesCountNode that = (LevelTimeSeriesCountNode) o;
    return level == that.level;
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), level);
  }
}
