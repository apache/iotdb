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
package org.apache.iotdb.db.mpp.plan.planner.plan.node.process;

import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;

/**
 * OffsetNode is used to skip top n result from upstream nodes. It uses the default order of
 * upstream nodes
 */
public class OffsetNode extends SingleChildProcessNode {

  private final long offset;

  public OffsetNode(PlanNodeId id, long offset) {
    super(id);
    this.offset = offset;
  }

  public OffsetNode(PlanNodeId id, PlanNode child, long offset) {
    super(id, child);
    this.offset = offset;
  }

  @Override
  public PlanNode clone() {
    return new OffsetNode(getPlanNodeId(), offset);
  }

  @Override
  public List<String> getOutputColumnNames() {
    return child.getOutputColumnNames();
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitOffset(this, context);
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.OFFSET.serialize(byteBuffer);
    ReadWriteIOUtils.write(offset, byteBuffer);
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.OFFSET.serialize(stream);
    ReadWriteIOUtils.write(offset, stream);
  }

  public static OffsetNode deserialize(ByteBuffer byteBuffer) {
    long offset = ReadWriteIOUtils.readLong(byteBuffer);
    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);
    return new OffsetNode(planNodeId, offset);
  }

  public long getOffset() {
    return offset;
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
    OffsetNode that = (OffsetNode) o;
    return offset == that.offset;
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), offset);
  }
}
