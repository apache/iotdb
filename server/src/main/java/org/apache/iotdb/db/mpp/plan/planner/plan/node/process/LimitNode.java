/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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

/** LimitNode is used to select top n result. It uses the default order of upstream nodes */
public class LimitNode extends SingleChildProcessNode {

  private final long limit;

  public LimitNode(PlanNodeId id, long limit) {
    super(id);
    this.limit = limit;
  }

  public LimitNode(PlanNodeId id, PlanNode child, long limit) {
    super(id, child);
    this.limit = limit;
  }

  public long getLimit() {
    return limit;
  }

  @Override
  public PlanNode clone() {
    return new LimitNode(getPlanNodeId(), this.limit);
  }

  @Override
  public List<String> getOutputColumnNames() {
    return child.getOutputColumnNames();
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitLimit(this, context);
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.LIMIT.serialize(byteBuffer);
    ReadWriteIOUtils.write(limit, byteBuffer);
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.LIMIT.serialize(stream);
    ReadWriteIOUtils.write(limit, stream);
  }

  public static LimitNode deserialize(ByteBuffer byteBuffer) {
    long limit = ReadWriteIOUtils.readLong(byteBuffer);
    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);
    return new LimitNode(planNodeId, limit);
  }

  @Override
  public String toString() {
    return "LimitNode-" + this.getPlanNodeId();
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
    LimitNode that = (LimitNode) o;
    return limit == that.limit;
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), limit);
  }
}
