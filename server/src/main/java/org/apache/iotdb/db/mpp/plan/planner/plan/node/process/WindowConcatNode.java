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

public class WindowConcatNode extends SingleChildProcessNode {

  private final long interval;
  private final long step;

  public WindowConcatNode(PlanNodeId id, PlanNode child, long interval, long step) {
    super(id, child);
    this.interval = interval;
    this.step = step;
  }

  public WindowConcatNode(PlanNodeId id, long interval, long step) {
    super(id);
    this.interval = interval;
    this.step = step;
  }

  public long getInterval() {
    return interval;
  }

  public long getStep() {
    return step;
  }

  @Override
  public PlanNode clone() {
    return new WindowConcatNode(getPlanNodeId(), interval, step);
  }

  @Override
  public List<String> getOutputColumnNames() {
    return child.getOutputColumnNames();
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.WINDOW_CONCAT.serialize(byteBuffer);
    ReadWriteIOUtils.write(interval, byteBuffer);
    ReadWriteIOUtils.write(step, byteBuffer);
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.WINDOW_CONCAT.serialize(stream);
    ReadWriteIOUtils.write(interval, stream);
    ReadWriteIOUtils.write(step, stream);
  }

  public static WindowConcatNode deserialize(ByteBuffer byteBuffer) {
    long interval = ReadWriteIOUtils.readLong(byteBuffer);
    long step = ReadWriteIOUtils.readLong(byteBuffer);
    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);
    return new WindowConcatNode(planNodeId, interval, step);
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
    WindowConcatNode that = (WindowConcatNode) o;
    return that.interval == interval && that.step == step;
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), interval, step);
  }

  @Override
  public String toString() {
    return String.format("WindowConcatNode-%s", getPlanNodeId());
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitWindowConcat(this, context);
  }
}
