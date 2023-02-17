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
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.FillDescriptor;
import org.apache.iotdb.db.mpp.plan.statement.component.Ordering;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;

/** FillNode is used to fill the empty field in one row. */
public class FillNode extends SingleChildProcessNode {

  // descriptions of how null values are filled
  private final FillDescriptor fillDescriptor;

  private final Ordering scanOrder;

  public FillNode(PlanNodeId id, FillDescriptor fillDescriptor, Ordering scanOrder) {
    super(id);
    this.fillDescriptor = fillDescriptor;
    this.scanOrder = scanOrder;
  }

  public FillNode(
      PlanNodeId id, PlanNode child, FillDescriptor fillDescriptor, Ordering scanOrder) {
    super(id, child);
    this.fillDescriptor = fillDescriptor;
    this.scanOrder = scanOrder;
  }

  @Override
  public PlanNode clone() {
    return new FillNode(getPlanNodeId(), fillDescriptor, scanOrder);
  }

  @Override
  public List<String> getOutputColumnNames() {
    return child.getOutputColumnNames();
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitFill(this, context);
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.FILL.serialize(byteBuffer);
    fillDescriptor.serialize(byteBuffer);
    ReadWriteIOUtils.write(scanOrder.ordinal(), byteBuffer);
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.FILL.serialize(stream);
    fillDescriptor.serialize(stream);
    ReadWriteIOUtils.write(scanOrder.ordinal(), stream);
  }

  public static FillNode deserialize(ByteBuffer byteBuffer) {
    FillDescriptor fillDescriptor = FillDescriptor.deserialize(byteBuffer);
    Ordering scanOrder = Ordering.values()[ReadWriteIOUtils.readInt(byteBuffer)];
    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);
    return new FillNode(planNodeId, fillDescriptor, scanOrder);
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
    FillNode that = (FillNode) o;
    return Objects.equals(fillDescriptor, that.fillDescriptor) && scanOrder == that.scanOrder;
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), fillDescriptor, scanOrder);
  }

  public FillDescriptor getFillDescriptor() {
    return fillDescriptor;
  }

  public Ordering getScanOrder() {
    return scanOrder;
  }
}
