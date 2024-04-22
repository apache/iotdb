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

package org.apache.iotdb.db.queryengine.plan.planner.plan.node.process;

import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.utils.columngenerator.parameter.ColumnGeneratorParameter;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class ColumnInjectNode extends SingleChildProcessNode {

  private final int targetIndex;
  private final ColumnGeneratorParameter columnGeneratorParameter;

  public ColumnInjectNode(
      PlanNodeId id, int targetIndex, ColumnGeneratorParameter columnGeneratorParameter) {
    super(id);
    this.targetIndex = targetIndex;
    this.columnGeneratorParameter = columnGeneratorParameter;
  }

  public ColumnInjectNode(
      PlanNodeId id,
      PlanNode child,
      int targetIndex,
      ColumnGeneratorParameter columnGeneratorParameter) {
    super(id, child);
    this.targetIndex = targetIndex;
    this.columnGeneratorParameter = columnGeneratorParameter;
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitColumnInject(this, context);
  }

  public List<TSDataType> getGeneratedColumnTypes() {
    return columnGeneratorParameter.getColumnTypes();
  }

  public int getTargetIndex() {
    return targetIndex;
  }

  public ColumnGeneratorParameter getColumnGeneratorParameter() {
    return columnGeneratorParameter;
  }

  @Override
  public PlanNodeType getType() {
    return PlanNodeType.COLUMN_INJECT;
  }

  @Override
  public PlanNode clone() {
    return new ColumnInjectNode(getPlanNodeId(), this.targetIndex, this.columnGeneratorParameter);
  }

  @Override
  public List<String> getOutputColumnNames() {
    List<String> outputColumnNames = new ArrayList<>();
    List<String> childOutputColumnNames = child.getOutputColumnNames();

    // just need to append column names
    if (targetIndex == childOutputColumnNames.size()) {
      outputColumnNames.addAll(childOutputColumnNames);
      outputColumnNames.addAll(columnGeneratorParameter.getColumnNames());
      return outputColumnNames;
    }

    // insert extra column names into the right position
    for (int i = 0; i < childOutputColumnNames.size(); i++) {
      if (i == targetIndex) {
        outputColumnNames.addAll(columnGeneratorParameter.getColumnNames());
      }
      outputColumnNames.add(childOutputColumnNames.get(i));
    }
    return outputColumnNames;
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.COLUMN_INJECT.serialize(byteBuffer);
    ReadWriteIOUtils.write(targetIndex, byteBuffer);
    columnGeneratorParameter.serialize(byteBuffer);
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.COLUMN_INJECT.serialize(stream);
    ReadWriteIOUtils.write(targetIndex, stream);
    columnGeneratorParameter.serialize(stream);
  }

  public static ColumnInjectNode deserialize(ByteBuffer byteBuffer) {
    int targetIndex = ReadWriteIOUtils.readInt(byteBuffer);
    ColumnGeneratorParameter columnGeneratorParameter =
        ColumnGeneratorParameter.deserialize(byteBuffer);
    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);
    return new ColumnInjectNode(planNodeId, targetIndex, columnGeneratorParameter);
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
    ColumnInjectNode that = (ColumnInjectNode) o;
    return targetIndex == that.targetIndex
        && columnGeneratorParameter.equals(that.columnGeneratorParameter);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), targetIndex, columnGeneratorParameter);
  }
}
