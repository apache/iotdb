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
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.GroupByTimeParameter;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class WindowSplitNode extends SingleChildProcessNode {

  private final GroupByTimeParameter groupByTimeParameter;
  private final List<Integer> samplingIndexes;

  public WindowSplitNode(
      PlanNodeId id,
      PlanNode child,
      GroupByTimeParameter groupByTimeParameter,
      List<Integer> samplingIndexes) {
    super(id, child);
    this.groupByTimeParameter = groupByTimeParameter;
    this.samplingIndexes = samplingIndexes;
  }

  public WindowSplitNode(
      PlanNodeId id, GroupByTimeParameter groupByTimeParameter, List<Integer> samplingIndexes) {
    super(id);
    this.groupByTimeParameter = groupByTimeParameter;
    this.samplingIndexes = samplingIndexes;
  }

  public GroupByTimeParameter getGroupByTimeParameter() {
    return groupByTimeParameter;
  }

  public List<Integer> getSamplingIndexes() {
    return samplingIndexes;
  }

  @Override
  public PlanNode clone() {
    return new WindowSplitNode(getPlanNodeId(), groupByTimeParameter, samplingIndexes);
  }

  @Override
  public List<String> getOutputColumnNames() {
    return child.getOutputColumnNames();
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.WINDOW_SPLIT.serialize(byteBuffer);
    groupByTimeParameter.serialize(byteBuffer);
    ReadWriteIOUtils.write(samplingIndexes.size(), byteBuffer);
    for (Integer index : samplingIndexes) {
      ReadWriteIOUtils.write(index, byteBuffer);
    }
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.WINDOW_SPLIT.serialize(stream);
    groupByTimeParameter.serialize(stream);
    ReadWriteIOUtils.write(samplingIndexes.size(), stream);
    for (Integer index : samplingIndexes) {
      ReadWriteIOUtils.write(index, stream);
    }
  }

  public static WindowSplitNode deserialize(ByteBuffer byteBuffer) {
    GroupByTimeParameter groupByTimeParameter = GroupByTimeParameter.deserialize(byteBuffer);

    int listSize = ReadWriteIOUtils.readInt(byteBuffer);
    List<Integer> samplingIndexes = new ArrayList<>(listSize);
    while (listSize > 0) {
      samplingIndexes.add(ReadWriteIOUtils.readInt(byteBuffer));
      listSize--;
    }

    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);
    return new WindowSplitNode(planNodeId, groupByTimeParameter, samplingIndexes);
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
    WindowSplitNode that = (WindowSplitNode) o;
    return groupByTimeParameter.equals(that.groupByTimeParameter)
        && samplingIndexes.equals(that.samplingIndexes);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), groupByTimeParameter, samplingIndexes);
  }

  @Override
  public String toString() {
    return String.format("WindowSplitNode-%s", getPlanNodeId());
  }
}
