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
package org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.last;

import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.MultiChildProcessNode;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import static org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.LastQueryScanNode.LAST_QUERY_HEADER_COLUMNS;

public class LastQueryTransformNode extends MultiChildProcessNode {

  private final String viewPath;

  private final String dataType;

  public LastQueryTransformNode(PlanNodeId id, String viewPath, String dataType) {
    super(id);
    this.viewPath = viewPath;
    this.dataType = dataType;
  }

  public LastQueryTransformNode(
      PlanNodeId id, List<PlanNode> planNodes, String viewPath, String dataType) {
    super(id, planNodes);
    this.viewPath = viewPath;
    this.dataType = dataType;
  }

  @Override
  public PlanNode clone() {
    return new LastQueryTransformNode(getPlanNodeId(), viewPath, dataType);
  }

  @Override
  public List<String> getOutputColumnNames() {
    return LAST_QUERY_HEADER_COLUMNS;
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.LAST_QUERY_TRANSFORM.serialize(byteBuffer);
    ReadWriteIOUtils.write(viewPath, byteBuffer);
    ReadWriteIOUtils.write(dataType, byteBuffer);
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.LAST_QUERY_TRANSFORM.serialize(stream);
    ReadWriteIOUtils.write(viewPath, stream);
    ReadWriteIOUtils.write(dataType, stream);
  }

  public static LastQueryTransformNode deserialize(ByteBuffer byteBuffer) {
    String viewPath = ReadWriteIOUtils.readString(byteBuffer);
    String dataType = ReadWriteIOUtils.readString(byteBuffer);
    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);
    return new LastQueryTransformNode(planNodeId, viewPath, dataType);
  }

  @Override
  public void addChild(PlanNode child) {
    children.add(child);
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitLastQueryTransform(this, context);
  }

  public String getViewPath() {
    return this.viewPath;
  }

  public String getDataType() {
    return this.dataType;
  }
}
