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

package org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.write.view;

import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathDeserializeUtil;
import org.apache.iotdb.commons.schema.view.viewExpression.ViewExpression;
import org.apache.iotdb.db.mpp.plan.analyze.Analysis;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.WritePlanNode;
import org.apache.iotdb.tsfile.exception.NotImplementedException;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class AlterLogicalViewNode extends WritePlanNode {

  /**
   * A map from target path to source expression. Yht target path is the name of this logical view,
   * and the source expression is the data source of this view.
   */
  private Map<PartialPath, ViewExpression> viewPathToSourceMap;

  /**
   * This variable will be set in function splitByPartition() according to analysis. And it will be
   * set when creating new split nodes.
   */
  private TRegionReplicaSet regionReplicaSet = null;

  public AlterLogicalViewNode(PlanNodeId id, Map<PartialPath, ViewExpression> viewPathToSourceMap) {
    super(id);
    this.viewPathToSourceMap = viewPathToSourceMap;
  }

  public Map<PartialPath, ViewExpression> getViewPathToSourceMap() {
    return viewPathToSourceMap;
  }

  // region Interfaces in WritePlanNode or PlanNode

  @Override
  public TRegionReplicaSet getRegionReplicaSet() {
    return this.regionReplicaSet;
  }

  @Override
  public List<PlanNode> getChildren() {
    return new ArrayList<>();
  }

  @Override
  public void addChild(PlanNode child) {
    // do nothing. this node should never have any child
  }

  @Override
  public PlanNode clone() {
    // TODO: CRTODO, complete this method
    throw new NotImplementedException("Clone of AlterLogicalNode is not implemented");
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    AlterLogicalViewNode that = (AlterLogicalViewNode) obj;
    return (this.getPlanNodeId().equals(that.getPlanNodeId())
        && Objects.equals(this.viewPathToSourceMap, that.viewPathToSourceMap));
  }

  @Override
  public int allowedChildCount() {
    // this node should never have any child
    return NO_CHILD_ALLOWED;
  }

  @Override
  public List<String> getOutputColumnNames() {
    // TODO: CRTODO, complete this method
    throw new NotImplementedException(
        "getOutputColumnNames of AlterLogicalViewNode is not implemented");
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.ALTER_LOGICAL_VIEW.serialize(byteBuffer);
    // serialize other member variables for this node
    ReadWriteIOUtils.write(this.viewPathToSourceMap.size(), byteBuffer);
    for (Map.Entry<PartialPath, ViewExpression> entry : viewPathToSourceMap.entrySet()) {
      entry.getKey().serialize(byteBuffer);
      ViewExpression.serialize(entry.getValue(), byteBuffer);
    }
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.ALTER_LOGICAL_VIEW.serialize(stream);
    // serialize other member variables for this node
    ReadWriteIOUtils.write(this.viewPathToSourceMap.size(), stream);
    for (Map.Entry<PartialPath, ViewExpression> entry : viewPathToSourceMap.entrySet()) {
      entry.getKey().serialize(stream);
      ViewExpression.serialize(entry.getValue(), stream);
    }
  }

  public static AlterLogicalViewNode deserialize(ByteBuffer byteBuffer) {
    // deserialize member variables
    Map<PartialPath, ViewExpression> viewPathToSourceMap = new HashMap<>();
    int size = byteBuffer.getInt();
    PartialPath path;
    ViewExpression viewExpression;
    for (int i = 0; i < size; i++) {
      path = (PartialPath) PathDeserializeUtil.deserialize(byteBuffer);
      viewExpression = ViewExpression.deserialize(byteBuffer);
      viewPathToSourceMap.put(path, viewExpression);
    }
    // deserialize PlanNodeId next
    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);
    return new AlterLogicalViewNode(planNodeId, viewPathToSourceMap);
  }

  @Override
  public List<WritePlanNode> splitByPartition(Analysis analysis) {
    Map<TRegionReplicaSet, Map<PartialPath, ViewExpression>> splitMap = new HashMap<>();
    for (Map.Entry<PartialPath, ViewExpression> entry : this.viewPathToSourceMap.entrySet()) {
      // for each entry in the map for target path to source expression,
      // build a map from TRegionReplicaSet to this entry.
      // Please note that getSchemaRegionReplicaSet needs a device path as parameter.
      TRegionReplicaSet regionReplicaSet =
          analysis.getSchemaPartitionInfo().getSchemaRegionReplicaSet(entry.getKey().getDevice());

      // create a map if the key(regionReplicaSet) is not exists,
      // then put this entry into this map(from regionReplicaSet to this entry)
      splitMap
          .computeIfAbsent(regionReplicaSet, k -> new HashMap<>())
          .put(entry.getKey(), entry.getValue());
    }

    // split this node into several nodes according to their regionReplicaSet
    List<WritePlanNode> result = new ArrayList<>();
    for (Map.Entry<TRegionReplicaSet, Map<PartialPath, ViewExpression>> entry :
        splitMap.entrySet()) {
      // for each entry in splitMap, create a plan node.
      result.add(new CreateLogicalViewNode(getPlanNodeId(), entry.getValue(), entry.getKey()));
    }
    return result;
  }
  // endregion
}
