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

import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanVisitor;

import java.nio.ByteBuffer;

public class SeriesSchemaMergeNode extends AbstractSchemaMergeNode {

  private boolean orderByHeat;

  public SeriesSchemaMergeNode(PlanNodeId id) {
    super(id);
  }

  public SeriesSchemaMergeNode(PlanNodeId id, boolean orderByHeat) {
    this(id);
    this.orderByHeat = orderByHeat;
  }

  @Override
  public PlanNode clone() {
    return new SeriesSchemaMergeNode(getPlanNodeId(), this.orderByHeat);
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.SCHEMA_MERGE.serialize(byteBuffer);
  }

  public static SeriesSchemaMergeNode deserialize(ByteBuffer byteBuffer) {
    PlanNodeId id = PlanNodeId.deserialize(byteBuffer);
    return new SeriesSchemaMergeNode(id);
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitSchemaMerge(this, context);
  }
}
