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

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/** This class defines the scanned result merge task of schema fetcher. */
public class SchemaFetchMergeNode extends AbstractSchemaMergeNode {

  public SchemaFetchMergeNode(PlanNodeId id) {
    super(id);
  }

  @Override
  public PlanNode clone() {
    return new SchemaFetchMergeNode(getPlanNodeId());
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.SCHEMA_FETCH_MERGE.serialize(byteBuffer);
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.SCHEMA_FETCH_MERGE.serialize(stream);
  }

  public static PlanNode deserialize(ByteBuffer byteBuffer) {
    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);
    return new SchemaFetchMergeNode(planNodeId);
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitSchemaFetchMerge(this, context);
  }

  public String toString() {
    return String.format("SchemaFetchMergeNode-%s", getPlanNodeId());
  }
}
