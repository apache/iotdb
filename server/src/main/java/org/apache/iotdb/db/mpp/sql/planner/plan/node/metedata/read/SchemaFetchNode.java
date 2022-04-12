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

package org.apache.iotdb.db.mpp.sql.planner.plan.node.metedata.read;

import org.apache.iotdb.db.mpp.common.schematree.PathPatternTree;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNodeType;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.nio.ByteBuffer;
import java.util.List;

public class SchemaFetchNode extends PlanNode {

  private final PathPatternTree patternTree;

  public SchemaFetchNode(PlanNodeId id, PathPatternTree patternTree) {
    super(id);
    this.patternTree = patternTree;
  }

  public PathPatternTree getPatternTree() {
    return patternTree;
  }

  @Override
  public List<PlanNode> getChildren() {
    return null;
  }

  @Override
  public void addChild(PlanNode child) {}

  @Override
  public PlanNode clone() {
    return new SchemaFetchNode(getPlanNodeId(), patternTree);
  }

  @Override
  public int allowedChildCount() {
    return 0;
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.SCHEMA_FETCH.serialize(byteBuffer);
    patternTree.serialize(byteBuffer);
  }

  public static SchemaFetchNode deserialize(ByteBuffer byteBuffer) {
    String id = ReadWriteIOUtils.readString(byteBuffer);
    PathPatternTree patternTree = PathPatternTree.deserialize(byteBuffer);
    return new SchemaFetchNode(new PlanNodeId(id), patternTree);
  }
}
