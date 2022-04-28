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

import org.apache.iotdb.db.mpp.common.header.ColumnHeader;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.process.ProcessNode;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public abstract class AbstractSchemaMergeNode extends ProcessNode {

  private final List<PlanNode> children;

  public AbstractSchemaMergeNode(PlanNodeId id) {
    super(id);
    children = new ArrayList<>();
  }

  @Override
  public List<PlanNode> getChildren() {
    return children;
  }

  @Override
  public void addChild(PlanNode child) {
    children.add(child);
  }

  @Override
  public int allowedChildCount() {
    return CHILD_COUNT_NO_LIMIT;
  }

  @Override
  public List<ColumnHeader> getOutputColumnHeaders() {
    if (children.size() > 0) {
      return children.get(0).getOutputColumnHeaders();
    }
    return Collections.emptyList();
  }

  @Override
  public List<String> getOutputColumnNames() {
    if (children.size() > 0) {
      return children.get(0).getOutputColumnNames();
    }
    return Collections.emptyList();
  }

  @Override
  public List<TSDataType> getOutputColumnTypes() {
    if (children.size() > 0) {
      return children.get(0).getOutputColumnTypes();
    }
    return Collections.emptyList();
  }
}
