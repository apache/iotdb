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

package org.apache.iotdb.db.queryengine.plan.relational.planner.node;

import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.IPlanVisitor;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.process.SingleChildProcessNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.db.queryengine.common.header.DatasetHeader;
import org.apache.iotdb.db.queryengine.execution.operator.process.copyto.CopyToOptions;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

public class CopyToNode extends SingleChildProcessNode {

  private final String targetFilePath;
  private final CopyToOptions copyToOptions;
  private final List<Symbol> childPermittedOutputs;
  private final List<Symbol> innerQueryOutputSymbols;
  private final DatasetHeader innerQueryDatasetHeader;

  public CopyToNode(
      PlanNodeId id,
      PlanNode child,
      String targetFilePath,
      CopyToOptions copyToOptions,
      List<Symbol> childPermittedOutputs,
      DatasetHeader innerQueryDatasetHeader,
      List<Symbol> innerQueryOutputSymbols) {
    super(id);
    this.child = child;
    this.targetFilePath = targetFilePath;
    this.copyToOptions = copyToOptions;
    this.childPermittedOutputs = childPermittedOutputs;
    this.innerQueryDatasetHeader = innerQueryDatasetHeader;
    this.innerQueryOutputSymbols = innerQueryOutputSymbols;
  }

  public DatasetHeader getInnerQueryDatasetHeader() {
    return innerQueryDatasetHeader;
  }

  public List<Symbol> getInnerQueryOutputSymbols() {
    return innerQueryOutputSymbols;
  }

  public String getTargetFilePath() {
    return targetFilePath;
  }

  public CopyToOptions getCopyToOptions() {
    return copyToOptions;
  }

  public List<Symbol> getChildPermittedOutputs() {
    return childPermittedOutputs;
  }

  @Override
  public PlanNode clone() {
    return new CopyToNode(
        id,
        child,
        targetFilePath,
        copyToOptions,
        childPermittedOutputs,
        innerQueryDatasetHeader,
        innerQueryOutputSymbols);
  }

  @Override
  public <R, C> R accept(IPlanVisitor<R, C> visitor, C context) {
    return ((PlanVisitor<R, C>) visitor).visitCopyTo(this, context);
  }

  @Override
  public List<Symbol> getOutputSymbols() {
    return copyToOptions.getOutputSymbols();
  }

  @Override
  public List<String> getOutputColumnNames() {
    return copyToOptions.getOutputColumnNames();
  }

  @Override
  public PlanNode replaceChildren(List<PlanNode> newChildren) {
    return new CopyToNode(
        id,
        newChildren.get(0),
        targetFilePath,
        copyToOptions,
        childPermittedOutputs,
        innerQueryDatasetHeader,
        innerQueryOutputSymbols);
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    throw new UnsupportedOperationException("CopyToNode should not be serialized");
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    throw new UnsupportedOperationException("CopyToNode should not be serialized");
  }
}
