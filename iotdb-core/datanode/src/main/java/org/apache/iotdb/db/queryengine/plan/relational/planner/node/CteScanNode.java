/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package org.apache.iotdb.db.queryengine.plan.relational.planner.node;

import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.db.queryengine.common.DataNodeEndPoints;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.SourceNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.QualifiedName;
import org.apache.iotdb.db.utils.cte.CteDataStore;

import com.google.common.collect.ImmutableList;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;

public class CteScanNode extends SourceNode {
  private final QualifiedName qualifiedName;
  // Indicate the column this node need to output
  private final List<Symbol> outputSymbols;
  private final CteDataStore dataStore;

  public CteScanNode(
      PlanNodeId id,
      QualifiedName qualifiedName,
      List<Symbol> outputSymbols,
      CteDataStore dataStore) {
    super(id);
    this.qualifiedName = qualifiedName;
    this.outputSymbols = outputSymbols;
    this.dataStore = dataStore;
  }

  public QualifiedName getQualifiedName() {
    return qualifiedName;
  }

  public CteDataStore getDataStore() {
    return dataStore;
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitCteScan(this, context);
  }

  @Override
  public void open() throws Exception {}

  @Override
  public void setRegionReplicaSet(TRegionReplicaSet regionReplicaSet) {}

  @Override
  public void close() throws Exception {}

  @Override
  public TRegionReplicaSet getRegionReplicaSet() {
    return new TRegionReplicaSet(
        null, ImmutableList.of(DataNodeEndPoints.getLocalDataNodeLocation()));
  }

  @Override
  public List<PlanNode> getChildren() {
    return ImmutableList.of();
  }

  @Override
  public void addChild(PlanNode child) {}

  @Override
  public PlanNode clone() {
    return new CteScanNode(getPlanNodeId(), qualifiedName, outputSymbols, dataStore);
  }

  @Override
  public int allowedChildCount() {
    return 0;
  }

  @Override
  public List<String> getOutputColumnNames() {
    return outputSymbols.stream().map(Symbol::getName).collect(Collectors.toList());
  }

  @Override
  public List<Symbol> getOutputSymbols() {
    return outputSymbols;
  }

  @Override
  public PlanNode replaceChildren(List<PlanNode> newChildren) {
    checkArgument(newChildren.isEmpty(), "newChildren is not empty");
    return this;
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    throw new UnsupportedOperationException();
  }
}
