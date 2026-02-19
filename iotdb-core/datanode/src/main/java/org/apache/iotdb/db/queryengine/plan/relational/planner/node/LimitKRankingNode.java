/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.queryengine.plan.relational.planner.node;

import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.SingleChildProcessNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.DataOrganizationSpecification;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * Plan node for the streaming per-partition limit optimization. Used when the input data is already
 * sorted by (partition_keys, time), so we can take the first K rows per partition without sorting.
 */
public class LimitKRankingNode extends SingleChildProcessNode {

  private final DataOrganizationSpecification specification;
  private final Symbol rankingSymbol;
  private final int maxRowCountPerPartition;

  public LimitKRankingNode(
      PlanNodeId id,
      DataOrganizationSpecification specification,
      Symbol rankingSymbol,
      int maxRowCountPerPartition) {
    super(id);
    this.specification = specification;
    this.rankingSymbol = rankingSymbol;
    this.maxRowCountPerPartition = maxRowCountPerPartition;
  }

  public LimitKRankingNode(
      PlanNodeId id,
      PlanNode child,
      DataOrganizationSpecification specification,
      Symbol rankingSymbol,
      int maxRowCountPerPartition) {
    super(id, child);
    this.specification = specification;
    this.rankingSymbol = rankingSymbol;
    this.maxRowCountPerPartition = maxRowCountPerPartition;
  }

  @Override
  public PlanNode clone() {
    return new LimitKRankingNode(
        getPlanNodeId(), specification, rankingSymbol, maxRowCountPerPartition);
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitLimitKRanking(this, context);
  }

  public DataOrganizationSpecification getSpecification() {
    return specification;
  }

  public Symbol getRankingSymbol() {
    return rankingSymbol;
  }

  public int getMaxRowCountPerPartition() {
    return maxRowCountPerPartition;
  }

  @Override
  public List<String> getOutputColumnNames() {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<Symbol> getOutputSymbols() {
    return ImmutableList.<Symbol>builder()
        .addAll(getChild().getOutputSymbols())
        .add(rankingSymbol)
        .build();
  }

  @Override
  public PlanNode replaceChildren(List<PlanNode> newChildren) {
    return new LimitKRankingNode(
        id,
        Iterables.getOnlyElement(newChildren),
        specification,
        rankingSymbol,
        maxRowCountPerPartition);
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.TABLE_LIMITK_RANKING_NODE.serialize(byteBuffer);
    specification.serialize(byteBuffer);
    Symbol.serialize(rankingSymbol, byteBuffer);
    ReadWriteIOUtils.write(maxRowCountPerPartition, byteBuffer);
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.TABLE_LIMITK_RANKING_NODE.serialize(stream);
    specification.serialize(stream);
    Symbol.serialize(rankingSymbol, stream);
    ReadWriteIOUtils.write(maxRowCountPerPartition, stream);
  }

  public static LimitKRankingNode deserialize(ByteBuffer byteBuffer) {
    DataOrganizationSpecification specification =
        DataOrganizationSpecification.deserialize(byteBuffer);
    Symbol rankingSymbol = Symbol.deserialize(byteBuffer);
    int maxRowCountPerPartition = ReadWriteIOUtils.readInt(byteBuffer);
    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);
    return new LimitKRankingNode(planNodeId, specification, rankingSymbol, maxRowCountPerPartition);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;
    LimitKRankingNode that = (LimitKRankingNode) o;
    return Objects.equal(specification, that.specification)
        && Objects.equal(rankingSymbol, that.rankingSymbol)
        && Objects.equal(maxRowCountPerPartition, that.maxRowCountPerPartition);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(
        super.hashCode(), specification, rankingSymbol, maxRowCountPerPartition);
  }

  @Override
  public String toString() {
    return "LimitKRankingNode-" + this.getPlanNodeId();
  }
}
