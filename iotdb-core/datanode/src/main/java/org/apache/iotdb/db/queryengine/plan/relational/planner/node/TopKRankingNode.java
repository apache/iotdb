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

public class TopKRankingNode extends SingleChildProcessNode {
  public enum RankingType {
    ROW_NUMBER,
    RANK,
    DENSE_RANK
  }

  private final DataOrganizationSpecification specification;
  private final RankingType rankingType;
  private final Symbol rankingSymbol;
  private final int maxRankingPerPartition;
  private final boolean partial;

  public TopKRankingNode(
      PlanNodeId id,
      DataOrganizationSpecification specification,
      RankingType rankingType,
      Symbol rankingSymbol,
      int maxRankingPerPartition,
      boolean partial) {
    super(id);

    this.specification = specification;
    this.rankingType = rankingType;
    this.rankingSymbol = rankingSymbol;
    this.maxRankingPerPartition = maxRankingPerPartition;
    this.partial = partial;
  }

  public TopKRankingNode(
      PlanNodeId id,
      PlanNode child,
      DataOrganizationSpecification specification,
      RankingType rankingType,
      Symbol rankingSymbol,
      int maxRankingPerPartition,
      boolean partial) {
    super(id, child);

    this.specification = specification;
    this.rankingType = rankingType;
    this.rankingSymbol = rankingSymbol;
    this.maxRankingPerPartition = maxRankingPerPartition;
    this.partial = partial;
  }

  @Override
  public PlanNode clone() {
    return new TopKRankingNode(
        getPlanNodeId(),
        specification,
        rankingType,
        rankingSymbol,
        maxRankingPerPartition,
        partial);
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitTopKRanking(this, context);
  }

  public DataOrganizationSpecification getSpecification() {
    return specification;
  }

  public boolean isPartial() {
    return partial;
  }

  public Symbol getRankingSymbol() {
    return rankingSymbol;
  }

  public int getMaxRankingPerPartition() {
    return maxRankingPerPartition;
  }

  public RankingType getRankingType() {
    return rankingType;
  }

  @Override
  public List<String> getOutputColumnNames() {
    throw new UnsupportedOperationException();
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.TABLE_TOPK_RANKING_NODE.serialize(byteBuffer);
    specification.serialize(byteBuffer);
    ReadWriteIOUtils.write(rankingType.ordinal(), byteBuffer);
    Symbol.serialize(rankingSymbol, byteBuffer);
    ReadWriteIOUtils.write(maxRankingPerPartition, byteBuffer);
    ReadWriteIOUtils.write(partial, byteBuffer);
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.TABLE_TOPK_RANKING_NODE.serialize(stream);
    specification.serialize(stream);
    ReadWriteIOUtils.write(rankingType.ordinal(), stream);
    Symbol.serialize(rankingSymbol, stream);
    ReadWriteIOUtils.write(maxRankingPerPartition, stream);
    ReadWriteIOUtils.write(partial, stream);
  }

  public static TopKRankingNode deserialize(ByteBuffer byteBuffer) {
    DataOrganizationSpecification specification =
        DataOrganizationSpecification.deserialize(byteBuffer);
    RankingType rankingType = RankingType.values()[ReadWriteIOUtils.readInt(byteBuffer)];
    Symbol rankingSymbol = Symbol.deserialize(byteBuffer);
    int maxRankingPerPartition = ReadWriteIOUtils.readInt(byteBuffer);
    boolean partial = ReadWriteIOUtils.readBoolean(byteBuffer);

    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);
    return new TopKRankingNode(
        planNodeId, specification, rankingType, rankingSymbol, maxRankingPerPartition, partial);
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
    return new TopKRankingNode(
        id,
        Iterables.getOnlyElement(newChildren),
        specification,
        rankingType,
        rankingSymbol,
        maxRankingPerPartition,
        partial);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;
    TopKRankingNode rankingNode = (TopKRankingNode) o;
    return Objects.equal(specification, rankingNode.specification)
        && Objects.equal(rankingType, rankingNode.rankingType)
        && Objects.equal(rankingSymbol, rankingNode.rankingSymbol)
        && Objects.equal(maxRankingPerPartition, rankingNode.maxRankingPerPartition)
        && Objects.equal(partial, rankingNode.partial);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(
        super.hashCode(),
        specification,
        rankingType,
        rankingSymbol,
        maxRankingPerPartition,
        partial);
  }

  @Override
  public String toString() {
    return "TopKRankingNode-" + this.getPlanNodeId();
  }
}
