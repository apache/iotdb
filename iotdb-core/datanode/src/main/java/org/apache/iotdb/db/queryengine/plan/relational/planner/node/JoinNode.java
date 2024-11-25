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
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.TwoChildProcessNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ComparisonExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.NullLiteral;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.JoinNode.JoinType.INNER;

public class JoinNode extends TwoChildProcessNode {

  private final JoinType joinType;
  private final List<EquiJoinClause> criteria;
  private final List<Symbol> leftOutputSymbols;
  private final List<Symbol> rightOutputSymbols;
  // some filter like 'a.xx_column < b.yy_column'
  private final Optional<Expression> filter;
  private final Optional<Boolean> spillable;

  // private final boolean maySkipOutputDuplicates;
  // private final Optional<Symbol> leftHashSymbol;
  // private final Optional<Symbol> rightHashSymbol;
  // private final Optional<DistributionType> distributionType;
  // private final Map<DynamicFilterId, Symbol> dynamicFilters;

  public JoinNode(
      PlanNodeId id,
      JoinType joinType,
      PlanNode leftChild,
      PlanNode rightChild,
      List<EquiJoinClause> criteria,
      List<Symbol> leftOutputSymbols,
      List<Symbol> rightOutputSymbols,
      Optional<Expression> filter,
      Optional<Boolean> spillable) {
    super(id);
    requireNonNull(joinType, "type is null");
    requireNonNull(leftChild, "left is null");
    requireNonNull(rightChild, "right is null");
    requireNonNull(criteria, "criteria is null");
    requireNonNull(leftOutputSymbols, "leftOutputSymbols is null");
    requireNonNull(rightOutputSymbols, "rightOutputSymbols is null");
    requireNonNull(filter, "filter is null");
    // The condition doesn't guarantee that filter is of type boolean, but was found to be a
    // practical way to identify
    // places where JoinNode could be created without appropriate coercions.
    checkArgument(
        !filter.isPresent() || !(filter.get() instanceof NullLiteral),
        "Filter must be an expression of boolean type: %s",
        filter);
    // requireNonNull(leftHashSymbol, "leftHashSymbol is null");
    // requireNonNull(rightHashSymbol, "rightHashSymbol is null");
    requireNonNull(spillable, "spillable is null");

    this.joinType = joinType;
    this.leftChild = leftChild;
    this.rightChild = rightChild;
    this.criteria = ImmutableList.copyOf(criteria);
    this.leftOutputSymbols = ImmutableList.copyOf(leftOutputSymbols);
    this.rightOutputSymbols = ImmutableList.copyOf(rightOutputSymbols);
    this.filter = filter;
    this.spillable = spillable;
    // this.maySkipOutputDuplicates = maySkipOutputDuplicates;
    // this.leftHashSymbol = leftHashSymbol;
    // this.rightHashSymbol = rightHashSymbol;

    Set<Symbol> leftSymbols = ImmutableSet.copyOf(leftChild.getOutputSymbols());
    Set<Symbol> rightSymbols = ImmutableSet.copyOf(rightChild.getOutputSymbols());

    checkArgument(
        leftSymbols.containsAll(leftOutputSymbols),
        "Left source inputs do not contain all left output symbols");
    checkArgument(
        rightSymbols.containsAll(rightOutputSymbols),
        "Right source inputs do not contain all right output symbols");

    //    checkArgument(
    //        !(criteria.isEmpty() && leftHashSymbol.isPresent()),
    //        "Left hash symbol is only valid in an equijoin");
    //    checkArgument(
    //        !(criteria.isEmpty() && rightHashSymbol.isPresent()),
    //        "Right hash symbol is only valid in an equijoin");

    criteria.forEach(
        equiJoinClause ->
            checkArgument(
                leftSymbols.contains(equiJoinClause.getLeft())
                    && rightSymbols.contains(equiJoinClause.getRight()),
                "Equality join criteria should be normalized according to join sides: %s",
                equiJoinClause));
  }

  // only used for deserialize
  public JoinNode(
      PlanNodeId id,
      JoinType joinType,
      List<EquiJoinClause> criteria,
      List<Symbol> leftOutputSymbols,
      List<Symbol> rightOutputSymbols) {
    super(id);
    requireNonNull(joinType, "type is null");
    requireNonNull(criteria, "criteria is null");

    this.leftOutputSymbols = leftOutputSymbols;
    this.rightOutputSymbols = rightOutputSymbols;
    this.filter = Optional.empty();
    this.spillable = Optional.empty();

    this.joinType = joinType;
    this.criteria = criteria;
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitJoin(this, context);
  }

  @Override
  public PlanNode replaceChildren(List<PlanNode> newChildren) {
    checkArgument(newChildren.size() == 2, "expected newChildren to contain 2 nodes for JoinNode");
    return new JoinNode(
        getPlanNodeId(),
        joinType,
        newChildren.get(0),
        newChildren.get(1),
        criteria,
        leftOutputSymbols,
        rightOutputSymbols,
        filter,
        spillable);
  }

  @Override
  public List<Symbol> getOutputSymbols() {
    return ImmutableList.<Symbol>builder()
        .addAll(leftOutputSymbols)
        .addAll(rightOutputSymbols)
        .build();
  }

  @Override
  public PlanNode clone() {
    JoinNode joinNode =
        new JoinNode(
            getPlanNodeId(),
            joinType,
            getLeftChild(),
            getRightChild(),
            criteria,
            leftOutputSymbols,
            rightOutputSymbols,
            filter,
            spillable);
    joinNode.setLeftChild(null);
    joinNode.setRightChild(null);
    return joinNode;
  }

  @Override
  public List<String> getOutputColumnNames() {
    throw new IllegalStateException();
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.TABLE_JOIN_NODE.serialize(byteBuffer);

    ReadWriteIOUtils.write(joinType.ordinal(), byteBuffer);

    ReadWriteIOUtils.write(criteria.size(), byteBuffer);
    for (EquiJoinClause equiJoinClause : criteria) {
      Symbol.serialize(equiJoinClause.getLeft(), byteBuffer);
      Symbol.serialize(equiJoinClause.getRight(), byteBuffer);
    }

    ReadWriteIOUtils.write(leftOutputSymbols.size(), byteBuffer);
    for (Symbol leftOutputSymbol : leftOutputSymbols) {
      Symbol.serialize(leftOutputSymbol, byteBuffer);
    }
    ReadWriteIOUtils.write(rightOutputSymbols.size(), byteBuffer);
    for (Symbol rightOutputSymbol : rightOutputSymbols) {
      Symbol.serialize(rightOutputSymbol, byteBuffer);
    }
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.TABLE_JOIN_NODE.serialize(stream);

    ReadWriteIOUtils.write(joinType.ordinal(), stream);

    ReadWriteIOUtils.write(criteria.size(), stream);
    for (EquiJoinClause equiJoinClause : criteria) {
      Symbol.serialize(equiJoinClause.getLeft(), stream);
      Symbol.serialize(equiJoinClause.getRight(), stream);
    }

    ReadWriteIOUtils.write(leftOutputSymbols.size(), stream);
    for (Symbol leftOutputSymbol : leftOutputSymbols) {
      Symbol.serialize(leftOutputSymbol, stream);
    }
    ReadWriteIOUtils.write(rightOutputSymbols.size(), stream);
    for (Symbol rightOutputSymbol : rightOutputSymbols) {
      Symbol.serialize(rightOutputSymbol, stream);
    }
  }

  public static JoinNode deserialize(ByteBuffer byteBuffer) {
    JoinType joinType = JoinType.values()[ReadWriteIOUtils.readInt(byteBuffer)];
    int size = ReadWriteIOUtils.readInt(byteBuffer);
    List<EquiJoinClause> criteria = new ArrayList<>(size);
    while (size-- > 0) {
      criteria.add(
          new EquiJoinClause(Symbol.deserialize(byteBuffer), Symbol.deserialize(byteBuffer)));
    }

    size = ReadWriteIOUtils.readInt(byteBuffer);
    List<Symbol> leftOutputSymbols = new ArrayList<>(size);
    while (size-- > 0) {
      leftOutputSymbols.add(Symbol.deserialize(byteBuffer));
    }

    size = ReadWriteIOUtils.readInt(byteBuffer);
    List<Symbol> rightOutputSymbols = new ArrayList<>(size);
    while (size-- > 0) {
      rightOutputSymbols.add(Symbol.deserialize(byteBuffer));
    }

    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);
    return new JoinNode(planNodeId, joinType, criteria, leftOutputSymbols, rightOutputSymbols);
  }

  public JoinType getJoinType() {
    return joinType;
  }

  public List<EquiJoinClause> getCriteria() {
    return criteria;
  }

  public List<Symbol> getLeftOutputSymbols() {
    return leftOutputSymbols;
  }

  public List<Symbol> getRightOutputSymbols() {
    return rightOutputSymbols;
  }

  public Optional<Expression> getFilter() {
    return filter;
  }

  public Optional<Boolean> isSpillable() {
    return spillable;
  }

  public boolean isCrossJoin() {
    return criteria.isEmpty() && !filter.isPresent() && joinType == INNER;
  }

  @Override
  public String toString() {
    return "JoinNode-" + this.getPlanNodeId();
  }

  public static class EquiJoinClause {
    private final Symbol left;
    private final Symbol right;

    public EquiJoinClause(Symbol left, Symbol right) {
      this.left = requireNonNull(left, "left is null");
      this.right = requireNonNull(right, "right is null");
    }

    public Symbol getLeft() {
      return left;
    }

    public Symbol getRight() {
      return right;
    }

    public ComparisonExpression toExpression() {
      return new ComparisonExpression(
          ComparisonExpression.Operator.EQUAL, left.toSymbolReference(), right.toSymbolReference());
    }

    public EquiJoinClause flip() {
      return new EquiJoinClause(right, left);
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }

      if (obj == null || !this.getClass().equals(obj.getClass())) {
        return false;
      }

      EquiJoinClause other = (EquiJoinClause) obj;

      return Objects.equals(this.left, other.left) && Objects.equals(this.right, other.right);
    }

    @Override
    public int hashCode() {
      return Objects.hash(left, right);
    }

    @Override
    public String toString() {
      return format("%s = %s", left, right);
    }
  }

  public enum JoinType {
    INNER("InnerJoin"),
    LEFT("LeftJoin"),
    RIGHT("RightJoin"),
    FULL("FullJoin");

    private final String joinLabel;

    JoinType(String joinLabel) {
      this.joinLabel = joinLabel;
    }

    public String getJoinLabel() {
      return joinLabel;
    }
  }
}
