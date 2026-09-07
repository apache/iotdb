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

package org.apache.iotdb.commons.queryengine.plan.relational.planner.node;

import org.apache.iotdb.commons.i18n.QueryMessages;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.ICoreQueryPlanVisitor;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.IPlanVisitor;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.process.TwoChildProcessNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.ComparisonExpression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.NullLiteral;

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
import static org.apache.iotdb.commons.queryengine.plan.relational.planner.node.JoinNode.EquiJoinClause.flipBatch;
import static org.apache.iotdb.commons.queryengine.plan.relational.planner.node.JoinNode.JoinType.INNER;

public class JoinNode extends TwoChildProcessNode {

  private final JoinType joinType;
  private final List<EquiJoinClause> criteria;
  private final Optional<AsofJoinClause> asofCriteria;
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
      Optional<AsofJoinClause> asofCriteria,
      List<Symbol> leftOutputSymbols,
      List<Symbol> rightOutputSymbols,
      Optional<Expression> filter,
      Optional<Boolean> spillable) {
    super(id);
    requireNonNull(joinType, QueryMessages.EXCEPTION_TYPE_IS_NULL_16A3D3EB);
    requireNonNull(leftChild, QueryMessages.EXCEPTION_LEFT_IS_NULL_2C1080C5);
    requireNonNull(rightChild, QueryMessages.EXCEPTION_RIGHT_IS_NULL_97BD6491);
    requireNonNull(criteria, QueryMessages.EXCEPTION_CRITERIA_IS_NULL_2996D1A3);
    requireNonNull(leftOutputSymbols, QueryMessages.EXCEPTION_LEFTOUTPUTSYMBOLS_IS_NULL_083AE900);
    requireNonNull(rightOutputSymbols, QueryMessages.EXCEPTION_RIGHTOUTPUTSYMBOLS_IS_NULL_F44B848F);
    requireNonNull(filter, QueryMessages.EXCEPTION_FILTER_IS_NULL_8F83BD19);
    // The condition doesn't guarantee that filter is of type boolean, but was found to be a
    // practical way to identify
    // places where JoinNode could be created without appropriate coercions.
    checkArgument(
        !filter.isPresent() || !(filter.get() instanceof NullLiteral),
        QueryMessages.EXCEPTION_FILTER_MUST_BE_AN_EXPRESSION_OF_BOOLEAN_TYPE_COLON_ARG_F358F1A8,
        filter);
    // requireNonNull(leftHashSymbol, "leftHashSymbol is null");
    // requireNonNull(rightHashSymbol, "rightHashSymbol is null");
    requireNonNull(spillable, QueryMessages.EXCEPTION_SPILLABLE_IS_NULL_8226EA70);

    this.joinType = joinType;
    this.leftChild = leftChild;
    this.rightChild = rightChild;
    this.criteria = ImmutableList.copyOf(criteria);
    this.asofCriteria = asofCriteria;
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
        QueryMessages.EXCEPTION_LEFT_SOURCE_INPUTS_DO_NOT_CONTAIN_ALL_LEFT_OUTPUT_SYMBOLS_71459E3B);
    checkArgument(
        rightSymbols.containsAll(rightOutputSymbols),
        QueryMessages
            .EXCEPTION_RIGHT_SOURCE_INPUTS_DO_NOT_CONTAIN_ALL_RIGHT_OUTPUT_SYMBOLS_23EBC024);

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
                QueryMessages
                    .EXCEPTION_EQUALITY_JOIN_CRITERIA_SHOULD_BE_NORMALIZED_ACCORDING_TO_JOIN_SIDES_COLON_ARG_76EB7C14,
                equiJoinClause));
  }

  // only used for deserialize
  public JoinNode(
      PlanNodeId id,
      JoinType joinType,
      List<EquiJoinClause> criteria,
      Optional<AsofJoinClause> asofCriteria,
      List<Symbol> leftOutputSymbols,
      List<Symbol> rightOutputSymbols) {
    super(id);
    requireNonNull(joinType, QueryMessages.EXCEPTION_TYPE_IS_NULL_16A3D3EB);
    requireNonNull(criteria, QueryMessages.EXCEPTION_CRITERIA_IS_NULL_2996D1A3);

    this.leftOutputSymbols = leftOutputSymbols;
    this.rightOutputSymbols = rightOutputSymbols;
    this.filter = Optional.empty();
    this.spillable = Optional.empty();

    this.joinType = joinType;
    this.criteria = criteria;
    this.asofCriteria = asofCriteria;
  }

  /**
   * @return a new JoinNode with the flipped attributes
   */
  public JoinNode flip() {
    return new JoinNode(
        id,
        joinType.flip(),
        rightChild,
        leftChild,
        flipBatch(criteria),
        asofCriteria,
        rightOutputSymbols,
        leftOutputSymbols,
        filter,
        spillable);
  }

  @Override
  public <R, C> R accept(IPlanVisitor<R, C> visitor, C context) {
    return ((ICoreQueryPlanVisitor<R, C>) visitor).visitJoin(this, context);
  }

  @Override
  public PlanNode replaceChildren(List<PlanNode> newChildren) {
    checkArgument(
        newChildren.size() == 2,
        QueryMessages.EXCEPTION_EXPECTED_NEWCHILDREN_TO_CONTAIN_2_NODES_FOR_JOINNODE_BEEC3D82);
    return new JoinNode(
        getPlanNodeId(),
        joinType,
        newChildren.get(0),
        newChildren.get(1),
        criteria,
        asofCriteria,
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
            asofCriteria,
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

    if (asofCriteria.isPresent()) {
      ReadWriteIOUtils.write(true, byteBuffer);
      AsofJoinClause asofJoinClause = asofCriteria.get();
      ReadWriteIOUtils.write(asofJoinClause.getOperator().ordinal(), byteBuffer);
      Symbol.serialize(asofJoinClause.getLeft(), byteBuffer);
      Symbol.serialize(asofJoinClause.getRight(), byteBuffer);
    } else {
      ReadWriteIOUtils.write(false, byteBuffer);
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

    if (asofCriteria.isPresent()) {
      ReadWriteIOUtils.write(true, stream);
      AsofJoinClause asofJoinClause = asofCriteria.get();
      ReadWriteIOUtils.write(asofJoinClause.getOperator().ordinal(), stream);
      Symbol.serialize(asofJoinClause.getLeft(), stream);
      Symbol.serialize(asofJoinClause.getRight(), stream);
    } else {
      ReadWriteIOUtils.write(false, stream);
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

    Optional<AsofJoinClause> asofJoinClause = Optional.empty();
    if (ReadWriteIOUtils.readBool(byteBuffer)) {
      asofJoinClause =
          Optional.of(
              new AsofJoinClause(
                  ComparisonExpression.Operator.values()[ReadWriteIOUtils.readInt(byteBuffer)],
                  Symbol.deserialize(byteBuffer),
                  Symbol.deserialize(byteBuffer)));
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
    return new JoinNode(
        planNodeId, joinType, criteria, asofJoinClause, leftOutputSymbols, rightOutputSymbols);
  }

  public JoinType getJoinType() {
    return joinType;
  }

  public List<EquiJoinClause> getCriteria() {
    return criteria;
  }

  public Optional<AsofJoinClause> getAsofCriteria() {
    return asofCriteria;
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
    return !asofCriteria.isPresent()
        && criteria.isEmpty()
        && !filter.isPresent()
        && joinType == INNER;
  }

  @Override
  public String toString() {
    return "JoinNode-" + this.getPlanNodeId();
  }

  public static class EquiJoinClause {
    private final Symbol left;
    private final Symbol right;

    public EquiJoinClause(Symbol left, Symbol right) {
      this.left = requireNonNull(left, QueryMessages.EXCEPTION_LEFT_IS_NULL_2C1080C5);
      this.right = requireNonNull(right, QueryMessages.EXCEPTION_RIGHT_IS_NULL_97BD6491);
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

    public static List<EquiJoinClause> flipBatch(List<EquiJoinClause> input) {
      ImmutableList.Builder<EquiJoinClause> builder = ImmutableList.builder();
      input.forEach(clause -> builder.add(clause.flip()));
      return builder.build();
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

  public static class AsofJoinClause {
    private final Symbol left;
    private final Symbol right;
    private final ComparisonExpression.Operator operator;

    public AsofJoinClause(ComparisonExpression.Operator operator, Symbol left, Symbol right) {
      this.operator = operator;
      this.left = requireNonNull(left, QueryMessages.EXCEPTION_LEFT_IS_NULL_2C1080C5);
      this.right = requireNonNull(right, QueryMessages.EXCEPTION_RIGHT_IS_NULL_97BD6491);
    }

    public Symbol getLeft() {
      return left;
    }

    public Symbol getRight() {
      return right;
    }

    public ComparisonExpression.Operator getOperator() {
      return operator;
    }

    public ComparisonExpression toExpression() {
      return new ComparisonExpression(
          operator, left.toSymbolReference(), right.toSymbolReference());
    }

    public AsofJoinClause flip() {
      return new AsofJoinClause(operator.flip(), right, left);
    }

    public boolean isOperatorContainsGreater() {
      switch (operator) {
        case GREATER_THAN:
        case GREATER_THAN_OR_EQUAL:
          return true;
        case LESS_THAN:
        case LESS_THAN_OR_EQUAL:
          return false;
        default:
          throw new IllegalArgumentException(
              String.format(QueryMessages.INVALID_OPERATOR_TYPE, operator));
      }
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }

      if (obj == null || !this.getClass().equals(obj.getClass())) {
        return false;
      }

      AsofJoinClause other = (AsofJoinClause) obj;

      return Objects.equals(this.operator, other.operator)
          && Objects.equals(this.left, other.left)
          && Objects.equals(this.right, other.right);
    }

    @Override
    public int hashCode() {
      return Objects.hash(operator, left, right);
    }

    @Override
    public String toString() {
      return format("%s %s %s", left, operator.getValue(), right);
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

    public JoinType flip() {
      switch (this) {
        case INNER:
          return INNER;
        case FULL:
          return FULL;
        case LEFT:
          return RIGHT;
        case RIGHT:
          return LEFT;
        default:
      }
      throw new IllegalArgumentException(String.format(QueryMessages.UNSUPPORTED_JOIN_TYPE, this));
    }
  }
}
