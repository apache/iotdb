package org.apache.iotdb.db.queryengine.plan.relational.planner.node;

import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.TwoChildProcessNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ComparisonExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.NullLiteral;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class JoinNode extends TwoChildProcessNode {

  private final JoinType joinType;
  private final List<EquiJoinClause> criteria;
  private final List<Symbol> leftOutputSymbols;
  private final List<Symbol> rightOutputSymbols;
  private final boolean maySkipOutputDuplicates;
  // some filter like 'a.xx_column < b.yy_column'
  private final Optional<Expression> filter;
  private final Optional<Symbol> leftHashSymbol;
  private final Optional<Symbol> rightHashSymbol;
  private final Optional<Boolean> spillable;

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
      boolean maySkipOutputDuplicates,
      Optional<Expression> filter,
      Optional<Symbol> leftHashSymbol,
      Optional<Symbol> rightHashSymbol,
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
    requireNonNull(leftHashSymbol, "leftHashSymbol is null");
    requireNonNull(rightHashSymbol, "rightHashSymbol is null");
    requireNonNull(spillable, "spillable is null");

    this.joinType = joinType;
    this.leftChild = leftChild;
    this.rightChild = rightChild;
    this.criteria = ImmutableList.copyOf(criteria);
    this.leftOutputSymbols = ImmutableList.copyOf(leftOutputSymbols);
    this.rightOutputSymbols = ImmutableList.copyOf(rightOutputSymbols);
    this.maySkipOutputDuplicates = maySkipOutputDuplicates;
    this.filter = filter;
    this.leftHashSymbol = leftHashSymbol;
    this.rightHashSymbol = rightHashSymbol;
    this.spillable = spillable;

    Set<Symbol> leftSymbols = ImmutableSet.copyOf(leftChild.getOutputSymbols());
    Set<Symbol> rightSymbols = ImmutableSet.copyOf(rightChild.getOutputSymbols());

    checkArgument(
        leftSymbols.containsAll(leftOutputSymbols),
        "Left source inputs do not contain all left output symbols");
    checkArgument(
        rightSymbols.containsAll(rightOutputSymbols),
        "Right source inputs do not contain all right output symbols");

    checkArgument(
        !(criteria.isEmpty() && leftHashSymbol.isPresent()),
        "Left hash symbol is only valid in an equijoin");
    checkArgument(
        !(criteria.isEmpty() && rightHashSymbol.isPresent()),
        "Right hash symbol is only valid in an equijoin");

    criteria.forEach(
        equiJoinClause ->
            checkArgument(
                leftSymbols.contains(equiJoinClause.getLeft())
                    && rightSymbols.contains(equiJoinClause.getRight()),
                "Equality join criteria should be normalized according to join sides: %s",
                equiJoinClause));
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
        maySkipOutputDuplicates,
        filter,
        leftHashSymbol,
        rightHashSymbol,
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
            maySkipOutputDuplicates,
            filter,
            leftHashSymbol,
            rightHashSymbol,
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
  protected void serializeAttributes(ByteBuffer byteBuffer) {}

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {}

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

  public boolean isMaySkipOutputDuplicates() {
    return maySkipOutputDuplicates;
  }

  public Optional<Expression> getFilter() {
    return filter;
  }

  public Optional<Symbol> getLeftHashSymbol() {
    return leftHashSymbol;
  }

  public Optional<Symbol> getRightHashSymbol() {
    return rightHashSymbol;
  }

  public Optional<Boolean> isSpillable() {
    return spillable;
  }

  public static class EquiJoinClause {
    private final Symbol left;
    private final Symbol right;

    @JsonCreator
    public EquiJoinClause(@JsonProperty("left") Symbol left, @JsonProperty("right") Symbol right) {
      this.left = requireNonNull(left, "left is null");
      this.right = requireNonNull(right, "right is null");
    }

    @JsonProperty("left")
    public Symbol getLeft() {
      return left;
    }

    @JsonProperty("right")
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
