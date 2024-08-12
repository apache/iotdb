package org.apache.iotdb.db.queryengine.plan.relational.planner.node;

import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.MultiChildProcessNode;
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

public class JoinNode extends MultiChildProcessNode {

  private final JoinType type;
  private final PlanNode left;
  private final PlanNode right;
  private final List<EquiJoinClause> criteria;
  private final List<Symbol> leftOutputSymbols;
  private final List<Symbol> rightOutputSymbols;
  private final boolean maySkipOutputDuplicates;
  private final Optional<Expression> filter;
  private final Optional<Symbol> leftHashSymbol;
  private final Optional<Symbol> rightHashSymbol;
  // private final Optional<DistributionType> distributionType;
  private final Optional<Boolean> spillable;

  // private final Map<DynamicFilterId, Symbol> dynamicFilters;

  @JsonCreator
  public JoinNode(
      @JsonProperty("id") PlanNodeId id,
      @JsonProperty("type") JoinType type,
      @JsonProperty("left") PlanNode left,
      @JsonProperty("right") PlanNode right,
      @JsonProperty("criteria") List<EquiJoinClause> criteria,
      @JsonProperty("leftOutputSymbols") List<Symbol> leftOutputSymbols,
      @JsonProperty("rightOutputSymbols") List<Symbol> rightOutputSymbols,
      @JsonProperty("maySkipOutputDuplicates") boolean maySkipOutputDuplicates,
      @JsonProperty("filter") Optional<Expression> filter,
      @JsonProperty("leftHashSymbol") Optional<Symbol> leftHashSymbol,
      @JsonProperty("rightHashSymbol") Optional<Symbol> rightHashSymbol,
      // @JsonProperty("distributionType") Optional<DistributionType> distributionType,
      @JsonProperty("spillable") Optional<Boolean> spillable)
        // @JsonProperty("dynamicFilters") Map<DynamicFilterId, Symbol> dynamicFilters,
        // @JsonProperty("reorderJoinStatsAndCost") Optional<PlanNodeStatsAndCostSummary>
        // reorderJoinStatsAndCost)
      {
    super(id);
    requireNonNull(type, "type is null");
    requireNonNull(left, "left is null");
    requireNonNull(right, "right is null");
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
    // requireNonNull(distributionType, "distributionType is null");
    requireNonNull(spillable, "spillable is null");

    this.type = type;
    this.left = left;
    this.right = right;
    this.criteria = ImmutableList.copyOf(criteria);
    this.leftOutputSymbols = ImmutableList.copyOf(leftOutputSymbols);
    this.rightOutputSymbols = ImmutableList.copyOf(rightOutputSymbols);
    this.maySkipOutputDuplicates = maySkipOutputDuplicates;
    this.filter = filter;
    this.leftHashSymbol = leftHashSymbol;
    this.rightHashSymbol = rightHashSymbol;
    // this.distributionType = distributionType;
    this.spillable = spillable;
    // this.dynamicFilters = ImmutableMap.copyOf(requireNonNull(dynamicFilters, "dynamicFilters is
    // null"));
    // this.reorderJoinStatsAndCost = requireNonNull(reorderJoinStatsAndCost,
    // "reorderJoinStatsAndCost is null");

    Set<Symbol> leftSymbols = ImmutableSet.copyOf(left.getOutputSymbols());
    Set<Symbol> rightSymbols = ImmutableSet.copyOf(right.getOutputSymbols());

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

    //        if (distributionType.isPresent()) {
    //            // The implementation of full outer join only works if the data is hash
    // partitioned.
    //            checkArgument(
    //                    !(distributionType.get() == REPLICATED && (type == RIGHT || type ==
    // FULL)),
    //                    "%s join do not work with %s distribution type",
    //                    type,
    //                    distributionType.get());
    //        }

    //        for (Symbol symbol : dynamicFilters.values()) {
    //            checkArgument(rightSymbols.contains(symbol), "Right join input doesn't contain
    // symbol for dynamic filter: %s", symbol);
    //        }
  }

  @Override
  public PlanNode clone() {
    return null;
  }

  @Override
  public List<String> getOutputColumnNames() {
    throw new IllegalStateException();
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {}

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {}

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
