package org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule;

import org.apache.iotdb.db.queryengine.plan.relational.function.BoundSignature;
import org.apache.iotdb.db.queryengine.plan.relational.function.FunctionId;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ResolvedFunction;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Assignments;
import org.apache.iotdb.db.queryengine.plan.relational.planner.PlannerContext;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.Rule;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ProjectNode;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SymbolReference;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Capture;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Captures;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Pattern;

import com.google.common.collect.ImmutableList;
import org.apache.tsfile.read.common.type.LongType;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;
import static org.apache.iotdb.db.queryengine.plan.relational.function.FunctionKind.AGGREGATE;
import static org.apache.iotdb.db.queryengine.plan.relational.metadata.FunctionNullability.getAggregationFunctionNullability;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.IrExpressionInterpreter.evaluateConstantExpression;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.ir.IrUtils.isEffectivelyLiteral;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.Patterns.aggregation;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.Patterns.project;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.Patterns.source;
import static org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Capture.newCapture;
import static org.apache.iotdb.db.utils.constant.SqlConstant.COUNT;

public class SimplifyCountOverConstant implements Rule<AggregationNode> {
  private static final Capture<ProjectNode> CHILD = newCapture();

  private static final Pattern<AggregationNode> PATTERN =
      aggregation().with(source().matching(project().capturedAs(CHILD)));

  private final PlannerContext plannerContext;

  public SimplifyCountOverConstant(PlannerContext plannerContext) {
    this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
  }

  @Override
  public Pattern<AggregationNode> getPattern() {
    return PATTERN;
  }

  @Override
  public Result apply(AggregationNode parent, Captures captures, Context context) {
    ProjectNode child = captures.get(CHILD);

    boolean changed = false;
    Map<Symbol, AggregationNode.Aggregation> aggregations = null;
    ResolvedFunction countWildcardFunction = null;

    for (Map.Entry<Symbol, AggregationNode.Aggregation> entry :
        parent.getAggregations().entrySet()) {
      Symbol symbol = entry.getKey();
      AggregationNode.Aggregation aggregation = entry.getValue();

      if (isCountOverConstant(context, aggregation, child.getAssignments())) {
        changed = true;

        if (countWildcardFunction == null) {
          aggregations = new LinkedHashMap<>(parent.getAggregations());
          countWildcardFunction =
              new ResolvedFunction(
                  new BoundSignature(COUNT, LongType.INT64, Collections.emptyList()),
                  FunctionId.NOOP_FUNCTION_ID,
                  AGGREGATE,
                  true,
                  getAggregationFunctionNullability(1));
        }

        aggregations.put(
            symbol,
            new AggregationNode.Aggregation(
                countWildcardFunction,
                ImmutableList.of(),
                false,
                Optional.empty(),
                Optional.empty(),
                aggregation.getMask()));
      }
    }

    if (!changed) {
      return Result.empty();
    }

    return Result.ofPlanNode(
        AggregationNode.builderFrom(parent)
            .setSource(child)
            .setAggregations(aggregations)
            .setPreGroupedSymbols(ImmutableList.of())
            .build());
  }

  private boolean isCountOverConstant(
      Context context, AggregationNode.Aggregation aggregation, Assignments inputs) {
    BoundSignature signature = aggregation.getResolvedFunction().getSignature();
    if (!signature.getName().equals(COUNT) || signature.getArgumentTypes().size() != 1) {
      return false;
    }

    Expression argument = aggregation.getArguments().get(0);
    if (argument instanceof SymbolReference) {
      argument = inputs.get(Symbol.from(argument));
    }

    if (isEffectivelyLiteral(argument, plannerContext, context.getSessionInfo())) {
      Object value = evaluateConstantExpression(argument, plannerContext, context.getSessionInfo());
      verify(!(value instanceof Expression));
      return value != null;
    }

    return false;
  }
}
