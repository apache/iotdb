package org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule;

import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.db.queryengine.plan.relational.planner.SymbolsExtractor;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.WindowNode;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.Util.restrictOutputs;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.Patterns.window;

public class PruneWindowColumns extends ProjectOffPushDownRule<WindowNode> {
  public PruneWindowColumns() {
    super(window());
  }

  @Override
  protected Optional<PlanNode> pushDownProjectOff(
      Context context, WindowNode windowNode, Set<Symbol> referencedOutputs) {
    Map<Symbol, WindowNode.Function> referencedFunctions =
        Maps.filterKeys(windowNode.getWindowFunctions(), referencedOutputs::contains);

    if (referencedFunctions.isEmpty()) {
      return Optional.of(windowNode.getChild());
    }

    ImmutableSet.Builder<Symbol> referencedInputs =
        ImmutableSet.<Symbol>builder()
            .addAll(
                windowNode.getChild().getOutputSymbols().stream()
                    .filter(referencedOutputs::contains)
                    .iterator())
            .addAll(windowNode.getSpecification().getPartitionBy());

    windowNode
        .getSpecification()
        .getOrderingScheme()
        .ifPresent(orderingScheme -> orderingScheme.getOrderBy().forEach(referencedInputs::add));
    windowNode.getHashSymbol().ifPresent(referencedInputs::add);

    for (WindowNode.Function windowFunction : referencedFunctions.values()) {
      referencedInputs.addAll(SymbolsExtractor.extractUnique(windowFunction));
    }

    PlanNode prunedWindowNode =
        new WindowNode(
            windowNode.getPlanNodeId(),
            restrictOutputs(
                    context.getIdAllocator(), windowNode.getChild(), referencedInputs.build())
                .orElse(windowNode.getChild()),
            windowNode.getSpecification(),
            referencedFunctions,
            windowNode.getHashSymbol(),
            windowNode.getPrePartitionedInputs(),
            windowNode.getPreSortedOrderPrefix());

    if (prunedWindowNode.getOutputSymbols().size() == windowNode.getOutputSymbols().size()) {
      // Neither function pruning nor input pruning was successful.
      return Optional.empty();
    }

    return Optional.of(prunedWindowNode);
  }
}
