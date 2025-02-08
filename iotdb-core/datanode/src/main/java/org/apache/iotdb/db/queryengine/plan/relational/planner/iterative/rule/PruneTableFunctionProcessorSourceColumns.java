package org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule;

import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.Rule;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TableFunctionNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TableFunctionProcessorNode;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Captures;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Pattern;

import com.google.common.collect.ImmutableSet;

import java.util.Optional;

import static org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.Util.restrictOutputs;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.Patterns.tableFunctionProcessor;

/**
 * This rule prunes unreferenced outputs of TableFunctionProcessorNode. Any source output symbols
 * not included in the required symbols can be pruned.
 *
 * <p>First, it extracts all symbols required for:
 *
 * <ul>
 *   <li>- pass-through
 *   <li>- table function computation
 *   <li>- partitioning and ordering (including the hashSymbol)
 * </ul>
 *
 * <p>Next, a mapping of input symbols to marker symbols is updated so that it only contains
 * mappings for the required symbols.
 */
public class PruneTableFunctionProcessorSourceColumns implements Rule<TableFunctionProcessorNode> {
  private static final Pattern<TableFunctionProcessorNode> PATTERN = tableFunctionProcessor();

  @Override
  public Pattern<TableFunctionProcessorNode> getPattern() {
    return PATTERN;
  }

  @Override
  public Result apply(TableFunctionProcessorNode node, Captures captures, Context context) {
    if (node.getChildren().isEmpty() || !node.getPassThroughSpecification().isPresent()) {
      return Result.empty();
    }

    ImmutableSet.Builder<Symbol> requiredInputs = ImmutableSet.builder();
    for (TableFunctionNode.PassThroughColumn column :
        node.getPassThroughSpecification().get().getColumns()) {
      requiredInputs.add(column.getSymbol());
    }
    node.getRequiredSymbols().forEach(requiredInputs::add);

    node.getDataOrganizationSpecification()
        .ifPresent(
            specification -> {
              requiredInputs.addAll(specification.getPartitionBy());
              specification
                  .getOrderingScheme()
                  .ifPresent(orderingScheme -> requiredInputs.addAll(orderingScheme.getOrderBy()));
            });

    return restrictOutputs(context.getIdAllocator(), node.getChild(), requiredInputs.build())
        .map(
            child ->
                Result.ofPlanNode(
                    new TableFunctionProcessorNode(
                        node.getPlanNodeId(),
                        node.getName(),
                        node.getProperOutputs(),
                        Optional.of(child),
                        node.isPruneWhenEmpty(),
                        node.getPassThroughSpecification(),
                        node.getRequiredSymbols(),
                        node.getDataOrganizationSpecification(),
                        node.getArguments())))
        .orElse(Result.empty());
  }
}
