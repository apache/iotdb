package org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule;

import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TableFunctionNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TableFunctionProcessorNode;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.Patterns.tableFunctionProcessor;

/**
 * TableFunctionProcessorNode has two kinds of outputs:
 *
 * <ul>
 *   <li>- proper outputs, which are the columns produced by the table function,
 *   <li>- pass-through outputs, which are the columns copied from table arguments.
 * </ul>
 *
 * <p>This rule filters out unreferenced pass-through symbols. Unreferenced proper symbols are not
 * pruned, because there is currently no way to communicate to the table function the request for
 * not producing certain columns.
 */
// TODO(UDF): prune table function's proper outputs
public class PruneTableFunctionProcessorColumns
    extends ProjectOffPushDownRule<TableFunctionProcessorNode> {
  public PruneTableFunctionProcessorColumns() {
    super(tableFunctionProcessor());
  }

  @Override
  protected Optional<PlanNode> pushDownProjectOff(
      Context context, TableFunctionProcessorNode node, Set<Symbol> referencedOutputs) {
    Optional<TableFunctionNode.PassThroughSpecification> prunedPassThroughSpecifications =
        node.getPassThroughSpecification()
            .map(
                sourceSpecification -> {
                  List<TableFunctionNode.PassThroughColumn> prunedPassThroughColumns =
                      sourceSpecification.getColumns().stream()
                          .filter(column -> referencedOutputs.contains(column.getSymbol()))
                          .collect(toImmutableList());
                  return new TableFunctionNode.PassThroughSpecification(
                      sourceSpecification.isDeclaredAsPassThrough(), prunedPassThroughColumns);
                });
    if (!prunedPassThroughSpecifications.isPresent()) {
      return Optional.empty();
    }
    int originalPassThroughCount = node.getPassThroughSpecification().get().getColumns().size();
    int prunedPassThroughCount = prunedPassThroughSpecifications.get().getColumns().size();
    if (originalPassThroughCount == prunedPassThroughCount) {
      return Optional.empty();
    }

    return Optional.of(
        new TableFunctionProcessorNode(
            node.getPlanNodeId(),
            node.getName(),
            node.getProperOutputs(),
            Optional.ofNullable(node.getChild()),
            node.isPruneWhenEmpty(),
            prunedPassThroughSpecifications,
            node.getRequiredSymbols(),
            node.getDataOrganizationSpecification(),
            node.getArguments()));
  }
}
