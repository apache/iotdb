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
                        node.getPassThroughSpecification(),
                        node.getRequiredSymbols(),
                        node.getDataOrganizationSpecification(),
                        node.isRowSemantic(),
                        node.getTableFunctionHandle(),
                        node.isRequireRecordSnapshot())))
        .orElse(Result.empty());
  }
}
