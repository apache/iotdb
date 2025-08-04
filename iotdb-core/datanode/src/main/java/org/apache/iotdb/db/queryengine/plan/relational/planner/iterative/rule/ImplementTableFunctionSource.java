/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule;

import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.OrderingScheme;
import org.apache.iotdb.db.queryengine.plan.relational.planner.SortOrder;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.Rule;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.GroupNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.Patterns;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TableFunctionNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TableFunctionProcessorNode;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Captures;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Pattern;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.collect.Iterables.getOnlyElement;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.SortOrder.ASC_NULLS_LAST;

/**
 * This rule prepares cartesian product of partitions from all inputs of table function.
 *
 * <p>It rewrites TableFunctionNode with potentially many sources into a TableFunctionProcessorNode.
 * The new node has one source being a combination of the original sources.
 */
public class ImplementTableFunctionSource implements Rule<TableFunctionNode> {

  private static final Pattern<TableFunctionNode> PATTERN = Patterns.tableFunction();

  public ImplementTableFunctionSource() {}

  @Override
  public Pattern<TableFunctionNode> getPattern() {
    return PATTERN;
  }

  @Override
  public Result apply(TableFunctionNode node, Captures captures, Context context) {
    if (node.getChildren().isEmpty()) {
      return Result.ofPlanNode(
          new TableFunctionProcessorNode(
              node.getPlanNodeId(),
              node.getName(),
              node.getProperOutputs(),
              Optional.empty(),
              Optional.empty(),
              ImmutableList.of(),
              Optional.empty(),
              false,
              node.getTableFunctionHandle(),
              false));
    } else if (node.getChildren().size() == 1) {
      // Single source does not require pre-processing.
      // If the source has row semantics, its specification is empty.
      // If the source has set semantics, its specification is present, even if there is no
      // partitioning or ordering specified.
      // This property can be used later to choose optimal distribution.
      TableFunctionNode.TableArgumentProperties sourceProperties =
          getOnlyElement(node.getTableArgumentProperties());
      AtomicReference<PlanNode> child = new AtomicReference<>(getOnlyElement(node.getChildren()));

      // generate sort node
      sourceProperties
          .getDataOrganizationSpecification()
          .ifPresent(
              dataOrganizationSpecification -> {
                List<Symbol> sortSymbols = new ArrayList<>();
                Map<Symbol, SortOrder> sortOrderings = new HashMap<>();
                for (Symbol symbol : dataOrganizationSpecification.getPartitionBy()) {
                  sortSymbols.add(symbol);
                  sortOrderings.put(symbol, ASC_NULLS_LAST);
                }
                int sortKeyOffset = sortSymbols.size();
                dataOrganizationSpecification
                    .getOrderingScheme()
                    .ifPresent(
                        orderingScheme -> {
                          for (Symbol symbol : orderingScheme.getOrderBy()) {
                            if (!sortOrderings.containsKey(symbol)) {
                              sortSymbols.add(symbol);
                              sortOrderings.put(symbol, orderingScheme.getOrdering(symbol));
                            }
                          }
                        });
                child.set(
                    new GroupNode(
                        context.getIdAllocator().genPlanNodeId(),
                        child.get(),
                        new OrderingScheme(sortSymbols, sortOrderings),
                        sortKeyOffset));
              });
      return Result.ofPlanNode(
          new TableFunctionProcessorNode(
              node.getPlanNodeId(),
              node.getName(),
              node.getProperOutputs(),
              Optional.of(child.get()),
              Optional.ofNullable(sourceProperties.getPassThroughSpecification()),
              sourceProperties.getRequiredColumns(),
              sourceProperties.getDataOrganizationSpecification(),
              sourceProperties.isRowSemantics(),
              node.getTableFunctionHandle(),
              sourceProperties.isRequireRecordSnapshot()));
    } else {
      // we don't support multiple source now.
      throw new IllegalArgumentException("table function does not support multiple source now.");
    }
  }
}
