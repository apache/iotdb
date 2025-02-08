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
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.Patterns;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.SortNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TableFunctionNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TableFunctionProcessorNode;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Captures;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Pattern;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.collect.Iterables.getOnlyElement;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.SortOrder.ASC_NULLS_LAST;

/**
 * This rule prepares cartesian product of partitions from all inputs of table function.
 *
 * <p>It rewrites TableFunctionNode with potentially many sources into a TableFunctionProcessorNode.
 * The new node has one source being a combination of the original sources.
 *
 * <p>The original sources are combined with joins. The join conditions depend on the prune when
 * empty property, and on the co-partitioning of sources.
 *
 * <p>The resulting source should be partitioned and ordered according to combined schemas from the
 * component sources.
 *
 * <p>Example transformation for two sources, both with set semantics and KEEP WHEN EMPTY property:
 *
 * <pre>{@code
 * - TableFunction foo
 *      - source T1(a1, b1) PARTITION BY a1 ORDER BY b1
 *      - source T2(a2, b2) PARTITION BY a2
 * }</pre>
 *
 * Is transformed into:
 *
 * <pre>{@code
 * - TableFunctionProcessor foo
 *      PARTITION BY (a1, a2), ORDER BY combined_row_number
 *      - Project
 *          marker_1 <= IF(table1_row_number = combined_row_number, table1_row_number, CAST(null AS bigint))
 *          marker_2 <= IF(table2_row_number = combined_row_number, table2_row_number, CAST(null AS bigint))
 *          - Project
 *              combined_row_number <= IF(COALESCE(table1_row_number, BIGINT '-1') > COALESCE(table2_row_number, BIGINT '-1'), table1_row_number, table2_row_number)
 *              combined_partition_size <= IF(COALESCE(table1_partition_size, BIGINT '-1') > COALESCE(table2_partition_size, BIGINT '-1'), table1_partition_size, table2_partition_size)
 *              - FULL Join
 *                  [table1_row_number = table2_row_number OR
 *                   table1_row_number > table2_partition_size AND table2_row_number = BIGINT '1' OR
 *                   table2_row_number > table1_partition_size AND table1_row_number = BIGINT '1']
 *                  - Window [PARTITION BY a1 ORDER BY b1]
 *                      table1_row_number <= row_number()
 *                      table1_partition_size <= count()
 *                          - source T1(a1, b1)
 *                  - Window [PARTITION BY a2]
 *                      table2_row_number <= row_number()
 *                      table2_partition_size <= count()
 *                          - source T2(a2, b2)
 * }</pre>
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
              false,
              Optional.empty(),
              ImmutableList.of(),
              Optional.empty(),
              node.getArguments()));
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
                ImmutableList.Builder<Symbol> orderBy = ImmutableList.builder();
                ImmutableMap.Builder<Symbol, SortOrder> orderings = ImmutableMap.builder();
                for (Symbol symbol : dataOrganizationSpecification.getPartitionBy()) {
                  orderBy.add(symbol);
                  orderings.put(symbol, ASC_NULLS_LAST);
                }
                dataOrganizationSpecification
                    .getOrderingScheme()
                    .ifPresent(
                        orderingScheme -> {
                          orderBy.addAll(orderingScheme.getOrderBy());
                          orderings.putAll(orderingScheme.getOrderings());
                        });
                child.set(
                    new SortNode(
                        context.getIdAllocator().genPlanNodeId(),
                        child.get(),
                        new OrderingScheme(orderBy.build(), orderings.build()),
                        false,
                        false));
              });
      return Result.ofPlanNode(
          new TableFunctionProcessorNode(
              node.getPlanNodeId(),
              node.getName(),
              node.getProperOutputs(),
              Optional.of(child.get()),
              sourceProperties.isPruneWhenEmpty(),
              Optional.ofNullable(sourceProperties.getPassThroughSpecification()),
              sourceProperties.getRequiredColumns(),
              sourceProperties.getDataOrganizationSpecification(),
              node.getArguments()));
    } else {
      // TODO(UDF): we dont support multiple source now.
      throw new IllegalArgumentException("table function does not support multiple source now.");
    }
  }
}
