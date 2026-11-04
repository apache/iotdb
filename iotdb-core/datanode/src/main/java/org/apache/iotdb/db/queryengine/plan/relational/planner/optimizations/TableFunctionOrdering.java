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

package org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations;

import org.apache.iotdb.commons.queryengine.plan.relational.function.TableBuiltinTableFunction;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.OrderingScheme;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.SortOrder;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.TableFunctionNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.TableFunctionProcessorNode;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/** Ordering guarantees for columns copied from a table function's input. */
public final class TableFunctionOrdering {

  private TableFunctionOrdering() {}

  public static boolean preservesInputOrder(TableFunctionProcessorNode node) {
    // A row-semantics declaration permits splitting the input, but does not constrain the order
    // in which a custom processor emits pass-through indices (including buffered output in
    // finish()). Only use an explicit guarantee from the built-in function registry.
    return TableBuiltinTableFunction.preservesInputOrder(node.getName());
  }

  public static Set<Symbol> getOrderPreservingSymbols(TableFunctionProcessorNode node) {
    boolean preservesRowOrder = preservesInputOrder(node);
    // Other functions may reorder rows within a partition. Partitions are processed sequentially,
    // so their partitioning columns still preserve a proven input ordering. Function-produced
    // columns have no inferred order.
    return node.getPassThroughSpecification()
        .map(
            specification ->
                specification.getColumns().stream()
                    .filter(column -> preservesRowOrder || column.isPartitioningColumn())
                    .map(TableFunctionNode.PassThroughColumn::getSymbol)
                    .collect(Collectors.toSet()))
        .orElseGet(Collections::emptySet);
  }

  public static OrderingScheme retainOrderingPrefix(
      OrderingScheme ordering, Set<Symbol> retainedSymbols) {
    if (ordering == null) {
      return null;
    }
    Map<Symbol, SortOrder> prefix = new LinkedHashMap<>();
    for (Symbol symbol : ordering.getOrderBy()) {
      if (!retainedSymbols.contains(symbol)) {
        break;
      }
      prefix.put(symbol, ordering.getOrdering(symbol));
    }
    if (prefix.isEmpty()) {
      return null;
    }
    return new OrderingScheme(ordering.getOrderBy().subList(0, prefix.size()), prefix);
  }
}
