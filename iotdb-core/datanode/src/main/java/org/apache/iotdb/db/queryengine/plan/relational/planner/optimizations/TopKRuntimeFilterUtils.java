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

import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.OrderingScheme;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.TopKNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.DeviceTableScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ExchangeNode;

import java.util.concurrent.atomic.AtomicBoolean;

public final class TopKRuntimeFilterUtils {

  private TopKRuntimeFilterUtils() {}

  public static boolean isOrderByTimeOnly(OrderingScheme orderingScheme) {
    if (orderingScheme.getOrderBy().size() != 1) {
      return false;
    }
    Symbol orderBy = orderingScheme.getOrderBy().get(0);
    return "time".equalsIgnoreCase(orderBy.getName());
  }

  public static boolean qualifiesForRuntimeFilter(TopKNode topKNode) {
    AtomicBoolean hasTableScan = new AtomicBoolean(false);
    AtomicBoolean hasExchange = new AtomicBoolean(false);
    PlanVisitor<Void, Void> subtreeDetector =
        new PlanVisitor<Void, Void>() {
          @Override
          public Void visitPlan(PlanNode node, Void unused) {
            for (PlanNode child : node.getChildren()) {
              child.accept(this, null);
            }
            return null;
          }

          @Override
          public Void visitDeviceTableScan(DeviceTableScanNode scanNode, Void unused) {
            hasTableScan.set(true);
            return null;
          }

          @Override
          public Void visitTableExchange(ExchangeNode exchangeNode, Void unused) {
            hasExchange.set(true);
            return null;
          }
        };
    for (PlanNode child : topKNode.getChildren()) {
      child.accept(subtreeDetector, null);
    }
    return hasTableScan.get() && !hasExchange.get();
  }
}
