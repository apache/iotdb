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

import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.Rule;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationTableScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.DeviceTableScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.OffsetNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TableScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Capture;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Captures;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Pattern;

import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.Patterns.offset;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.Patterns.source;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.Patterns.tableScan;
import static org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Capture.newCapture;

/**
 * <b>Optimization phase:</b> Distributed plan planning.
 *
 * <p>The OFFSET can be eliminated when the following conditions are met:
 * <li>Its child is DeviceTableScan, which means there OFFSET is effect on only one region
 * <li>The query expressions are all scalar expression.
 */
public class PushDownOffsetIntoTableScan implements Rule<OffsetNode> {
  private static final Capture<TableScanNode> CHILD = newCapture();

  private static final Pattern<OffsetNode> PATTERN =
      offset().with(source().matching(tableScan().capturedAs(CHILD)));

  @Override
  public Pattern<OffsetNode> getPattern() {
    return PATTERN;
  }

  @Override
  public Result apply(OffsetNode parent, Captures captures, Context context) {
    TableScanNode tableScanNode = captures.get(CHILD);
    if ((tableScanNode instanceof DeviceTableScanNode
            && !(tableScanNode instanceof AggregationTableScanNode))
        && !((DeviceTableScanNode) tableScanNode).isPushLimitToEachDevice()) {
      tableScanNode.setPushDownOffset(parent.getCount());
      // consider case that there is no limit
      tableScanNode.setPushDownLimit(
          tableScanNode.getPushDownLimit() == 0
              ? 0
              : tableScanNode.getPushDownLimit() - parent.getCount());
      return Result.ofPlanNode(tableScanNode);
    }

    return Result.empty();
  }
}
