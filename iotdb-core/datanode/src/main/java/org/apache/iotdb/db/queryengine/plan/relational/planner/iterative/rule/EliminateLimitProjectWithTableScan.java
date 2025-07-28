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
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.DeviceTableScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.LimitNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ProjectNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TableScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Capture;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Captures;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Pattern;

import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.Patterns.limit;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.Patterns.project;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.Patterns.source;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.Patterns.tableScan;
import static org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Capture.newCapture;

public class EliminateLimitProjectWithTableScan implements Rule<LimitNode> {
  private static final Capture<ProjectNode> PROJECT = newCapture();
  private static final Capture<TableScanNode> TABLE_SCAN_NODE = newCapture();

  private static final Pattern<LimitNode> PATTERN =
      limit()
          .with(
              source()
                  .matching(
                      project()
                          .capturedAs(PROJECT)
                          // .matching(ProjectNode::isIdentity)
                          .with(source().matching(tableScan().capturedAs(TABLE_SCAN_NODE)))));

  @Override
  public Pattern<LimitNode> getPattern() {
    return PATTERN;
  }

  @Override
  public Rule.Result apply(LimitNode parent, Captures captures, Rule.Context context) {
    ProjectNode projectNode = captures.get(PROJECT);
    TableScanNode tableScanNode = captures.get(TABLE_SCAN_NODE);

    if (tableScanNode instanceof DeviceTableScanNode
        && ((DeviceTableScanNode) tableScanNode).isPushLimitToEachDevice()) {
      return Rule.Result.empty();
    }

    if (parent.getCount() == tableScanNode.getPushDownLimit()) {
      return Rule.Result.ofPlanNode(projectNode);
    } else {
      return Rule.Result.empty();
    }
  }
}
