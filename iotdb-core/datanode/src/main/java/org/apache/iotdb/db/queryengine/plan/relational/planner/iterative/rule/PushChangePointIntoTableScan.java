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

import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.Rule;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ChangePointNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ChangePointTableScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.DeviceTableScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Capture;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Captures;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Pattern;

import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.Patterns.changePoint;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.Patterns.source;
import static org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Capture.newCapture;
import static org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Pattern.typeOf;

/**
 * Pushes a ChangePointNode into a DeviceTableScanNode, creating a ChangePointTableScanNode that
 * can leverage TsFile statistics for optimization.
 */
public class PushChangePointIntoTableScan implements Rule<ChangePointNode> {

  private static final Capture<DeviceTableScanNode> TABLE_SCAN_CAPTURE = newCapture();

  private final Pattern<ChangePointNode> pattern;

  public PushChangePointIntoTableScan() {
    this.pattern =
        changePoint()
            .with(
                source()
                    .matching(
                        typeOf(DeviceTableScanNode.class)
                            .matching(
                                scan -> !(scan instanceof ChangePointTableScanNode))
                            .capturedAs(TABLE_SCAN_CAPTURE)));
  }

  @Override
  public Pattern<ChangePointNode> getPattern() {
    return pattern;
  }

  @Override
  public Result apply(ChangePointNode changePointNode, Captures captures, Context context) {
    DeviceTableScanNode scanNode = captures.get(TABLE_SCAN_CAPTURE);

    PlanNode merged =
        ChangePointTableScanNode.combineChangePointAndTableScan(changePointNode, scanNode);
    return Result.ofPlanNode(merged);
  }
}
