/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.db.queryengine.plan.relational.planner.distribute;

import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TableScanNode;

import java.util.Collections;
import java.util.List;

public class ExchangeNodeGenerator
    extends SimplePlanRewriter<ExchangeNodeGenerator.DistributionPlanContext> {

  @Override
  public List<PlanNode> visitTableScan(
      TableScanNode node, ExchangeNodeGenerator.DistributionPlanContext context) {
    // TODO process that the data of TableScanNode locates in multi data regions
    return Collections.singletonList(node);
  }

  public static class DistributionPlanContext {}
}
