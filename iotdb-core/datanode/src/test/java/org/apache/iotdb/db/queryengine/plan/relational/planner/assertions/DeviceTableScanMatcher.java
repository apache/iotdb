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
package org.apache.iotdb.db.queryengine.plan.relational.planner.assertions;

import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.DeviceTableScanNode;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.MoreObjects.toStringHelper;

public class DeviceTableScanMatcher extends TableScanMatcher {

  public DeviceTableScanMatcher(
      String expectedTableName,
      Optional<Boolean> hasTableLayout,
      List<String> outputSymbols,
      Set<String> assignmentsKeys) {
    super(expectedTableName, hasTableLayout, outputSymbols, assignmentsKeys);
  }

  @Override
  public boolean shapeMatches(PlanNode node) {
    return node instanceof DeviceTableScanNode;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .omitNullValues()
        .add("expectedTableName", expectedTableName)
        .add("hasTableLayout", hasTableLayout.orElse(null))
        .add("outputSymbols", outputSymbols)
        .add("assignmentsKeys", assignmentsKeys)
        .toString();
  }
}
