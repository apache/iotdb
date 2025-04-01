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

package org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped;

import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.AggregationMask;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.RamUsageEstimator;

public class GroupedApproxCountDistinctAccumulator implements GroupedAccumulator {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(GroupedApproxCountDistinctAccumulator.class);
  private final TSDataType seriesDataType;

  public GroupedApproxCountDistinctAccumulator(TSDataType seriesDataType) {
    this.seriesDataType = seriesDataType;
  }

  @Override
  public long getEstimatedSize() {
    return INSTANCE_SIZE;
  }

  @Override
  public void setGroupCount(long groupCount) {}

  @Override
  public void addInput(int[] groupIds, Column[] arguments, AggregationMask mask) {}

  @Override
  public void addIntermediate(int[] groupIds, Column argument) {}

  @Override
  public void evaluateIntermediate(int groupId, ColumnBuilder columnBuilder) {}

  @Override
  public void evaluateFinal(int groupId, ColumnBuilder columnBuilder) {}

  @Override
  public void prepareFinal() {}

  @Override
  public void reset() {}
}
