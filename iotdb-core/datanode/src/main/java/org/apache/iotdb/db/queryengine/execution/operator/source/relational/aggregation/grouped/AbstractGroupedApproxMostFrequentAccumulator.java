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

import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.approximate.SpaceSavingStateFactory;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.array.SpaceSavingBigArray;

import com.google.gson.Gson;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.utils.Binary;

import java.nio.charset.StandardCharsets;

public abstract class AbstractGroupedApproxMostFrequentAccumulator<T>
    implements GroupedAccumulator {
  protected final SpaceSavingStateFactory.GroupedSpaceSavingState<T> state =
      SpaceSavingStateFactory.createGroupedState();

  public AbstractGroupedApproxMostFrequentAccumulator() {}

  @Override
  public void setGroupCount(long groupCount) {
    state.getSpaceSavings().ensureCapacity(groupCount);
  }

  @Override
  public void evaluateIntermediate(int groupId, ColumnBuilder columnBuilder) {
    columnBuilder.writeBinary(new Binary(state.getSpaceSavings().get(groupId).serialize()));
  }

  @Override
  public void evaluateFinal(int groupId, ColumnBuilder columnBuilder) {
    Binary result =
        new Binary(
            new Gson().toJson(state.getSpaceSavings().get(groupId).getBuckets()),
            StandardCharsets.UTF_8);
    columnBuilder.writeBinary(result);
  }

  @Override
  public void prepareFinal() {}

  @Override
  public void reset() {
    state.getSpaceSavings().reset();
  }

  public SpaceSavingBigArray<T> getOrCreateSpaceSaving(
      SpaceSavingStateFactory.GroupedSpaceSavingState<T> state) {
    if (state.isEmpty()) {
      state.setSpaceSavings(new SpaceSavingBigArray<>());
    }
    return state.getSpaceSavings();
  }
}
