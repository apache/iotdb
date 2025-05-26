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

package org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.approximate;

import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.array.SpaceSavingBigArray;

import org.apache.tsfile.utils.RamUsageEstimator;

import static java.util.Objects.requireNonNull;
import static org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.approximate.SpaceSaving.getDefaultEstimatedSize;

public class SpaceSavingStateFactory {
  public static <T> SingleSpaceSavingState<T> createSingleState() {
    return new SingleSpaceSavingState<T>();
  }

  public static <T> GroupedSpaceSavingState<T> createGroupedState() {
    return new GroupedSpaceSavingState<T>();
  }

  public static class SingleSpaceSavingState<T> {
    private static final long INSTANCE_SIZE =
        RamUsageEstimator.shallowSizeOfInstance(SingleSpaceSavingState.class);
    private SpaceSaving<T> spaceSaving;

    public SpaceSaving<T> getSpaceSaving() {
      return spaceSaving;
    }

    public void setSpaceSaving(SpaceSaving<T> value) {
      this.spaceSaving = value;
    }

    public long getEstimatedSize() {
      return spaceSaving == null
          ? INSTANCE_SIZE + getDefaultEstimatedSize()
          : INSTANCE_SIZE + spaceSaving.getEstimatedSize();
    }

    public void merge(SpaceSaving<T> other) {
      if (this.spaceSaving == null) {
        setSpaceSaving(other);
      } else {
        spaceSaving.merge(other);
      }
    }
  }

  public static class GroupedSpaceSavingState<T> {
    private static final long INSTANCE_SIZE =
        RamUsageEstimator.shallowSizeOfInstance(GroupedSpaceSavingState.class);
    private SpaceSavingBigArray<T> spaceSavings = new SpaceSavingBigArray<>();

    public SpaceSavingBigArray<T> getSpaceSavings() {
      return spaceSavings;
    }

    public void setSpaceSavings(SpaceSavingBigArray<T> value) {
      requireNonNull(value, "value is null");
      this.spaceSavings = value;
    }

    public long getEstimatedSize() {
      return INSTANCE_SIZE + spaceSavings.sizeOf();
    }

    public void merge(int groupId, SpaceSaving<T> spaceSaving) {
      SpaceSaving<T> existingSpaceSaving = spaceSavings.get(groupId, spaceSaving);
      if (!existingSpaceSaving.equals(spaceSaving)) {
        existingSpaceSaving.merge(spaceSaving);
      }
    }

    public boolean isEmpty() {
      return spaceSavings.isEmpty();
    }
  }
}
