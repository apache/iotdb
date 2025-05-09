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

package org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation;

import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.array.SpaceSavingBigArray;

import org.apache.tsfile.utils.RamUsageEstimator;

import static java.util.Objects.requireNonNull;

public class SpaceSavingStateFactory {
  public static SingleSpaceSavingState createSingleState() {
    return new SingleSpaceSavingState();
  }

  public static GroupedSpaceSavingState createGroupedState() {
    return new GroupedSpaceSavingState();
  }

  public static class SingleSpaceSavingState {
    private static final long INSTANCE_SIZE =
        RamUsageEstimator.shallowSizeOfInstance(SingleSpaceSavingState.class);
    private SpaceSaving spaceSaving;

    public SpaceSaving getSpaceSaving() {
      return spaceSaving;
    }

    public void setSpaceSaving(SpaceSaving value) {
      this.spaceSaving = value;
    }

    public long getEstimatedSize() {
      return spaceSaving == null ? 0L : spaceSaving.getEstimatedSize();
    }

    public void merge(SpaceSaving other) {
      if (this.spaceSaving == null) {
        setSpaceSaving(other);
      } else {
        spaceSaving.merge(other);
      }
    }
  }

  public static class GroupedSpaceSavingState {
    private static final long INSTANCE_SIZE =
        RamUsageEstimator.shallowSizeOfInstance(GroupedSpaceSavingState.class);
    private SpaceSavingBigArray spaceSavings;

    public SpaceSavingBigArray getSpaceSavings() {
      return spaceSavings;
    }

    public void setSpaceSavings(SpaceSavingBigArray value) {
      requireNonNull(value, "value is null");
      this.spaceSavings = value;
    }

    public long getEstimatedSize() {
      return INSTANCE_SIZE + spaceSavings.sizeOf();
    }

    public void merge(int groupId, SpaceSaving spaceSaving) {
      SpaceSaving existingSpaceSaving = spaceSavings.get(groupId, spaceSaving);
      if (!existingSpaceSaving.equals(spaceSaving)) {
        existingSpaceSaving.merge(spaceSaving);
      }
    }

    public boolean isEmpty() {
      return spaceSavings.isEmpty();
    }
  }
}
