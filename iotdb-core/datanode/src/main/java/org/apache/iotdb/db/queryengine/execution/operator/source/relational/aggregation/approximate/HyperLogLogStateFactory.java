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

import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.array.HyperLogLogBigArray;

import org.apache.tsfile.utils.RamUsageEstimator;

import static java.util.Objects.requireNonNull;

public class HyperLogLogStateFactory {
  public static SingleHyperLogLogState createSingleState() {
    return new SingleHyperLogLogState();
  }

  public static GroupedHyperLogLogState createGroupedState() {
    return new GroupedHyperLogLogState();
  }

  public static class SingleHyperLogLogState {
    private static final long INSTANCE_SIZE =
        RamUsageEstimator.shallowSizeOfInstance(SingleHyperLogLogState.class);
    private HyperLogLog hll;

    public HyperLogLog getHyperLogLog() {
      return hll;
    }

    public void setHyperLogLog(HyperLogLog value) {
      hll = value;
    }

    public long getEstimatedSize() {
      // not used
      return INSTANCE_SIZE + hll.getEstimatedSize();
    }

    public void merge(HyperLogLog other) {
      if (this.hll == null) {
        setHyperLogLog(other);
      } else {
        hll.merge(other);
      }
    }
  }

  public static class GroupedHyperLogLogState {
    private static final long INSTANCE_SIZE =
        RamUsageEstimator.shallowSizeOfInstance(GroupedHyperLogLogState.class);
    private HyperLogLogBigArray hlls = new HyperLogLogBigArray();

    public HyperLogLogBigArray getHyperLogLogs() {
      return hlls;
    }

    public void setHyperLogLogs(HyperLogLogBigArray value) {
      requireNonNull(value, "value is null");
      this.hlls = value;
    }

    public long getEstimatedSize() {
      return INSTANCE_SIZE + hlls.sizeOf();
    }

    public void merge(int groupId, HyperLogLog hll) {
      HyperLogLog existingHll = hlls.get(groupId, hll);
      if (!existingHll.equals(hll)) {
        existingHll.merge(hll);
      }
    }

    public boolean isEmpty() {
      return hlls.isEmpty();
    }
  }
}
