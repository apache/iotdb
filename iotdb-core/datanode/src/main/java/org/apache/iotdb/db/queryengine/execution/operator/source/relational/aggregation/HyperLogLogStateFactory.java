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

import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.array.ObjectBigArray;

import org.apache.tsfile.utils.RamUsageEstimator;

import static java.util.Objects.requireNonNull;
import static org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.HyperLogLog.DEFAULT_STANDARD_ERROR;

public class HyperLogLogStateFactory {
  public static SingleHyperLogLogState createSingleState() {
    return new SingleHyperLogLogState();
  }

  public static GroupedHyperLogLogState createGroupedState() {
    return new GroupedHyperLogLogState();
  }

  public static HyperLogLog getOrCreateHyperLogLog(SingleHyperLogLogState state) {
    return getOrCreateHyperLogLog(state, DEFAULT_STANDARD_ERROR);
  }

  public static HyperLogLog getOrCreateHyperLogLog(
      SingleHyperLogLogState state, double maxStandardError) {
    HyperLogLog hll = state.getHyperLogLog();
    if (hll == null) {
      hll = new HyperLogLog(maxStandardError);
      state.setHyperLogLog(hll);
    }
    return hll;
  }

  public static ObjectBigArray<HyperLogLog> getOrCreateHyperLogLog(GroupedHyperLogLogState state) {
    return getOrCreateHyperLogLog(state, DEFAULT_STANDARD_ERROR);
  }

  public static ObjectBigArray<HyperLogLog> getOrCreateHyperLogLog(
      GroupedHyperLogLogState state, double maxStandardError) {
    ObjectBigArray<HyperLogLog> hlls = state.getHyperLogLogs();
    if (hlls == null) {
      hlls = new ObjectBigArray<>(new HyperLogLog(maxStandardError));
      state.setHyperLogLogs(hlls);
    }
    return hlls;
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
  }

  public static class GroupedHyperLogLogState {
    private static final long INSTANCE_SIZE =
        RamUsageEstimator.shallowSizeOfInstance(GroupedHyperLogLogState.class);
    private ObjectBigArray<HyperLogLog> hlls = new ObjectBigArray<>();

    public ObjectBigArray<HyperLogLog> getHyperLogLogs() {
      return hlls;
    }

    public void setHyperLogLogs(ObjectBigArray<HyperLogLog> value) {
      requireNonNull(value, "value is null");
      this.hlls = value;
    }
  }
}
