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

import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.array.TDigestBigArray;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.RamUsageEstimator;

import static java.util.Objects.requireNonNull;

public class TDigestStateFactory {
  public static SingleTDigestState createSingleState() {
    return new SingleTDigestState();
  }

  public static GroupedTDigestState createGroupedState(TSDataType seriesDataType) {
    return new GroupedTDigestState();
  }

  public static class SingleTDigestState {
    private static final long INSTANCE_SIZE =
        RamUsageEstimator.shallowSizeOfInstance(SingleTDigestState.class);
    private TDigest tDigest;

    public TDigest getTDigest() {
      return tDigest;
    }

    public void setTDigest(TDigest value) {
      tDigest = value;
    }

    public long getEstimatedSize() {
      return INSTANCE_SIZE + tDigest.getEstimatedSize();
    }

    public void merge(TDigest other) {
      if (this.tDigest == null) {
        setTDigest(other);
      } else {
        tDigest.add(other);
      }
    }
  }

  public static class GroupedTDigestState {
    private static final long INSTANCE_SIZE =
        RamUsageEstimator.shallowSizeOfInstance(GroupedTDigestState.class);
    private TDigestBigArray tDigestBigArray = new TDigestBigArray();

    public TDigestBigArray getTDigestBigArray() {
      return tDigestBigArray;
    }

    public void setTDigestBigArray(TDigestBigArray value) {
      requireNonNull(value, "value is null");
      tDigestBigArray = value;
    }

    public long getEstimatedSize() {
      return INSTANCE_SIZE + tDigestBigArray.sizeOf();
    }

    public void merge(int groupId, TDigest other) {
      TDigest tDigest = tDigestBigArray.get(groupId);
      tDigest.add(other);
    }
  }
}
