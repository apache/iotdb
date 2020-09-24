/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.cluster.utils;

import static org.apache.iotdb.cluster.config.ClusterConstant.HASH_SALT;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.iotdb.cluster.partition.PartitionTable;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.DeletePlan;
import org.apache.iotdb.db.qp.physical.crud.InsertTabletPlan;
import org.apache.iotdb.db.qp.physical.sys.AuthorPlan;
import org.apache.iotdb.db.qp.physical.sys.DataAuthPlan;
import org.apache.iotdb.db.qp.physical.sys.DeleteStorageGroupPlan;
import org.apache.iotdb.db.qp.physical.sys.DeleteTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.FlushPlan;
import org.apache.iotdb.db.qp.physical.sys.LoadConfigurationPlan;
import org.apache.iotdb.db.qp.physical.sys.LoadConfigurationPlan.LoadConfigurationPlanType;
import org.apache.iotdb.db.qp.physical.sys.LoadDataPlan;
import org.apache.iotdb.db.qp.physical.sys.MergePlan;
import org.apache.iotdb.db.qp.physical.sys.OperateFilePlan;
import org.apache.iotdb.db.qp.physical.sys.SetStorageGroupPlan;
import org.apache.iotdb.db.qp.physical.sys.SetTTLPlan;
import org.apache.iotdb.db.qp.physical.sys.ShowTTLPlan;
import org.apache.iotdb.service.rpc.thrift.TSStatus;
import org.apache.iotdb.tsfile.read.filter.GroupByFilter;
import org.apache.iotdb.tsfile.read.filter.TimeFilter.TimeEq;
import org.apache.iotdb.tsfile.read.filter.TimeFilter.TimeGt;
import org.apache.iotdb.tsfile.read.filter.TimeFilter.TimeGtEq;
import org.apache.iotdb.tsfile.read.filter.TimeFilter.TimeIn;
import org.apache.iotdb.tsfile.read.filter.TimeFilter.TimeLt;
import org.apache.iotdb.tsfile.read.filter.TimeFilter.TimeLtEq;
import org.apache.iotdb.tsfile.read.filter.TimeFilter.TimeNotEq;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.operator.AndFilter;
import org.apache.iotdb.tsfile.read.filter.operator.NotFilter;
import org.apache.iotdb.tsfile.read.filter.operator.OrFilter;
import org.apache.iotdb.tsfile.utils.Murmur128Hash;

public class PartitionUtils {

  private PartitionUtils() {
    // util class
  }

  /**
   * Whether the plan should be directly executed without spreading it into the cluster.
   *
   * @param plan
   * @return
   */
  public static boolean isLocalNonQueryPlan(PhysicalPlan plan) {
    return plan instanceof LoadDataPlan
        || plan instanceof OperateFilePlan
        || (plan instanceof LoadConfigurationPlan
        && ((LoadConfigurationPlan) plan).getLoadConfigurationPlanType().equals(
        LoadConfigurationPlanType.LOCAL))
        ;
  }

  /**
   * GlobalMetaPlan will be executed on all meta group nodes.
   *
   * @param plan
   * @return
   */
  public static boolean isGlobalMetaPlan(PhysicalPlan plan) {
    return plan instanceof SetStorageGroupPlan
        || plan instanceof SetTTLPlan
        || plan instanceof ShowTTLPlan
        || (plan instanceof LoadConfigurationPlan && ((LoadConfigurationPlan) plan)
        .getLoadConfigurationPlanType().equals(LoadConfigurationPlanType.GLOBAL))
        || plan instanceof AuthorPlan
        || plan instanceof DeleteStorageGroupPlan
        // DataAuthPlan is global because all nodes must have all user info
        || plan instanceof DataAuthPlan
        ;
  }

  /**
   * GlobalDataPlan will be executed on all data group nodes.
   *
   * @param plan the plan to check
   * @return is globalDataPlan or not
   */
  public static boolean isGlobalDataPlan(PhysicalPlan plan) {
    return
        // because deletePlan has an infinite time range.
        plan instanceof DeletePlan
            || plan instanceof DeleteTimeSeriesPlan
            || plan instanceof MergePlan
            || plan instanceof FlushPlan;
  }

  public static int calculateStorageGroupSlotByTime(String storageGroupName, long timestamp,
      int slotNum) {
    long partitionNum = StorageEngine.getTimePartition(timestamp);
    return calculateStorageGroupSlotByPartition(storageGroupName, partitionNum, slotNum);
  }

  private static int calculateStorageGroupSlotByPartition(String storageGroupName,
      long partitionNum,
      int slotNum) {
    int hash = Murmur128Hash.hash(storageGroupName, partitionNum, HASH_SALT);
    return Math.abs(hash % slotNum);
  }


  public static InsertTabletPlan copy(InsertTabletPlan plan, long[] times, Object[] values) {
    InsertTabletPlan newPlan = new InsertTabletPlan(plan.getDeviceId(), plan.getMeasurements());
    newPlan.setDataTypes(plan.getDataTypes());
    //according to TSServiceImpl.insertBatch(), only the deviceId, measurements, dataTypes,
    //times, columns, and rowCount are need to be maintained.
    newPlan.setColumns(values);
    newPlan.setTimes(times);
    newPlan.setRowCount(times.length);
    newPlan.setMeasurementMNodes(plan.getMeasurementMNodes());
    return newPlan;
  }

  public static void reordering(InsertTabletPlan plan, TSStatus[] status, TSStatus[] subStatus) {
    List<Integer> range = plan.getRange();
    int destLoc = 0;
    for (int i = 0; i < range.size(); i += 2) {
      int start = range.get(i);
      int end = range.get(i + 1);
      System.arraycopy(subStatus, destLoc, status, start, end - start);
      destLoc += end - start;
    }
  }

  public static Intervals extractTimeInterval(Filter filter) {
    if (filter == null) {
      return Intervals.ALL_INTERVAL;
    }
    // and, or, not, value, time, group by
    // eq, neq, gt, gteq, lt, lteq, in
    if (filter instanceof AndFilter) {
      AndFilter andFilter = ((AndFilter) filter);
      Intervals leftIntervals = extractTimeInterval(andFilter.getLeft());
      Intervals rightIntervals = extractTimeInterval(andFilter.getRight());
      return leftIntervals.intersection(rightIntervals);
    } else if (filter instanceof OrFilter) {
      OrFilter orFilter = ((OrFilter) filter);
      Intervals leftIntervals = extractTimeInterval(orFilter.getLeft());
      Intervals rightIntervals = extractTimeInterval(orFilter.getRight());
      return leftIntervals.union(rightIntervals);
    } else if (filter instanceof NotFilter) {
      NotFilter notFilter = ((NotFilter) filter);
      return extractTimeInterval(notFilter.getFilter()).not();
    } else if (filter instanceof TimeGt) {
      TimeGt timeGt = ((TimeGt) filter);
      return new Intervals(((long) timeGt.getValue()) + 1, Long.MAX_VALUE);
    } else if (filter instanceof TimeGtEq) {
      TimeGtEq timeGtEq = ((TimeGtEq) filter);
      return new Intervals(((long) timeGtEq.getValue()), Long.MAX_VALUE);
    } else if (filter instanceof TimeEq) {
      TimeEq timeEq = ((TimeEq) filter);
      return new Intervals(((long) timeEq.getValue()), ((long) timeEq.getValue()));
    } else if (filter instanceof TimeNotEq) {
      TimeNotEq timeNotEq = ((TimeNotEq) filter);
      Intervals intervals = new Intervals();
      intervals.addInterval(Long.MIN_VALUE, (long) timeNotEq.getValue() - 1);
      intervals.addInterval((long) timeNotEq.getValue() + 1, Long.MAX_VALUE);
      return intervals;
    } else if (filter instanceof TimeLt) {
      TimeLt timeLt = ((TimeLt) filter);
      return new Intervals(Long.MIN_VALUE, (long) timeLt.getValue() - 1);
    } else if (filter instanceof TimeLtEq) {
      TimeLtEq timeLtEq = ((TimeLtEq) filter);
      return new Intervals(Long.MIN_VALUE, (long) timeLtEq.getValue());
    } else if (filter instanceof TimeIn) {
      TimeIn timeIn = ((TimeIn) filter);
      Intervals intervals = new Intervals();
      for (Object value : timeIn.getValues()) {
        long time = ((long) value);
        intervals.addInterval(time, time);
      }
      return intervals;
    } else if (filter instanceof GroupByFilter) {
      GroupByFilter groupByFilter = ((GroupByFilter) filter);
      return new Intervals(groupByFilter.getStartTime(), groupByFilter.getEndTime() + 1);
    }
    // value filter
    return Intervals.ALL_INTERVAL;
  }

  /**
   * All intervals are closed.
   */
  public static class Intervals extends ArrayList<Long> {

    static final Intervals ALL_INTERVAL = new Intervals(Long.MIN_VALUE,
        Long.MAX_VALUE);

    public Intervals() {
      super();
    }

    Intervals(long lowerBound, long upperBound) {
      super();
      addInterval(lowerBound, upperBound);
    }

    public int getIntervalSize() {
      return size() / 2;
    }

    public long getLowerBound(int index) {
      return get(index * 2);
    }

    public long getUpperBound(int index) {
      return get(index * 2 + 1);
    }

    void setLowerBound(int index, long lb) {
      set(index * 2, lb);
    }

    void setUpperBound(int index, long ub) {
      set(index * 2 + 1, ub);
    }

    public void addInterval(long lowerBound, long upperBound) {
      add(lowerBound);
      add(upperBound);
    }

    Intervals intersection(Intervals that) {
      Intervals result = new Intervals();
      int thisSize = this.getIntervalSize();
      int thatSize = that.getIntervalSize();
      for (int i = 0; i < thisSize; i++) {
        for (int j = 0; j < thatSize; j++) {
          long thisLB = this.getLowerBound(i);
          long thisUB = this.getUpperBound(i);
          long thatLB = that.getLowerBound(i);
          long thatUB = that.getUpperBound(i);
          if (thisUB >= thatLB) {
            if (thisUB <= thatUB) {
              result.addInterval(Math.max(thisLB, thatLB), thisUB);
            } else if (thisLB <= thatUB) {
              result.addInterval(Math.max(thisLB, thatLB), thatUB);
            }
          }
        }
      }
      return result;
    }

    /**
     * The union is implemented by merge, so the two intervals must be ordered.
     *
     * @param that
     * @return
     */
    Intervals union(Intervals that) {
      if (this.isEmpty()) {
        return that;
      } else if (that.isEmpty()) {
        return this;
      }
      Intervals result = new Intervals();

      int thisSize = this.getIntervalSize();
      int thatSize = that.getIntervalSize();
      int thisIndex = 0;
      int thatIndex = 0;
      // merge the heads of the two intervals
      while (thisIndex < thisSize && thatIndex < thatSize) {
        long thisLB = this.getLowerBound(thisIndex);
        long thisUB = this.getUpperBound(thisIndex);
        long thatLB = that.getLowerBound(thatIndex);
        long thatUB = that.getUpperBound(thatIndex);
        if (thisLB <= thatLB) {
          result.mergeLast(thisLB, thisUB);
          thisIndex++;
        } else {
          result.mergeLast(thatLB, thatUB);
          thatIndex++;
        }
      }
      // merge the remaining intervals
      Intervals remainingIntervals = thisIndex < thisSize ? this : that;
      int remainingIndex = thisIndex < thisSize ? thisIndex : thatIndex;
      mergeRemainingIntervals(remainingIndex, remainingIntervals, result);

      return result;
    }

    private void mergeRemainingIntervals(int remainingIndex, Intervals remainingIntervals,
        Intervals result) {
      for (int i = remainingIndex; i < remainingIntervals.getIntervalSize(); i++) {
        long lb = remainingIntervals.getLowerBound(i);
        long ub = remainingIntervals.getUpperBound(i);
        result.mergeLast(lb, ub);
      }
    }

    /**
     * Merge an interval of [lowerBound, upperBound] with the last interval if they can be merged,
     * or just add it as the last interval if its lowerBound is larger than the upperBound of the
     * last interval. If the upperBound of the new interval is less than the lowerBound of the last
     * interval, nothing will be done.
     *
     * @param lowerBound
     * @param upperBound
     */
    private void mergeLast(long lowerBound, long upperBound) {
      if (getIntervalSize() == 0) {
        addInterval(lowerBound, upperBound);
        return;
      }
      int lastIndex = getIntervalSize() - 1;
      long lastLB = getLowerBound(lastIndex);
      long lastUB = getUpperBound(lastIndex);
      if (lowerBound > lastUB + 1) {
        // e.g., last [3, 5], new [7, 10], just add the new interval
        addInterval(lowerBound, upperBound);
        return;
      }
      if (upperBound < lastLB - 1) {
        // e.g., last [7, 10], new [3, 5], do nothing
        return;
      }
      // merge the new interval into the last one
      setLowerBound(lastIndex, Math.min(lastLB, lowerBound));
      setUpperBound(lastIndex, Math.max(lastUB, upperBound));
    }

    public Intervals not() {
      if (isEmpty()) {
        return ALL_INTERVAL;
      }
      Intervals result = new Intervals();
      long firstLB = getLowerBound(0);
      if (firstLB != Long.MIN_VALUE) {
        result.addInterval(Long.MIN_VALUE, firstLB - 1);
      }

      int intervalSize = getIntervalSize();
      for (int i = 0; i < intervalSize - 1; i++) {
        long currentUB = getUpperBound(i);
        long nextLB = getLowerBound(i + 1);
        if (currentUB + 1 <= nextLB - 1) {
          result.addInterval(currentUB + 1, nextLB - 1);
        }
      }

      long lastUB = getUpperBound(result.getIntervalSize() - 1);
      if (lastUB != Long.MAX_VALUE) {
        result.addInterval(lastUB + 1, Long.MAX_VALUE);
      }
      return result;
    }
  }

  /**
   * Calculate the headers of the groups that possibly store the data of a timeseries over the given
   * time range.
   *
   * @param storageGroupName
   * @param timeLowerBound
   * @param timeUpperBound
   * @param partitionTable
   * @param result
   */
  public static void getIntervalHeaders(String storageGroupName, long timeLowerBound,
      long timeUpperBound,
      PartitionTable partitionTable, Set<Node> result) {
    long partitionInterval = StorageEngine.getTimePartitionInterval();
    long currPartitionStart = timeLowerBound / partitionInterval * partitionInterval;
    while (currPartitionStart <= timeUpperBound) {
      result.add(partitionTable.routeToHeaderByTime(storageGroupName, currPartitionStart));
      currPartitionStart += partitionInterval;
    }
  }

}
