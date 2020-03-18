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
import java.util.Set;
import org.apache.iotdb.cluster.partition.PartitionTable;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.BatchInsertPlan;
import org.apache.iotdb.db.qp.physical.crud.DeletePlan;
import org.apache.iotdb.db.qp.physical.crud.QueryPlan;
import org.apache.iotdb.db.qp.physical.sys.AuthorPlan;
import org.apache.iotdb.db.qp.physical.sys.DeleteStorageGroupPlan;
import org.apache.iotdb.db.qp.physical.sys.DeleteTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.LoadConfigurationPlan;
import org.apache.iotdb.db.qp.physical.sys.LoadDataPlan;
import org.apache.iotdb.db.qp.physical.sys.OperateFilePlan;
import org.apache.iotdb.db.qp.physical.sys.SetStorageGroupPlan;
import org.apache.iotdb.db.qp.physical.sys.SetTTLPlan;
import org.apache.iotdb.db.qp.physical.sys.ShowPlan;
import org.apache.iotdb.db.qp.physical.sys.ShowPlan.ShowContentType;
import org.apache.iotdb.db.qp.physical.sys.ShowTTLPlan;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PartitionUtils {

  private static final Logger logger = LoggerFactory.getLogger(PartitionUtils.class);

  private PartitionUtils() {
    // util class
  }

  /**
   * Localplan only be executed locally.
   * @param plan
   * @return
   */
  public static boolean isLocalPlan(PhysicalPlan plan) {
    //QueryPlan is hard to be splited, so we do it locally and use a remote series reader to get data.
    return plan instanceof QueryPlan
        || plan instanceof LoadDataPlan
        || plan instanceof OperateFilePlan
        || (plan instanceof ShowPlan
              && ((ShowPlan) plan).getShowContentType().equals(ShowContentType.DYNAMIC_PARAMETER))
        || (plan instanceof ShowPlan
        && ((ShowPlan) plan).getShowContentType().equals(ShowContentType.FLUSH_TASK_INFO))
        || (plan instanceof ShowPlan
        && ((ShowPlan) plan).getShowContentType().equals(ShowContentType.VERSION))
        || (plan instanceof ShowPlan
        && ((ShowPlan) plan).getShowContentType().equals(ShowContentType.TTL))
        ;
  }

  /**
   * GlobalPlan will be executed on all nodes.
   * @param plan
   * @return
   */
  public static boolean isGlobalPlan(PhysicalPlan plan) {
    // TODO-Cluster#348: support more plans
    return plan instanceof SetStorageGroupPlan
          || plan instanceof DeletePlan // because deletePlan has an infinite time range.
          || plan instanceof SetTTLPlan
          || plan instanceof ShowTTLPlan
          || plan instanceof LoadConfigurationPlan
          || plan instanceof DeleteTimeSeriesPlan
          //delete timeseries plan is global because all nodes may have its data
          || plan instanceof AuthorPlan
          || plan instanceof DeleteStorageGroupPlan
    ;
  }

  public static int calculateStorageGroupSlotByTime(String storageGroupName, long timestamp,
      int slotNum) {
    long partitionInstance = StorageEngine.fromTimeToTimePartition(timestamp);
    int hash = Murmur128Hash.hash(storageGroupName, partitionInstance, HASH_SALT);
    return Math.abs(hash % slotNum);
  }

  public static int calculateStorageGroupSlotByPartition(String storageGroupName, long partitionNum,
      int slotNum) {
    int hash = Murmur128Hash.hash(storageGroupName, partitionNum, HASH_SALT);
    return Math.abs(hash % slotNum);
  }


  public static BatchInsertPlan copy(BatchInsertPlan plan, long[] times, Object[] values) {
    BatchInsertPlan newPlan = new BatchInsertPlan(plan.getDeviceId(), plan.getMeasurements());
    newPlan.setDataTypes(plan.getDataTypes());
    //according to TSServiceImpl.insertBatch(), only the deviceId, measreuments, dataTypes,
    //times, columns, and rowCount are need to be maintained.
    newPlan.setColumns(values);
    newPlan.setTimes(times);
    newPlan.setRowCount(times.length);
    return newPlan;
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

  public static class Intervals extends ArrayList<Long> {

    public static final Intervals ALL_INTERVAL = new Intervals(Long.MIN_VALUE,
        Long.MAX_VALUE);

    public Intervals() {
      super();
    }

    public Intervals(long lowerBound, long upperBound) {
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

    public void addInterval(long lowerBound, long upperBound) {
      add(lowerBound);
      add(upperBound);
    }

    public Intervals intersection(Intervals that) {
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
     * @param that
     * @return
     */
    public Intervals union(Intervals that) {
      Intervals result = new Intervals();
      if (this.isEmpty()) {
        return that;
      } else if (that.isEmpty()) {
        return this;
      }

      int thisSize = this.getIntervalSize();
      int thatSize = that.getIntervalSize();
      int thisIndex = 0;
      int thatIndex = 0;
      long lastLowerBound = 0;
      long lastUpperBound = 0;
      boolean lastBoundSet = false;
      // merge the heads of the two intervals
      while (thisIndex < thisSize && thatIndex < thatSize) {
        long thisLB = this.getLowerBound(thisIndex);
        long thisUB = this.getUpperBound(thisIndex);
        long thatLB = that.getLowerBound(thatIndex);
        long thatUB = that.getUpperBound(thatIndex);
        if (!lastBoundSet) {
          lastBoundSet = true;
          if (thisLB <= thatLB) {
            lastLowerBound = thisLB;
            lastUpperBound = thisUB;
            thisIndex ++;
          } else {
            lastLowerBound = thatLB;
            lastUpperBound = thatUB;
            thatIndex ++;
          }
        } else {
          if (thisLB <= lastUpperBound + 1 && thisUB >= lastLowerBound - 1) {
            // the next interval from this can merge with last interval
            lastLowerBound = Math.min(thisLB, lastLowerBound);
            lastUpperBound = Math.max(thisUB, lastUpperBound);
            thisIndex ++;
          } else if (thatLB <= lastUpperBound + 1 && thatUB >= lastLowerBound - 1) {
            // the next interval from that can merge with last interval
            lastLowerBound = Math.min(thatLB, lastLowerBound);
            lastUpperBound = Math.max(thatUB, lastUpperBound);
            thatIndex ++;
          } else {
            // neither intervals can merge, add the last interval to the result and select a new
            // one as base
            result.addInterval(lastLowerBound, lastUpperBound);
            lastBoundSet = false;
          }
        }
      }
      // merge the remaining intervals
      Intervals remainingIntervals = thisIndex < thisSize ? this : that;
      int remainingIndex = thisIndex < thisSize ? thisIndex : thatIndex;
      for (int i = remainingIndex; i < remainingIntervals.getIntervalSize(); i++) {
        long lb = remainingIntervals.getLowerBound(i);
        long ub = remainingIntervals.getUpperBound(i);
        if (lb <= lastUpperBound && ub >= lastLowerBound) {
          // the next interval can merge with last interval
          lastLowerBound = Math.min(lb, lastLowerBound);
          lastUpperBound = Math.max(ub, lastUpperBound);
        } else {
          // the two interval does not intersect, add the previous interval to the result
          result.addInterval(lastLowerBound, lastUpperBound);
          lastLowerBound = lb;
          lastUpperBound = ub;
        }
      }
      // add the last interval
      result.addInterval(lastLowerBound, lastLowerBound);
      return result;
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
        if (currentUB + 1 <= nextLB -1) {
          result.addInterval(currentUB + 1, nextLB -1);
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
   * Calculate the headers of the groups that possibly store the data of a timeseries over the
   * given time range.
   * @param storageGroupName
   * @param timeLowerBound
   * @param timeUpperBound
   * @param partitionTable
   * @param result
   */
  public static void getIntervalHeaders(String storageGroupName, long timeLowerBound, long timeUpperBound,
      PartitionTable partitionTable, Set<Node> result) {
    long partitionInterval = StorageEngine.getTimePartitionInterval();
    long currPartitionStart = timeLowerBound / partitionInterval * partitionInterval;
    while (currPartitionStart <= timeUpperBound) {
      result.add(partitionTable.routeToHeaderByTime(storageGroupName, currPartitionStart));
      currPartitionStart += partitionInterval;
    }
  }

}
