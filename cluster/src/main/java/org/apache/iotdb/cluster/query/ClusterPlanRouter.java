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

package org.apache.iotdb.cluster.query;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.iotdb.cluster.exception.UnsupportedPlanException;
import org.apache.iotdb.cluster.partition.PartitionGroup;
import org.apache.iotdb.cluster.partition.PartitionTable;
import org.apache.iotdb.cluster.utils.PartitionUtils;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.metadata.StorageGroupNotSetException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertRowPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertTabletPlan;
import org.apache.iotdb.db.qp.physical.crud.UpdatePlan;
import org.apache.iotdb.db.qp.physical.sys.AlterTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.CountPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.ShowChildPathsPlan;
import org.apache.iotdb.db.qp.physical.sys.ShowPlan.ShowContentType;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.utils.TestOnly;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.utils.Binary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClusterPlanRouter {

  private static final Logger logger = LoggerFactory.getLogger(ClusterPlanRouter.class);

  private PartitionTable partitionTable;

  public ClusterPlanRouter(PartitionTable partitionTable) {
    this.partitionTable = partitionTable;
  }

  private MManager getMManager() {
    return IoTDB.metaManager;
  }

  @TestOnly
  public PartitionGroup routePlan(PhysicalPlan plan)
      throws UnsupportedPlanException, MetadataException {
    if (plan instanceof InsertRowPlan) {
      return routePlan((InsertRowPlan) plan);
    } else if (plan instanceof CreateTimeSeriesPlan) {
      return routePlan((CreateTimeSeriesPlan) plan);
    } else if (plan instanceof ShowChildPathsPlan) {
      return routePlan((ShowChildPathsPlan) plan);
    }
    //the if clause can be removed after the program is stable
    if (PartitionUtils.isLocalNonQueryPlan(plan)) {
      logger.error("{} is a local plan. Please run it locally directly", plan);
    } else if (PartitionUtils.isGlobalMetaPlan(plan) || PartitionUtils.isGlobalDataPlan(plan)) {
      logger.error("{} is a global plan. Please forward it to all partitionGroups", plan);
    }
    if (plan.canBeSplit()) {
      logger.error("{} can be split. Please call splitPlanAndMapToGroups", plan);
    }
    throw new UnsupportedPlanException(plan);
  }

  private PartitionGroup routePlan(InsertRowPlan plan)
      throws MetadataException {
    return partitionTable.partitionByPathTime(plan.getDeviceId(), plan.getTime());
  }

  private PartitionGroup routePlan(CreateTimeSeriesPlan plan)
      throws MetadataException {
    return partitionTable.partitionByPathTime(plan.getPath().getFullPath(), 0);
  }

  private PartitionGroup routePlan(ShowChildPathsPlan plan) {
    try {
      return partitionTable.route(getMManager().getStorageGroupName(plan.getPath().getFullPath())
          , 0);
    } catch (MetadataException e) {
      //the path is too short to have no a storage group name, e.g., "root"
      //so we can do it locally.
      return partitionTable.getLocalGroups().get(0);
    }
  }

  public Map<PhysicalPlan, PartitionGroup> splitAndRoutePlan(PhysicalPlan plan)
      throws UnsupportedPlanException, MetadataException {
    if (plan instanceof InsertTabletPlan) {
      return splitAndRoutePlan((InsertTabletPlan) plan);
    } else if (plan instanceof UpdatePlan) {
      return splitAndRoutePlan((UpdatePlan) plan);
    } else if (plan instanceof CountPlan) {
      return splitAndRoutePlan((CountPlan) plan);
    } else if (plan instanceof CreateTimeSeriesPlan) {
      return splitAndRoutePlan((CreateTimeSeriesPlan) plan);
    } else if (plan instanceof InsertRowPlan) {
      return splitAndRoutePlan((InsertRowPlan) plan);
    } else if (plan instanceof AlterTimeSeriesPlan) {
      return splitAndRoutePlan((AlterTimeSeriesPlan) plan);
    }
    //the if clause can be removed after the program is stable
    if (PartitionUtils.isLocalNonQueryPlan(plan)) {
      logger.error("{} is a local plan. Please run it locally directly", plan);
    } else if (PartitionUtils.isGlobalMetaPlan(plan) || PartitionUtils.isGlobalDataPlan(plan)) {
      logger.error("{} is a global plan. Please forward it to all partitionGroups", plan);
    }
    if (!plan.canBeSplit()) {
      logger.error("{} cannot be split. Please call routePlan", plan);
    }
    throw new UnsupportedPlanException(plan);
  }

  private Map<PhysicalPlan, PartitionGroup> splitAndRoutePlan(InsertRowPlan plan)
      throws MetadataException {
    PartitionGroup partitionGroup = partitionTable.partitionByPathTime(plan.getDeviceId(),
        plan.getTime());
    return Collections.singletonMap(plan, partitionGroup);
  }

  private Map<PhysicalPlan, PartitionGroup> splitAndRoutePlan(AlterTimeSeriesPlan plan)
      throws MetadataException {
    PartitionGroup partitionGroup =
        partitionTable.partitionByPathTime(plan.getPath().getFullPath(), 0);
    return Collections.singletonMap(plan, partitionGroup);
  }

  private Map<PhysicalPlan, PartitionGroup> splitAndRoutePlan(CreateTimeSeriesPlan plan)
      throws MetadataException {
    PartitionGroup partitionGroup =
        partitionTable.partitionByPathTime(plan.getPath().getFullPath(), 0);
    return Collections.singletonMap(plan, partitionGroup);
  }

  @SuppressWarnings("SuspiciousSystemArraycopy")
  private Map<PhysicalPlan, PartitionGroup> splitAndRoutePlan(InsertTabletPlan plan)
      throws MetadataException {
    String storageGroup = getMManager().getStorageGroupName(plan.getDeviceId());
    Map<PhysicalPlan, PartitionGroup> result = new HashMap<>();
    long[] times = plan.getTimes();
    if (times.length == 0) {
      return Collections.emptyMap();
    }
    long startTime = (times[0] / StorageEngine.getTimePartitionInterval()) * StorageEngine
        .getTimePartitionInterval();//included
    long endTime = startTime + StorageEngine.getTimePartitionInterval();//excluded
    int startLoc = 0; //included

    Map<PartitionGroup, List<Integer>> splitMap = new HashMap<>();
    //for each List in split, they are range1.start, range1.end, range2.start, range2.end, ...
    for (int i = 1; i < times.length; i++) {// times are sorted in session API.
      if (times[i] >= endTime) {
        // a new range.
        PartitionGroup group = partitionTable.route(storageGroup, startTime);
        List<Integer> ranges = splitMap.computeIfAbsent(group, x -> new ArrayList<>());
        ranges.add(startLoc);//include
        ranges.add(i);//excluded
        //next init
        startLoc = i;
        startTime = endTime;
        endTime =
            (times[i] / StorageEngine.getTimePartitionInterval() + 1) * StorageEngine
                .getTimePartitionInterval();
      }
    }
    //the final range
    PartitionGroup group = partitionTable.route(storageGroup, startTime);
    List<Integer> ranges = splitMap.computeIfAbsent(group, x -> new ArrayList<>());
    ranges.add(startLoc);//includec
    ranges.add(times.length);//excluded

    List<Integer> locs;
    for (Map.Entry<PartitionGroup, List<Integer>> entry : splitMap.entrySet()) {
      //generate a new times and values
      locs = entry.getValue();
      int count = 0;
      for (int i = 0; i < locs.size(); i += 2) {
        int start = locs.get(i);
        int end = locs.get(i + 1);
        count += end - start;
      }
      long[] subTimes = new long[count];
      int destLoc = 0;
      Object[] values = initTabletValues(plan.getMeasurements().length, count, plan.getDataTypes());
      for (int i = 0; i < locs.size(); i += 2) {
        int start = locs.get(i);
        int end = locs.get(i + 1);
        System.arraycopy(plan.getTimes(), start, subTimes, destLoc, end - start);
        for (int k = 0; k < values.length; k++) {
          System.arraycopy(plan.getColumns()[k], start, values[k], destLoc, end - start);
        }
        destLoc += end - start;
      }
      InsertTabletPlan newBatch = PartitionUtils.copy(plan, subTimes, values);
      newBatch.setRange(locs);
      result.put(newBatch, entry.getKey());
    }
    return result;
  }

  private Object[] initTabletValues(int columnSize, int rowSize, TSDataType[] dataTypes) {
    Object[] values = new Object[columnSize];
    for (int i = 0; i < values.length; i++) {
      switch (dataTypes[i]) {
        case TEXT:
          values[i] = new Binary[rowSize];
          break;
        case FLOAT:
          values[i] = new float[rowSize];
          break;
        case INT32:
          values[i] = new int[rowSize];
          break;
        case INT64:
          values[i] = new long[rowSize];
          break;
        case DOUBLE:
          values[i] = new double[rowSize];
          break;
        case BOOLEAN:
          values[i] = new boolean[rowSize];
          break;
      }
    }
    return values;
  }

  private Map<PhysicalPlan, PartitionGroup> splitAndRoutePlan(UpdatePlan plan)
      throws UnsupportedPlanException {
    logger.error("UpdatePlan is not implemented");
    throw new UnsupportedPlanException(plan);
  }

  private Map<PhysicalPlan, PartitionGroup> splitAndRoutePlan(CountPlan plan)
      throws StorageGroupNotSetException, IllegalPathException {
    //CountPlan is quite special because it has the behavior of wildcard at the tail of the path
    // even though there is no wildcard
    Map<String, String> sgPathMap = getMManager()
        .determineStorageGroup(plan.getPath().getFullPath() + ".*");
    if (sgPathMap.isEmpty()) {
      throw new StorageGroupNotSetException(plan.getPath().getFullPath());
    }
    Map<PhysicalPlan, PartitionGroup> result = new HashMap<>();
    if (plan.getShowContentType().equals(ShowContentType.COUNT_TIMESERIES)) {
      //support wildcard
      for (Map.Entry<String, String> entry : sgPathMap.entrySet()) {
        CountPlan plan1 = new CountPlan(ShowContentType.COUNT_TIMESERIES,
            new Path(entry.getValue()), plan.getLevel());
        result.put(plan1, partitionTable.route(entry.getKey(), 0));
      }
    } else {
      //do not support wildcard
      if (sgPathMap.size() == 1) {
        // the path of the original plan has only one SG, or there is only one SG in the system.
        for (Map.Entry<String, String> entry : sgPathMap.entrySet()) {
          //actually, there is only one entry
          result.put(plan, partitionTable.route(entry.getKey(), 0));
        }
      } else {
        // the path of the original plan contains more than one SG, and we added a wildcard at the tail.
        // we have to remove it.
        for (Map.Entry<String, String> entry : sgPathMap.entrySet()) {
          CountPlan plan1 = new CountPlan(ShowContentType.COUNT_TIMESERIES,
              new Path(entry.getValue().substring(0, entry.getValue().lastIndexOf(".*"))),
              plan.getLevel());
          result.put(plan1, partitionTable.route(entry.getKey(), 0));
        }
      }
    }
    return result;
  }
}
