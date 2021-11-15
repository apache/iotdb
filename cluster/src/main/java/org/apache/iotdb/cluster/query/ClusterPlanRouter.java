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

import org.apache.iotdb.cluster.exception.UnknownLogTypeException;
import org.apache.iotdb.cluster.exception.UnsupportedPlanException;
import org.apache.iotdb.cluster.log.Log;
import org.apache.iotdb.cluster.log.logtypes.AddNodeLog;
import org.apache.iotdb.cluster.log.logtypes.RemoveNodeLog;
import org.apache.iotdb.cluster.partition.PartitionGroup;
import org.apache.iotdb.cluster.partition.PartitionTable;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.utils.PartitionUtils;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertMultiTabletPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertRowPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertRowsOfOneDevicePlan;
import org.apache.iotdb.db.qp.physical.crud.InsertRowsPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertTabletPlan;
import org.apache.iotdb.db.qp.physical.sys.AlterTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateAlignedTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateMultiTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.LogPlan;
import org.apache.iotdb.db.qp.physical.sys.ShowChildPathsPlan;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.utils.TestOnly;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.BitMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

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
    // the if clause can be removed after the program is stable
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

  private PartitionGroup routePlan(InsertRowPlan plan) throws MetadataException {
    return partitionTable.partitionByPathTime(plan.getPrefixPath(), plan.getTime());
  }

  private PartitionGroup routePlan(CreateTimeSeriesPlan plan) throws MetadataException {
    return partitionTable.partitionByPathTime(plan.getPath(), 0);
  }

  private PartitionGroup routePlan(ShowChildPathsPlan plan) {
    try {
      return partitionTable.route(
          getMManager().getBelongedStorageGroup(plan.getPath()).getFullPath(), 0);
    } catch (MetadataException e) {
      // the path is too short to have no a storage group name, e.g., "root"
      // so we can do it locally.
      return partitionTable.getLocalGroups().get(0);
    }
  }

  public Map<PhysicalPlan, PartitionGroup> splitAndRoutePlan(PhysicalPlan plan)
      throws UnsupportedPlanException, MetadataException, UnknownLogTypeException {
    if (plan instanceof InsertRowsPlan) {
      return splitAndRoutePlan((InsertRowsPlan) plan);
    } else if (plan instanceof InsertTabletPlan) {
      return splitAndRoutePlan((InsertTabletPlan) plan);
    } else if (plan instanceof InsertMultiTabletPlan) {
      return splitAndRoutePlan((InsertMultiTabletPlan) plan);
    } else if (plan instanceof CreateTimeSeriesPlan) {
      return splitAndRoutePlan((CreateTimeSeriesPlan) plan);
    } else if (plan instanceof CreateAlignedTimeSeriesPlan) {
      return splitAndRoutePlan((CreateAlignedTimeSeriesPlan) plan);
    } else if (plan instanceof InsertRowPlan) {
      return splitAndRoutePlan((InsertRowPlan) plan);
    } else if (plan instanceof InsertRowsOfOneDevicePlan) {
      return splitAndRoutePlan((InsertRowsOfOneDevicePlan) plan);
    } else if (plan instanceof AlterTimeSeriesPlan) {
      return splitAndRoutePlan((AlterTimeSeriesPlan) plan);
    } else if (plan instanceof CreateMultiTimeSeriesPlan) {
      return splitAndRoutePlan((CreateMultiTimeSeriesPlan) plan);
    }
    // the if clause can be removed after the program is stable
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

  public Map<PhysicalPlan, PartitionGroup> splitAndRouteChangeMembershipLog(Log log) {
    Map<PhysicalPlan, PartitionGroup> result = new HashMap<>();
    LogPlan plan = new LogPlan(log.serialize());
    List<Node> oldRing = new ArrayList<>(partitionTable.getAllNodes());
    if (log instanceof AddNodeLog) {
      oldRing.remove(((AddNodeLog) log).getNewNode());
    } else if (log instanceof RemoveNodeLog) {
      if (!oldRing.contains(((RemoveNodeLog) log).getRemovedNode())) {
        oldRing.add(((RemoveNodeLog) log).getRemovedNode());
        oldRing.sort(Comparator.comparingInt(Node::getNodeIdentifier));
      }
    }
    for (PartitionGroup partitionGroup : partitionTable.calculateGlobalGroups(oldRing)) {
      // It doesn't need to notify the data group which will be removed from cluster.
      if (log instanceof RemoveNodeLog
          && partitionGroup.getHeader().getNode().equals(((RemoveNodeLog) log).getRemovedNode())) {
        continue;
      }
      result.put(new LogPlan(plan), partitionGroup);
    }
    return result;
  }

  private Map<PhysicalPlan, PartitionGroup> splitAndRoutePlan(InsertRowPlan plan)
      throws MetadataException {
    PartitionGroup partitionGroup =
        partitionTable.partitionByPathTime(plan.getPrefixPath(), plan.getTime());
    return Collections.singletonMap(plan, partitionGroup);
  }

  private Map<PhysicalPlan, PartitionGroup> splitAndRoutePlan(AlterTimeSeriesPlan plan)
      throws MetadataException {
    PartitionGroup partitionGroup = partitionTable.partitionByPathTime(plan.getPath(), 0);
    return Collections.singletonMap(plan, partitionGroup);
  }

  private Map<PhysicalPlan, PartitionGroup> splitAndRoutePlan(CreateTimeSeriesPlan plan)
      throws MetadataException {
    PartitionGroup partitionGroup = partitionTable.partitionByPathTime(plan.getPath(), 0);
    return Collections.singletonMap(plan, partitionGroup);
  }

  private Map<PhysicalPlan, PartitionGroup> splitAndRoutePlan(CreateAlignedTimeSeriesPlan plan)
      throws MetadataException {
    PartitionGroup partitionGroup = partitionTable.partitionByPathTime(plan.getPrefixPath(), 0);
    return Collections.singletonMap(plan, partitionGroup);
  }

  /**
   * @param plan InsertMultiTabletPlan
   * @return key is InsertMultiTabletPlan, value is the partition group the plan belongs to, all
   *     InsertTabletPlans in InsertMultiTabletPlan belongs to one same storage group.
   */
  private Map<PhysicalPlan, PartitionGroup> splitAndRoutePlan(InsertMultiTabletPlan plan)
      throws MetadataException {
    /*
     * the key of pgSgPathPlanMap is the partition group; the value is one map,
     * the key of the map is storage group, the value is the InsertMultiTabletPlan,
     * all InsertTabletPlans in InsertMultiTabletPlan belongs to one same storage group.
     */
    Map<PartitionGroup, Map<PartialPath, InsertMultiTabletPlan>> pgSgPathPlanMap = new HashMap<>();
    for (int i = 0; i < plan.getInsertTabletPlanList().size(); i++) {
      InsertTabletPlan insertTabletPlan = plan.getInsertTabletPlanList().get(i);
      Map<PhysicalPlan, PartitionGroup> tmpResult = splitAndRoutePlan(insertTabletPlan);
      for (Map.Entry<PhysicalPlan, PartitionGroup> entry : tmpResult.entrySet()) {
        // 1. handle the value returned by call splitAndRoutePlan(InsertTabletPlan)
        InsertTabletPlan tmpPlan = (InsertTabletPlan) entry.getKey();
        PartitionGroup tmpPg = entry.getValue();
        // 1.1 the sg that the plan(actually calculated based on device) belongs to
        PartialPath tmpSgPath = IoTDB.metaManager.getBelongedStorageGroup(tmpPlan.getPrefixPath());
        Map<PartialPath, InsertMultiTabletPlan> sgPathPlanMap = pgSgPathPlanMap.get(tmpPg);
        if (sgPathPlanMap == null) {
          // 2.1 construct the InsertMultiTabletPlan
          List<InsertTabletPlan> insertTabletPlanList = new ArrayList<>();
          List<Integer> parentInsetTablePlanIndexList = new ArrayList<>();
          insertTabletPlanList.add(tmpPlan);
          parentInsetTablePlanIndexList.add(i);
          InsertMultiTabletPlan insertMultiTabletPlan =
              new InsertMultiTabletPlan(insertTabletPlanList, parentInsetTablePlanIndexList);

          // 2.2 construct the sgPathPlanMap
          sgPathPlanMap = new HashMap<>();
          sgPathPlanMap.put(tmpSgPath, insertMultiTabletPlan);

          // 2.3 put the sgPathPlanMap to the pgSgPathPlanMap
          pgSgPathPlanMap.put(tmpPg, sgPathPlanMap);
        } else {
          InsertMultiTabletPlan insertMultiTabletPlan = sgPathPlanMap.get(tmpSgPath);
          if (insertMultiTabletPlan == null) {
            List<InsertTabletPlan> insertTabletPlanList = new ArrayList<>();
            List<Integer> parentInsetTablePlanIndexList = new ArrayList<>();
            insertTabletPlanList.add(tmpPlan);
            parentInsetTablePlanIndexList.add(i);
            insertMultiTabletPlan =
                new InsertMultiTabletPlan(insertTabletPlanList, parentInsetTablePlanIndexList);

            // 2.4 put the insertMultiTabletPlan to the tmpSgPath
            sgPathPlanMap.put(tmpSgPath, insertMultiTabletPlan);
          } else {
            // 2.5 just add the tmpPlan to the insertMultiTabletPlan
            insertMultiTabletPlan.addInsertTabletPlan(tmpPlan, i);
          }
        }
      }
    }

    Map<PhysicalPlan, PartitionGroup> result = new HashMap<>(pgSgPathPlanMap.values().size());
    for (Map.Entry<PartitionGroup, Map<PartialPath, InsertMultiTabletPlan>> pgMapEntry :
        pgSgPathPlanMap.entrySet()) {
      PartitionGroup pg = pgMapEntry.getKey();
      Map<PartialPath, InsertMultiTabletPlan> sgPathPlanMap = pgMapEntry.getValue();
      // All InsertTabletPlan in InsertMultiTabletPlan belong to the same storage group
      for (Map.Entry<PartialPath, InsertMultiTabletPlan> sgPathEntry : sgPathPlanMap.entrySet()) {
        result.put(sgPathEntry.getValue(), pg);
      }
    }
    return result;
  }

  /**
   * @param insertRowsPlan InsertRowsPlan
   * @return key is InsertRowsPlan, value is the partition group the plan belongs to, all
   *     InsertRowPlans in InsertRowsPlan belongs to one same storage group.
   */
  private Map<PhysicalPlan, PartitionGroup> splitAndRoutePlan(InsertRowsPlan insertRowsPlan)
      throws MetadataException {
    Map<PhysicalPlan, PartitionGroup> result = new HashMap<>();
    Map<PartitionGroup, InsertRowsPlan> groupPlanMap = new HashMap<>();
    for (int i = 0; i < insertRowsPlan.getInsertRowPlanList().size(); i++) {
      InsertRowPlan rowPlan = insertRowsPlan.getInsertRowPlanList().get(i);
      PartialPath storageGroup = getMManager().getBelongedStorageGroup(rowPlan.getPrefixPath());
      PartitionGroup group = partitionTable.route(storageGroup.getFullPath(), rowPlan.getTime());
      if (groupPlanMap.containsKey(group)) {
        InsertRowsPlan tmpPlan = groupPlanMap.get(group);
        tmpPlan.addOneInsertRowPlan(rowPlan, i);
      } else {
        InsertRowsPlan tmpPlan = new InsertRowsPlan();
        tmpPlan.addOneInsertRowPlan(rowPlan, i);
        groupPlanMap.put(group, tmpPlan);
      }
    }

    for (Entry<PartitionGroup, InsertRowsPlan> entry : groupPlanMap.entrySet()) {
      result.put(entry.getValue(), entry.getKey());
    }
    return result;
  }

  @SuppressWarnings("SuspiciousSystemArraycopy")
  private Map<PhysicalPlan, PartitionGroup> splitAndRoutePlan(InsertTabletPlan plan)
      throws MetadataException {
    PartialPath storageGroup = getMManager().getBelongedStorageGroup(plan.getPrefixPath());
    Map<PhysicalPlan, PartitionGroup> result = new HashMap<>();
    long[] times = plan.getTimes();
    if (times.length == 0) {
      return Collections.emptyMap();
    }
    long startTime =
        (times[0] / StorageEngine.getTimePartitionInterval())
            * StorageEngine.getTimePartitionInterval(); // included
    long endTime = startTime + StorageEngine.getTimePartitionInterval(); // excluded
    int startLoc = 0; // included

    Map<PartitionGroup, List<Integer>> splitMap = new HashMap<>();
    // for each List in split, they are range1.start, range1.end, range2.start, range2.end, ...
    for (int i = 1; i < times.length; i++) { // times are sorted in session API.
      if (times[i] >= endTime) {
        // a new range.
        PartitionGroup group = partitionTable.route(storageGroup.getFullPath(), startTime);
        List<Integer> ranges = splitMap.computeIfAbsent(group, x -> new ArrayList<>());
        ranges.add(startLoc); // included
        ranges.add(i); // excluded
        // next init
        startLoc = i;
        startTime = endTime;
        endTime =
            (times[i] / StorageEngine.getTimePartitionInterval() + 1)
                * StorageEngine.getTimePartitionInterval();
      }
    }
    // the final range
    PartitionGroup group = partitionTable.route(storageGroup.getFullPath(), startTime);
    List<Integer> ranges = splitMap.computeIfAbsent(group, x -> new ArrayList<>());
    ranges.add(startLoc); // included
    ranges.add(times.length); // excluded

    List<Integer> locs;
    for (Map.Entry<PartitionGroup, List<Integer>> entry : splitMap.entrySet()) {
      // generate a new times and values
      locs = entry.getValue();
      int count = 0;
      for (int i = 0; i < locs.size(); i += 2) {
        int start = locs.get(i);
        int end = locs.get(i + 1);
        count += end - start;
      }
      long[] subTimes = new long[count];
      int destLoc = 0;
      Object[] values = initTabletValues(plan.getDataTypes().length, count, plan.getDataTypes());
      BitMap[] bitMaps =
          plan.getBitMaps() == null ? null : initBitmaps(plan.getDataTypes().length, count);
      for (int i = 0; i < locs.size(); i += 2) {
        int start = locs.get(i);
        int end = locs.get(i + 1);
        System.arraycopy(plan.getTimes(), start, subTimes, destLoc, end - start);
        for (int k = 0; k < values.length; k++) {
          System.arraycopy(plan.getColumns()[k], start, values[k], destLoc, end - start);
          if (bitMaps != null && plan.getBitMaps()[k] != null) {
            BitMap.copyOfRange(plan.getBitMaps()[k], start, bitMaps[k], destLoc, end - start);
          }
        }
        destLoc += end - start;
      }
      InsertTabletPlan newBatch = PartitionUtils.copy(plan, subTimes, values, bitMaps);
      newBatch.setRange(locs);
      newBatch.setAligned(plan.isAligned());
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

  private BitMap[] initBitmaps(int columnSize, int rowSize) {
    BitMap[] bitMaps = new BitMap[columnSize];
    for (int i = 0; i < columnSize; i++) {
      bitMaps[i] = new BitMap(rowSize);
    }
    return bitMaps;
  }

  private Map<PhysicalPlan, PartitionGroup> splitAndRoutePlan(CreateMultiTimeSeriesPlan plan)
      throws MetadataException {
    Map<PhysicalPlan, PartitionGroup> result = new HashMap<>();
    Map<PartitionGroup, PhysicalPlan> groupHoldPlan = new HashMap<>();

    for (int i = 0; i < plan.getPaths().size(); i++) {
      PartialPath path = plan.getPaths().get(i);
      if (plan.getResults().containsKey(i)) {
        continue;
      }
      PartitionGroup partitionGroup = partitionTable.partitionByPathTime(path, 0);
      CreateMultiTimeSeriesPlan subPlan;
      if (groupHoldPlan.get(partitionGroup) == null) {
        subPlan = createSubPlan(plan);
        groupHoldPlan.put(partitionGroup, subPlan);
      } else {
        subPlan = (CreateMultiTimeSeriesPlan) groupHoldPlan.get(partitionGroup);
      }

      subPlan.getPaths().add(path);
      subPlan.getDataTypes().add(plan.getDataTypes().get(i));
      subPlan.getEncodings().add(plan.getEncodings().get(i));
      subPlan.getCompressors().add(plan.getCompressors().get(i));
      if (plan.getAlias() != null) {
        subPlan.getAlias().add(plan.getAlias().get(i));
      }
      if (plan.getProps() != null) {
        subPlan.getProps().add(plan.getProps().get(i));
      }
      if (plan.getTags() != null) {
        subPlan.getTags().add(plan.getTags().get(i));
      }
      if (plan.getAttributes() != null) {
        subPlan.getAttributes().add(plan.getAttributes().get(i));
      }
      subPlan.getIndexes().add(i);
    }

    for (Map.Entry<PartitionGroup, PhysicalPlan> entry : groupHoldPlan.entrySet()) {
      result.put(entry.getValue(), entry.getKey());
    }
    return result;
  }

  private CreateMultiTimeSeriesPlan createSubPlan(CreateMultiTimeSeriesPlan plan) {
    CreateMultiTimeSeriesPlan subPlan = new CreateMultiTimeSeriesPlan();
    subPlan.setPaths(new ArrayList<>());
    subPlan.setDataTypes(new ArrayList<>());
    subPlan.setEncodings(new ArrayList<>());
    subPlan.setCompressors(new ArrayList<>());
    if (plan.getAlias() != null) {
      subPlan.setAlias(new ArrayList<>());
    }
    if (plan.getProps() != null) {
      subPlan.setProps(new ArrayList<>());
    }
    if (plan.getTags() != null) {
      subPlan.setTags(new ArrayList<>());
    }
    if (plan.getAttributes() != null) {
      subPlan.setAttributes(new ArrayList<>());
    }
    subPlan.setIndexes(new ArrayList<>());
    return subPlan;
  }

  /**
   * @param plan InsertRowsOfOneDevicePlan
   * @return key is InsertRowsOfOneDevicePlan, value is the partition group the plan belongs to. All
   *     InsertRowPlans in InsertRowsOfOneDevicePlan belong to one same storage group.
   */
  private Map<PhysicalPlan, PartitionGroup> splitAndRoutePlan(InsertRowsOfOneDevicePlan plan)
      throws MetadataException {
    Map<PhysicalPlan, PartitionGroup> result = new HashMap<>();
    Map<PartitionGroup, List<InsertRowPlan>> groupPlanMap = new HashMap<>();
    Map<PartitionGroup, List<Integer>> groupPlanIndexMap = new HashMap<>();
    PartialPath storageGroup = getMManager().getBelongedStorageGroup(plan.getPrefixPath());
    for (int i = 0; i < plan.getRowPlans().length; i++) {
      InsertRowPlan p = plan.getRowPlans()[i];
      PartitionGroup group = partitionTable.route(storageGroup.getFullPath(), p.getTime());
      List<InsertRowPlan> groupedPlans =
          groupPlanMap.computeIfAbsent(group, k -> new ArrayList<>());
      List<Integer> groupedPlanIndex =
          groupPlanIndexMap.computeIfAbsent(group, k -> new ArrayList<>());
      groupedPlans.add(p);
      groupedPlanIndex.add(plan.getRowPlanIndexList()[i]);
    }

    for (Entry<PartitionGroup, List<InsertRowPlan>> entry : groupPlanMap.entrySet()) {
      PhysicalPlan reducedPlan =
          new InsertRowsOfOneDevicePlan(
              plan.getPrefixPath(),
              entry.getValue().toArray(new InsertRowPlan[0]),
              groupPlanIndexMap.get(entry.getKey()).stream().mapToInt(i -> i).toArray());
      result.put(reducedPlan, entry.getKey());
    }
    return result;
  }
}
