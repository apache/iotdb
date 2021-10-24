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

import org.apache.iotdb.cluster.partition.PartitionTable;
import org.apache.iotdb.cluster.rpc.thrift.RaftNode;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.CreateTemplatePlan;
import org.apache.iotdb.db.qp.physical.crud.DeletePlan;
import org.apache.iotdb.db.qp.physical.crud.InsertTabletPlan;
import org.apache.iotdb.db.qp.physical.crud.SetSchemaTemplatePlan;
import org.apache.iotdb.db.qp.physical.sys.AuthorPlan;
import org.apache.iotdb.db.qp.physical.sys.ClearCachePlan;
import org.apache.iotdb.db.qp.physical.sys.CreateFunctionPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateSnapshotPlan;
import org.apache.iotdb.db.qp.physical.sys.DataAuthPlan;
import org.apache.iotdb.db.qp.physical.sys.DeleteStorageGroupPlan;
import org.apache.iotdb.db.qp.physical.sys.DeleteTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.DropFunctionPlan;
import org.apache.iotdb.db.qp.physical.sys.FlushPlan;
import org.apache.iotdb.db.qp.physical.sys.LoadConfigurationPlan;
import org.apache.iotdb.db.qp.physical.sys.LoadConfigurationPlan.LoadConfigurationPlanType;
import org.apache.iotdb.db.qp.physical.sys.LoadDataPlan;
import org.apache.iotdb.db.qp.physical.sys.MergePlan;
import org.apache.iotdb.db.qp.physical.sys.OperateFilePlan;
import org.apache.iotdb.db.qp.physical.sys.SetStorageGroupPlan;
import org.apache.iotdb.db.qp.physical.sys.SetSystemModePlan;
import org.apache.iotdb.db.qp.physical.sys.SetTTLPlan;
import org.apache.iotdb.db.qp.physical.sys.ShowTTLPlan;
import org.apache.iotdb.service.rpc.thrift.TSStatus;
import org.apache.iotdb.tsfile.utils.BitMap;
import org.apache.iotdb.tsfile.utils.Murmur128Hash;

import java.util.List;
import java.util.Set;

import static org.apache.iotdb.cluster.config.ClusterConstant.HASH_SALT;

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
            && ((LoadConfigurationPlan) plan)
                .getLoadConfigurationPlanType()
                .equals(LoadConfigurationPlanType.LOCAL));
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
        || (plan instanceof LoadConfigurationPlan
            && ((LoadConfigurationPlan) plan)
                .getLoadConfigurationPlanType()
                .equals(LoadConfigurationPlanType.GLOBAL))
        || plan instanceof AuthorPlan
        || plan instanceof DeleteStorageGroupPlan
        // DataAuthPlan is global because all nodes must have all user info
        || plan instanceof DataAuthPlan
        || plan instanceof CreateTemplatePlan
        || plan instanceof CreateFunctionPlan
        || plan instanceof DropFunctionPlan
        || plan instanceof CreateSnapshotPlan
        || plan instanceof SetSystemModePlan;
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
        || plan instanceof FlushPlan
        || plan instanceof SetSchemaTemplatePlan
        || plan instanceof ClearCachePlan;
  }

  public static int calculateStorageGroupSlotByTime(
      String storageGroupName, long timestamp, int slotNum) {
    long partitionNum = StorageEngine.getTimePartition(timestamp);
    return calculateStorageGroupSlotByPartition(storageGroupName, partitionNum, slotNum);
  }

  private static int calculateStorageGroupSlotByPartition(
      String storageGroupName, long partitionNum, int slotNum) {
    int hash = Murmur128Hash.hash(storageGroupName, partitionNum, HASH_SALT);
    return Math.abs(hash % slotNum);
  }

  public static InsertTabletPlan copy(
      InsertTabletPlan plan, long[] times, Object[] values, BitMap[] bitMaps) {
    InsertTabletPlan newPlan = new InsertTabletPlan(plan.getPrefixPath(), plan.getMeasurements());
    newPlan.setDataTypes(plan.getDataTypes());
    // according to TSServiceImpl.insertBatch(), only the deviceId, measurements, dataTypes,
    // times, columns, and rowCount are need to be maintained.
    newPlan.setColumns(values);
    newPlan.setBitMaps(bitMaps);
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
  public static void getIntervalHeaders(
      String storageGroupName,
      long timeLowerBound,
      long timeUpperBound,
      PartitionTable partitionTable,
      Set<RaftNode> result) {
    long partitionInterval = StorageEngine.getTimePartitionInterval();
    long currPartitionStart = timeLowerBound / partitionInterval * partitionInterval;
    while (currPartitionStart <= timeUpperBound) {
      result.add(partitionTable.routeToHeaderByTime(storageGroupName, currPartitionStart));
      currPartitionStart += partitionInterval;
    }
  }
}
