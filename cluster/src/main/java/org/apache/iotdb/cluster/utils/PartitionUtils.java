/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at      http://www.apache.org/licenses/LICENSE-2.0  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.iotdb.cluster.utils;

import static org.apache.iotdb.cluster.config.ClusterConstant.HASH_SALT;

import java.util.Objects;
import org.apache.commons.collections4.map.MultiKeyMap;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.exception.UnsupportedPlanException;
import org.apache.iotdb.cluster.log.Log;
import org.apache.iotdb.cluster.log.logtypes.PhysicalPlanLog;
import org.apache.iotdb.cluster.partition.PartitionGroup;
import org.apache.iotdb.cluster.partition.PartitionTable;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.metadata.StorageGroupNotSetException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.BatchInsertPlan;
import org.apache.iotdb.db.qp.physical.crud.DeletePlan;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateTimeSeriesPlan;
import org.apache.iotdb.tsfile.utils.Murmur128Hash;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PartitionUtils {

  private static final Logger logger = LoggerFactory.getLogger(PartitionUtils.class);
  private static final long PARTITION_INTERVAL = ClusterDescriptor.getINSTANCE().getConfig().getPartitionInterval();

  private PartitionUtils() {
    // util class
  }

  public static boolean isPlanPartitioned(PhysicalPlan plan) {
    // TODO-Cluster#348: support more plans
    return plan instanceof CreateTimeSeriesPlan ||
        plan instanceof InsertPlan ||
        plan instanceof BatchInsertPlan ||
        plan instanceof DeletePlan;
  }

  public static int calculateLogSlot(Log log, PartitionTable partitionTable) {
    if (log instanceof PhysicalPlanLog) {
      PhysicalPlanLog physicalPlanLog = ((PhysicalPlanLog) log);
      PhysicalPlan plan = physicalPlanLog.getPlan();
      String storageGroup = null;
      if (plan instanceof CreateTimeSeriesPlan) {
        try {
          storageGroup = MManager.getInstance()
              .getStorageGroupNameByPath(((CreateTimeSeriesPlan) plan).getPath().getFullPath());
          return calculateStorageGroupSlot(storageGroup, 0, partitionTable.getSlotNum());
        } catch (MetadataException e) {
          logger.error("Cannot find the storage group of {}", ((CreateTimeSeriesPlan) plan).getPath());
          return -1;
        }
      } else if (plan instanceof InsertPlan || plan instanceof BatchInsertPlan) {
        try {
          storageGroup = MManager.getInstance()
              .getStorageGroupNameByPath(((InsertPlan) plan).getDeviceId());
        } catch (StorageGroupNotSetException e) {
          logger.error("Cannot find the storage group of {}", ((CreateTimeSeriesPlan) plan).getPath());
          return -1;
        }
      } else if (plan instanceof DeletePlan) {
        //TODO deleteplan may have many SGs.
        logger.error("not implemented for DeletePlan in cluster {}", plan);
        return -1;
      }

      return Math.abs(Objects.hash(storageGroup, 0));
    }
    return 0;
  }

  public static PartitionGroup partitionPlan(PhysicalPlan plan, PartitionTable partitionTable)
      throws UnsupportedPlanException {
    // TODO-Cluster#348: support more plans
    try {
      if (plan instanceof CreateTimeSeriesPlan) {
        CreateTimeSeriesPlan createTimeSeriesPlan = ((CreateTimeSeriesPlan) plan);
        return partitionByPathTime(createTimeSeriesPlan.getPath().getFullPath(), 0, partitionTable);
      } else if (plan instanceof InsertPlan) {
        InsertPlan insertPlan = ((InsertPlan) plan);
        return partitionByPathTime(insertPlan.getDeviceId(), 0, partitionTable);
        // TODO-Cluster#350: use time in partitioning
        // return partitionByPathTime(insertPlan.getDeviceId(), insertPlan.getTime(),
        // partitionTable);
      }
    } catch (StorageGroupNotSetException e) {
      logger.debug("Storage group is not found for plan {}", plan);
      return null;
    }
    logger.error("Unable to partition plan {}", plan);
    throw new UnsupportedPlanException(plan);
  }

  public static PartitionGroup partitionByPathTime(String path, long timestamp, PartitionTable partitionTable)
      throws StorageGroupNotSetException {
    String storageGroup = MManager.getInstance().getStorageGroupNameByPath(path);
    return partitionTable.route(storageGroup, timestamp);
  }

  /**
   * Get partition info by path and range time
   *
   * @UsedBy NodeTool
   */
  public static MultiKeyMap<Long, PartitionGroup> partitionByPathRangeTime(String path,
      long startTime, long endTime, PartitionTable partitionTable)
      throws StorageGroupNotSetException {
    MultiKeyMap<Long, PartitionGroup> timeRangeMapRaftGroup = new MultiKeyMap<>();
    String storageGroup = MManager.getInstance().getStorageGroupNameByPath(path);
    while (startTime <= endTime) {
      long nextTime = (startTime / PARTITION_INTERVAL + 1) * PARTITION_INTERVAL; //FIXME considering the time unit
      timeRangeMapRaftGroup.put(startTime, Math.min(nextTime - 1, endTime),
          partitionTable.route(storageGroup, startTime));
      startTime = nextTime;
    }
    return timeRangeMapRaftGroup;
  }

  public static int calculateStorageGroupSlot(String storageGroupName, long timestamp,
      int slotNum) {
    long partitionInstance = timestamp / PARTITION_INTERVAL; //FIXME considering the time unit
    int hash = Murmur128Hash.hash(storageGroupName, partitionInstance, HASH_SALT);
    return Math.abs(hash % slotNum);
  }
}
