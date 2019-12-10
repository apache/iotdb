/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at      http://www.apache.org/licenses/LICENSE-2.0  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.iotdb.cluster.utils;

import static org.apache.iotdb.cluster.config.ClusterConstant.HASH_SALT;
import static org.apache.iotdb.cluster.partition.SlotPartitionTable.PARTITION_INTERVAL;

import java.util.Objects;
import org.apache.iotdb.cluster.config.ClusterConstant;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PartitionUtils {

  private static final Logger logger = LoggerFactory.getLogger(PartitionUtils.class);

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
      if (plan instanceof CreateTimeSeriesPlan) {
        String storageGroup;
        try {
          storageGroup = MManager.getInstance()
              .getStorageGroupNameByPath(((CreateTimeSeriesPlan) plan).getPath().getFullPath());
          return Math.abs(Objects.hash(storageGroup, 0));
        } catch (MetadataException e) {
          logger.error("Cannot find the storage group of {}", ((CreateTimeSeriesPlan) plan).getPath());
          return 0;
        }
      }
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

  public static int calculateStorageGroupSlot(String storageGroupName, long timestamp) {
    long partitionInstance = timestamp / PARTITION_INTERVAL;
    int hash = Objects.hash(storageGroupName, partitionInstance * HASH_SALT);
    return Math.abs(hash % ClusterConstant.SLOT_NUM);
  }
}
