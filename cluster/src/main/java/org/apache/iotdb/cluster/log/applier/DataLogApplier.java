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

package org.apache.iotdb.cluster.log.applier;

import org.apache.iotdb.cluster.config.ClusterConstant;
import org.apache.iotdb.cluster.log.Log;
import org.apache.iotdb.cluster.log.logtypes.CloseFileLog;
import org.apache.iotdb.cluster.log.logtypes.PhysicalPlanLog;
import org.apache.iotdb.cluster.server.member.DataGroupMember;
import org.apache.iotdb.cluster.server.member.MetaGroupMember;
import org.apache.iotdb.cluster.utils.PartitionUtils;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.metadata.PathNotExistException;
import org.apache.iotdb.db.exception.metadata.StorageGroupNotSetException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.BatchInsertPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.tsfile.read.filter.operator.In;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DataLogApplier applies logs like data insertion/deletion/update and timeseries creation to IoTDB.
 */
public class DataLogApplier extends BaseApplier {

  private static final Logger logger = LoggerFactory.getLogger(DataLogApplier.class);

  private DataGroupMember dataGroupMember;

  public DataLogApplier(MetaGroupMember metaGroupMember, DataGroupMember dataGroupMember) {
    super(metaGroupMember);
    this.dataGroupMember = dataGroupMember;
  }

  @Override
  public void apply(Log log) throws QueryProcessException {
    if (log instanceof PhysicalPlanLog) {
      PhysicalPlanLog physicalPlanLog = (PhysicalPlanLog) log;
      PhysicalPlan plan = physicalPlanLog.getPlan();
      if (plan instanceof InsertPlan || plan instanceof BatchInsertPlan) {
        try {
          applyInsert(plan);
        } catch (StorageGroupNotSetException e) {
          throw new QueryProcessException(e);
        }
      } else {
        applyPhysicalPlan(plan);
      }
    } else if (log instanceof CloseFileLog) {
      CloseFileLog closeFileLog = ((CloseFileLog) log);
      try {
        StorageEngine.getInstance().asyncCloseProcessor(closeFileLog.getStorageGroupName(),
            closeFileLog.getPartitionId(),
            closeFileLog.isSeq());
      } catch (StorageGroupNotSetException e) {
        logger.error("Cannot close {} file in {}, partitionId {}",
            closeFileLog.isSeq() ? "seq" : "unseq",
            closeFileLog.getStorageGroupName(), closeFileLog.getPartitionId());
      }
    } else {
      // TODO-Cluster#348 support more types of logs
      logger.error("Unsupported log: {}", log);
    }
  }

  private void applyInsert(PhysicalPlan plan)
      throws StorageGroupNotSetException, QueryProcessException {
    // check if the corresponding slot is being pulled
    String sg;
    long time;
    try {
      if (plan instanceof InsertPlan) {
        InsertPlan insertPlan = (InsertPlan) plan;
        sg = MManager.getInstance().getStorageGroupName(insertPlan.getDeviceId());
        time = insertPlan.getTime();
      } else {
        BatchInsertPlan batchInsertPlan = (BatchInsertPlan) plan;
        sg = MManager.getInstance().getStorageGroupName(batchInsertPlan.getDeviceId());
        time = batchInsertPlan.getMinTime();
      }
    } catch (StorageGroupNotSetException e) {
      // the sg may not exist because the node does not catch up with the leader, retry after
      // synchronization
      metaGroupMember.syncLeader();
      if (plan instanceof InsertPlan) {
        InsertPlan insertPlan = (InsertPlan) plan;
        sg = MManager.getInstance().getStorageGroupName(insertPlan.getDeviceId());
        time = insertPlan.getTime();
      } else {
        BatchInsertPlan batchInsertPlan = (BatchInsertPlan) plan;
        sg = MManager.getInstance().getStorageGroupName(batchInsertPlan.getDeviceId());
        time = batchInsertPlan.getMinTime();
      }
    }
    int slotId = PartitionUtils.calculateStorageGroupSlotByTime(sg, time, ClusterConstant.SLOT_NUM);
    // the slot may not be writable because it is pulling file versions, wait until it is done
    dataGroupMember.getSlotManager().waitSlotForWrite(slotId);
    applyPhysicalPlan(plan);
  }
}
