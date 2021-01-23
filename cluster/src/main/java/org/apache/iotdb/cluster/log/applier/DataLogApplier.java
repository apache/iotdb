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
import org.apache.iotdb.cluster.exception.CheckConsistencyException;
import org.apache.iotdb.cluster.log.Log;
import org.apache.iotdb.cluster.log.logtypes.CloseFileLog;
import org.apache.iotdb.cluster.log.logtypes.PhysicalPlanLog;
import org.apache.iotdb.cluster.partition.slot.SlotPartitionTable;
import org.apache.iotdb.cluster.server.member.DataGroupMember;
import org.apache.iotdb.cluster.server.member.MetaGroupMember;
import org.apache.iotdb.cluster.utils.IOUtils;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.PathNotExistException;
import org.apache.iotdb.db.exception.metadata.StorageGroupNotSetException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertMultiTabletPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertTabletPlan;
import org.apache.iotdb.db.service.IoTDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DataLogApplier applies logs like data insertion/deletion/update and timeseries creation to
 * IoTDB.
 */
public class DataLogApplier extends BaseApplier {

  private static final Logger logger = LoggerFactory.getLogger(DataLogApplier.class);

  private DataGroupMember dataGroupMember;

  public DataLogApplier(MetaGroupMember metaGroupMember, DataGroupMember dataGroupMember) {
    super(metaGroupMember);
    this.dataGroupMember = dataGroupMember;
  }

  @Override
  public void apply(Log log) {
    logger.debug("DataMember [{}] start applying Log {}", dataGroupMember.getName(), log);

    try {
      if (log instanceof PhysicalPlanLog) {
        PhysicalPlanLog physicalPlanLog = (PhysicalPlanLog) log;
        PhysicalPlan plan = physicalPlanLog.getPlan();
        if (plan instanceof InsertMultiTabletPlan) {
          applyInsert((InsertMultiTabletPlan) plan);
        } else if (plan instanceof InsertPlan) {
          applyInsert((InsertPlan) plan);
        } else {
          applyPhysicalPlan(plan, dataGroupMember);
        }
      } else if (log instanceof CloseFileLog) {
        CloseFileLog closeFileLog = ((CloseFileLog) log);
        StorageEngine.getInstance()
            .closeStorageGroupProcessor(new PartialPath(closeFileLog.getStorageGroupName()),
                closeFileLog.getPartitionId(),
                closeFileLog.isSeq(), false);
      } else {
        logger.error("Unsupported log: {}", log);
      }
    } catch (Exception e) {
      Throwable rootCause = IOUtils.getRootCause(e);
      if (!(rootCause instanceof PathNotExistException)) {
        logger.debug("Exception occurred when applying {}", log, e);
      }
      log.setException(e);
    } finally {
      log.setApplied(true);
    }
  }

  private void applyInsert(InsertMultiTabletPlan plan)
      throws StorageGroupNotSetException, QueryProcessException, StorageEngineException {
    for (InsertTabletPlan insertTabletPlan : plan.getInsertTabletPlanList()) {
      applyInsert(insertTabletPlan);
    }
  }

  private void applyInsert(InsertPlan plan)
      throws StorageGroupNotSetException, QueryProcessException, StorageEngineException {
    // check if the corresponding slot is being pulled
    PartialPath sg;
    long time = plan.getMinTime();
    try {
      sg = IoTDB.metaManager.getStorageGroupPath(plan.getDeviceId());
    } catch (StorageGroupNotSetException e) {
      // the sg may not exist because the node does not catch up with the leader, retry after
      // synchronization
      try {
        metaGroupMember.syncLeaderWithConsistencyCheck(true);
      } catch (CheckConsistencyException ce) {
        throw new QueryProcessException(ce.getMessage());
      }
      sg = IoTDB.metaManager.getStorageGroupPath(plan.getDeviceId());
    }
    int slotId = SlotPartitionTable.getSlotStrategy().calculateSlotByTime(sg.getFullPath(), time,
        ClusterConstant.SLOT_NUM);
    // the slot may not be writable because it is pulling file versions, wait until it is done
    dataGroupMember.getSlotManager().waitSlotForWrite(slotId);
    applyPhysicalPlan(plan, dataGroupMember);
  }
}
