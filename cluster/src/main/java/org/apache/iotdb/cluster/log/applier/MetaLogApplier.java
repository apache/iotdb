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

import org.apache.iotdb.cluster.exception.ChangeMembershipException;
import org.apache.iotdb.cluster.exception.UnsupportedPlanException;
import org.apache.iotdb.cluster.log.Log;
import org.apache.iotdb.cluster.log.logtypes.AddNodeLog;
import org.apache.iotdb.cluster.log.logtypes.PhysicalPlanLog;
import org.apache.iotdb.cluster.log.logtypes.RemoveNodeLog;
import org.apache.iotdb.cluster.server.member.MetaGroupMember;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.StorageGroupNotSetException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.physical.sys.LogPlan;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TSStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MetaLogApplier applies logs like node addition and storage group creation to IoTDB.
 */
public class MetaLogApplier extends BaseApplier {

  private static final Logger logger = LoggerFactory.getLogger(MetaLogApplier.class);
  private MetaGroupMember member;

  public MetaLogApplier(MetaGroupMember member) {
    super(member);
    this.member = member;
  }

  @Override
  public void apply(Log log) {
    apply(log, false);
  }

  public void apply(Log log, boolean isLeader) {
    try {
      logger.debug("MetaMember [{}] starts applying Log {}", metaGroupMember.getName(), log);
      if (log instanceof AddNodeLog) {
        if (isLeader) {
          sendLogToAllDataGroups(log);
        }
        member.applyAddNode((AddNodeLog) log);
      } else if (log instanceof PhysicalPlanLog) {
        applyPhysicalPlan(((PhysicalPlanLog) log).getPlan(), null);
      } else if (log instanceof RemoveNodeLog) {
        if (isLeader) {
          sendLogToAllDataGroups(log);
        }
        member.applyRemoveNode(((RemoveNodeLog) log));
      } else {
        logger.error("Unsupported log: {} {}", log.getClass().getName(), log);
      }
    } catch (StorageEngineException | StorageGroupNotSetException | QueryProcessException | ChangeMembershipException | UnsupportedPlanException e) {
      logger.debug("Exception occurred when executing {}", log, e);
      log.setException(e);
    } finally {
      log.setApplied(true);
    }
  }

  private void sendLogToAllDataGroups(Log log)
      throws ChangeMembershipException, UnsupportedPlanException {
    LogPlan plan = new LogPlan(log.serialize());
    TSStatus status = member.processPartitionedPlan(plan);
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new ChangeMembershipException(String.format("apply %s failed with status {%s}", log, status));
    }
  }
}
