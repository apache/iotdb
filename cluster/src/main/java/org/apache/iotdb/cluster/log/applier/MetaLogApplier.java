/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at      http://www.apache.org/licenses/LICENSE-2.0  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.iotdb.cluster.log.applier;

import org.apache.iotdb.cluster.log.Log;
import org.apache.iotdb.cluster.log.LogApplier;
import org.apache.iotdb.cluster.log.logs.AddNodeLog;
import org.apache.iotdb.cluster.log.logs.MetaPlanLog;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.server.member.MetaGroupMember;
import org.apache.iotdb.db.exception.ProcessorException;
import org.apache.iotdb.db.qp.executor.QueryProcessExecutor;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.sys.SetStorageGroupPlan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MetaLogApplier applies logs like node addition and storage group creation to IoTDB.
 */
public class MetaLogApplier implements LogApplier {

  private static final Logger logger = LoggerFactory.getLogger(MetaLogApplier.class);
  private MetaGroupMember member;
  private QueryProcessExecutor queryExecutor;

  public MetaLogApplier(MetaGroupMember member) {
    this.member = member;
  }

  @Override
  public void apply(Log log) throws ProcessorException {
    logger.debug("Applying {}", log);
    if (log instanceof AddNodeLog) {
      AddNodeLog addNodeLog = (AddNodeLog) log;
      Node newNode = addNodeLog.getNewNode();
      member.applyAddNode(newNode);
    } else if (log instanceof MetaPlanLog) {
      applyPhysicalPlan(((MetaPlanLog) log).getPlan());
    } else {
      // TODO-Cluster support more types of logs
      logger.error("Unsupported log: {}", log);
    }
  }

  private void applyPhysicalPlan(PhysicalPlan plan) throws ProcessorException {
    if (plan instanceof SetStorageGroupPlan) {
      getQueryExecutor().processNonQuery(plan);
    } else {
      // TODO-Cluster support more types of logs
      logger.error("Unsupported physical plan: {}", plan);
    }
  }

  @Override
  public void revert(Log log) {
    if (log instanceof AddNodeLog) {
      // TODO-Cluster implement node deletion
    } else {
      // TODO-Cluster support more types of logs
      logger.error("Unsupported log: {}", log);
    }
  }

  private QueryProcessExecutor getQueryExecutor() {
    if (queryExecutor == null) {
      queryExecutor = new QueryProcessExecutor();
    }
    return queryExecutor;
  }
}
