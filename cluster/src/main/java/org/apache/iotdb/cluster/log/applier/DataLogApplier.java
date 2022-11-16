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

import org.apache.iotdb.cluster.ClusterIoTDB;
import org.apache.iotdb.cluster.expr.craft.FragmentedLog;
import org.apache.iotdb.cluster.log.Log;
import org.apache.iotdb.cluster.log.logtypes.AddNodeLog;
import org.apache.iotdb.cluster.log.logtypes.CloseFileLog;
import org.apache.iotdb.cluster.log.logtypes.RemoveNodeLog;
import org.apache.iotdb.cluster.log.logtypes.RequestLog;
import org.apache.iotdb.cluster.server.member.DataGroupMember;
import org.apache.iotdb.cluster.utils.IOUtils;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.consensus.common.request.IConsensusRequest;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.exception.metadata.PathNotExistException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.physical.crud.DeletePlan;
import org.apache.iotdb.db.qp.physical.sys.DeleteTimeSeriesPlan;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DataLogApplier applies logs like data insertion/deletion/update and timeseries creation to IoTDB.
 */
public class DataLogApplier extends BaseApplier {

  private static final Logger logger = LoggerFactory.getLogger(DataLogApplier.class);

  protected DataGroupMember dataGroupMember;

  public DataLogApplier(DataGroupMember dataGroupMember) {
    super(dataGroupMember.getStateMachine());
    this.dataGroupMember = dataGroupMember;
  }

  @Override
  public void apply(Log log) {
    logger.debug("DataMember [{}] start applying Log {}", dataGroupMember.getName(), log);

    try {
      if (log instanceof AddNodeLog) {
        ClusterIoTDB.getInstance()
            .getDataGroupEngine()
            .preAddNodeForDataGroup((AddNodeLog) log, dataGroupMember);
        dataGroupMember.setAndSaveLastAppliedPartitionTableVersion(
            ((AddNodeLog) log).getMetaLogIndex());
      } else if (log instanceof RemoveNodeLog) {
        ClusterIoTDB.getInstance()
            .getDataGroupEngine()
            .preRemoveNodeForDataGroup((RemoveNodeLog) log, dataGroupMember);
        dataGroupMember.setAndSaveLastAppliedPartitionTableVersion(
            ((RemoveNodeLog) log).getMetaLogIndex());
      } else if (log instanceof RequestLog) {
        RequestLog requestLog = (RequestLog) log;
        IConsensusRequest request = requestLog.getRequest();
        TSStatus status = applyRequest(request);
        if (status.code != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
          log.setException(new QueryProcessException(status.message, status.code));
        }
      } else if (log instanceof CloseFileLog) {
        CloseFileLog closeFileLog = ((CloseFileLog) log);
        StorageEngine.getInstance()
            .closeStorageGroupProcessor(
                new PartialPath(closeFileLog.getStorageGroupName()),
                closeFileLog.getPartitionId(),
                closeFileLog.isSeq(),
                false);
      } else if (!(log instanceof FragmentedLog)) {
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

  public TSStatus applyRequest(IConsensusRequest request) {
    if (request instanceof DeletePlan) {
      ((DeletePlan) request).setPartitionFilter(dataGroupMember.getTimePartitionFilter());
    } else if (request instanceof DeleteTimeSeriesPlan) {
      ((DeleteTimeSeriesPlan) request).setPartitionFilter(dataGroupMember.getTimePartitionFilter());
    }

    return stateMachine.write(request);
  }
}
