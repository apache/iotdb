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

package org.apache.iotdb.cluster.common;

import org.apache.iotdb.cluster.log.Log;
import org.apache.iotdb.cluster.log.LogApplier;
import org.apache.iotdb.cluster.log.logtypes.CloseFileLog;
import org.apache.iotdb.cluster.log.logtypes.PhysicalPlanLog;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.StorageGroupNotSetException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.executor.PlanExecutor;

public class TestLogApplier implements LogApplier {

  private PlanExecutor planExecutor;

  @Override
  public void apply(Log log) {
    try {
      if (log instanceof PhysicalPlanLog) {
        PhysicalPlanLog physicalPlanLog = (PhysicalPlanLog) log;
        getPlanExecutor().processNonQuery(physicalPlanLog.getPlan());
      } else if (log instanceof CloseFileLog) {
        CloseFileLog closeFileLog = ((CloseFileLog) log);
        try {
          StorageEngine.getInstance()
              .closeStorageGroupProcessor(
                  new PartialPath(closeFileLog.getStorageGroupName()),
                  closeFileLog.getPartitionId(),
                  closeFileLog.isSeq(),
                  false);
        } catch (StorageGroupNotSetException | IllegalPathException e) {
          throw new QueryProcessException(e);
        }
      }
    } catch (Exception e) {
      log.setException(e);
    } finally {
      log.setApplied(true);
    }
  }

  public PlanExecutor getPlanExecutor() throws QueryProcessException {
    return planExecutor == null ? planExecutor = new PlanExecutor() : planExecutor;
  }
}
