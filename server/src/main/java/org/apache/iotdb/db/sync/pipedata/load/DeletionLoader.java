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
package org.apache.iotdb.db.sync.pipedata.load;

import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.exception.sync.PipeDataLoadException;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.modification.Deletion;
import org.apache.iotdb.db.exception.LoadFileException;
import org.apache.iotdb.db.mpp.plan.Coordinator;
import org.apache.iotdb.db.mpp.plan.execution.ExecutionResult;
import org.apache.iotdb.db.mpp.plan.statement.Statement;
import org.apache.iotdb.db.mpp.plan.statement.crud.DeleteDataStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.DeleteTimeSeriesStatement;
import org.apache.iotdb.db.query.control.SessionManager;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;

/** This loader is used to load deletion plan. */
public class DeletionLoader implements ILoader {
  private static final Logger logger = LoggerFactory.getLogger(DeletionLoader.class);

  private Deletion deletion;

  public DeletionLoader(Deletion deletion) {
    this.deletion = deletion;
  }

  @Override
  public void load() throws PipeDataLoadException {
    if (CommonDescriptor.getInstance().getConfig().isReadOnly()) {
      throw new PipeDataLoadException("storage engine readonly");
    }
    try {
      Statement statement = generateStatement();
      long queryId = SessionManager.getInstance().requestQueryId();
      ExecutionResult result =
          Coordinator.getInstance()
              .execute(
                  statement,
                  queryId,
                  null,
                  "",
                  PARTITION_FETCHER,
                  SCHEMA_FETCHER,
                  IoTDBDescriptor.getInstance().getConfig().getQueryTimeoutThreshold());
      if (result.status.code != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        logger.error(String.format("Delete %s error, statement: %s.", deletion, statement));
        logger.error(String.format("Delete result status : %s.", result.status));
        throw new LoadFileException(
            String.format("Can not execute delete statement: %s", statement));
      }
    } catch (Exception e) {
      throw new PipeDataLoadException(e.getMessage());
    }
  }

  private Statement generateStatement() {
    if (deletion.getStartTime() == Long.MIN_VALUE && deletion.getEndTime() == Long.MAX_VALUE) {
      return new DeleteTimeSeriesStatement(Collections.singletonList(deletion.getPath()));
    }
    DeleteDataStatement statement = new DeleteDataStatement();
    statement.setPathList(Collections.singletonList(deletion.getPath()));
    statement.setDeleteStartTime(deletion.getStartTime());
    statement.setDeleteEndTime(deletion.getEndTime());
    return statement;
  }
}
