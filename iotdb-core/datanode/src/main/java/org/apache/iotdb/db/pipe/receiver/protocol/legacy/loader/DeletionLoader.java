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

package org.apache.iotdb.db.pipe.receiver.protocol.legacy.loader;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.queryengine.common.SessionInfo;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.load.LoadFileException;
import org.apache.iotdb.db.i18n.DataNodePipeMessages;
import org.apache.iotdb.db.protocol.session.SessionManager;
import org.apache.iotdb.db.queryengine.plan.Coordinator;
import org.apache.iotdb.db.queryengine.plan.execution.ExecutionResult;
import org.apache.iotdb.db.queryengine.plan.relational.security.TreeAccessCheckContext;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.DeleteDataStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.DeleteTimeSeriesStatement;
import org.apache.iotdb.db.storageengine.dataregion.modification.v1.Deletion;
import org.apache.iotdb.pipe.api.exception.PipeException;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;

/** This loader is used to load deletion plan. */
public class DeletionLoader implements ILoader {

  private static final Logger LOGGER = LoggerFactory.getLogger(DeletionLoader.class);

  private final Deletion deletion;

  public DeletionLoader(Deletion deletion) {
    this.deletion = deletion;
  }

  @Override
  public void load(final SessionInfo sessionInfo) throws PipeException {
    if (CommonDescriptor.getInstance().getConfig().isReadOnly()) {
      throw new PipeException(DataNodePipeMessages.STORAGE_ENGINE_READONLY);
    }
    try {
      Statement statement = generateStatement();
      TSStatus authorityStatus =
          AuthorityChecker.checkAuthority(
              statement,
              new TreeAccessCheckContext(
                  sessionInfo.getUserId(),
                  sessionInfo.getUserName(),
                  sessionInfo.getCliHostname()));
      if (authorityStatus.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        throw new PipeException(authorityStatus.getMessage());
      }

      long queryId = SessionManager.getInstance().requestQueryId();
      ExecutionResult result =
          Coordinator.getInstance()
              .executeForTreeModel(
                  statement,
                  queryId,
                  sessionInfo,
                  "",
                  PARTITION_FETCHER,
                  SCHEMA_FETCHER,
                  IoTDBDescriptor.getInstance().getConfig().getQueryTimeoutThreshold(),
                  false,
                  statement.isDebug());
      if (result.status.code != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        LOGGER.error(DataNodePipeMessages.DELETE_ERROR_STATEMENT, deletion, statement);
        LOGGER.error(DataNodePipeMessages.DELETE_RESULT_STATUS, result.status);
        throw new LoadFileException(
            String.format(
                DataNodePipeMessages.PIPE_EXCEPTION_CAN_NOT_EXECUTE_DELETE_STATEMENT_S_3563E8A3,
                statement));
      }
    } catch (Exception e) {
      throw new PipeException(e.getMessage());
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
