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

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.queryengine.common.SessionInfo;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.load.LoadFileException;
import org.apache.iotdb.db.i18n.DataNodePipeMessages;
import org.apache.iotdb.db.protocol.session.SessionManager;
import org.apache.iotdb.db.queryengine.plan.Coordinator;
import org.apache.iotdb.db.queryengine.plan.execution.ExecutionResult;
import org.apache.iotdb.db.queryengine.plan.statement.crud.LoadTsFileStatement;
import org.apache.iotdb.pipe.api.exception.PipeException;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/** This loader is used to load tsFiles. If .mods file exists, it will be loaded as well. */
public class TsFileLoader implements ILoader {

  private static final Logger LOGGER = LoggerFactory.getLogger(TsFileLoader.class);

  private final File tsFile;
  private final String database;

  public TsFileLoader(File tsFile, String database) {
    this.tsFile = tsFile;
    this.database = database;
  }

  @Override
  public void load(final SessionInfo sessionInfo) {
    try {
      LoadTsFileStatement statement = LoadTsFileStatement.createUnchecked(tsFile.getAbsolutePath());
      statement.setDeleteAfterLoad(true);
      statement.setConvertOnTypeMismatch(true);
      statement.setDatabaseLevel(parseSgLevel());
      statement.setVerifySchema(true);
      statement.setAutoCreateDatabase(
          IoTDBDescriptor.getInstance().getConfig().isAutoCreateSchemaEnabled());

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
        LOGGER.error(DataNodePipeMessages.LOAD_TSFILE_ERROR_STATEMENT, tsFile.getPath(), statement);
        LOGGER.error(DataNodePipeMessages.LOAD_TSFILE_RESULT_STATUS, result.status);
        throw new LoadFileException(
            String.format(
                DataNodePipeMessages
                    .PIPE_EXCEPTION_CAN_NOT_EXECUTE_LOAD_TSFILE_STATEMENT_S_8CC1A096,
                statement));
      }
    } catch (Exception e) {
      throw new PipeException(e.getMessage());
    }
  }

  private int parseSgLevel() throws IllegalPathException {
    return new PartialPath(database).getNodeLength() - 1;
  }
}
