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

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.sync.PipeDataLoadException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.LoadFileException;
import org.apache.iotdb.db.mpp.plan.Coordinator;
import org.apache.iotdb.db.mpp.plan.execution.ExecutionResult;
import org.apache.iotdb.db.mpp.plan.statement.crud.LoadTsFileStatement;
import org.apache.iotdb.db.query.control.SessionManager;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/** This loader is used to load tsFiles. If .mods file exists, it will be loaded as well. */
public class TsFileLoader implements ILoader {
  private static final Logger logger = LoggerFactory.getLogger(TsFileLoader.class);

  private final File tsFile;
  private final String database;

  public TsFileLoader(File tsFile, String database) {
    this.tsFile = tsFile;
    this.database = database;
  }

  @Override
  public void load() throws PipeDataLoadException {
    try {

      LoadTsFileStatement statement = new LoadTsFileStatement(tsFile.getAbsolutePath());
      statement.setDeleteAfterLoad(true);
      statement.setSgLevel(parseSgLevel());
      statement.setVerifySchema(true);
      statement.setAutoCreateDatabase(false);

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
        logger.error("Load TsFile {} error, statement: {}.", tsFile.getPath(), statement);
        logger.error("Load TsFile result status : {}.", result.status);
        throw new LoadFileException(
            String.format("Can not execute load TsFile statement: %s", statement));
      }
    } catch (Exception e) {
      throw new PipeDataLoadException(e.getMessage());
    }
  }

  private int parseSgLevel() throws IllegalPathException {
    return new PartialPath(database).getNodeLength() - 1;
  }
}
