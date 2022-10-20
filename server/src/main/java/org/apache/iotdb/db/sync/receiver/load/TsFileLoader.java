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
package org.apache.iotdb.db.sync.receiver.load;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.sync.PipeDataLoadException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.LoadFileException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.mpp.plan.Coordinator;
import org.apache.iotdb.db.mpp.plan.execution.ExecutionResult;
import org.apache.iotdb.db.mpp.plan.statement.crud.LoadTsFileStatement;
import org.apache.iotdb.db.qp.executor.PlanExecutor;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.sys.OperateFilePlan;
import org.apache.iotdb.db.query.control.SessionManager;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/** This loader is used to load tsFiles. If .mods file exists, it will be loaded as well. */
public class TsFileLoader implements ILoader {
  private static final Logger logger = LoggerFactory.getLogger(TsFileLoader.class);
  private static PlanExecutor planExecutor;

  static {
    try {
      planExecutor = new PlanExecutor();
    } catch (QueryProcessException e) {
      logger.error(e.getMessage());
    }
  }

  private final File tsFile;
  // TODO(sync): use storage group to support auto create schema
  private final String storageGroup;

  public TsFileLoader(File tsFile, String storageGroup) {
    this.tsFile = tsFile;
    this.storageGroup = storageGroup;
  }

  @Override
  public void load() throws PipeDataLoadException {
    try {
      if (!config.isMppMode()) {
        PhysicalPlan plan =
            new OperateFilePlan(
                tsFile,
                Operator.OperatorType.LOAD_FILES,
                true,
                IoTDBDescriptor.getInstance().getConfig().getDefaultStorageGroupLevel(),
                true,
                true);
        planExecutor.processNonQuery(plan);
        return;
      }

      LoadTsFileStatement statement = new LoadTsFileStatement(tsFile.getAbsolutePath());
      statement.setDeleteAfterLoad(true);
      statement.setSgLevel(parseSgLevel());
      statement.setVerifySchema(true);
      statement.setAutoCreateSchema(true);

      long queryId = SessionManager.getInstance().requestQueryId(false);
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
        logger.error(
            String.format("Load TsFile %s error, statement: %s.", tsFile.getPath(), statement));
        logger.error(String.format("Load TsFile result status : %s.", result.status));
        throw new LoadFileException(
            String.format("Can not execute load TsFile statement: %s", statement));
      }
    } catch (Exception e) {
      throw new PipeDataLoadException(e.getMessage());
    }
  }

  private int parseSgLevel() throws IllegalPathException {
    return new PartialPath(storageGroup).getNodeLength() - 1;
  }
}
