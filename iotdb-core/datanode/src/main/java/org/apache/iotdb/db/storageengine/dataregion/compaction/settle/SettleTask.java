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

package org.apache.iotdb.db.storageengine.dataregion.compaction.settle;

import org.apache.iotdb.commons.concurrent.WrappedRunnable;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.db.exception.WriteProcessException;
import org.apache.iotdb.db.i18n.StorageEngineMessages;
import org.apache.iotdb.db.service.SettleService;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.tools.settle.TsFileAndModSettleTool;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SettleTask extends WrappedRunnable {
  private static final Logger logger = LoggerFactory.getLogger(SettleTask.class);
  private final TsFileResource resourceToBeSettled;

  public SettleTask(TsFileResource resourceToBeSettled) {
    this.resourceToBeSettled = resourceToBeSettled;
  }

  @Override
  public void runMayThrow() {
    try {
      settleTsFile();
    } catch (Exception e) {
      logger.error(
          StorageEngineMessages.STORAGE_LOG_MEET_ERROR_WHEN_SETTLING_FILE_CBA0F9D7,
          resourceToBeSettled.getTsFile().getAbsolutePath(),
          e);
    }
  }

  public void settleTsFile() throws WriteProcessException {
    List<TsFileResource> settledResources = new ArrayList<>();
    if (!resourceToBeSettled.isClosed()) {
      logger.warn(
          StorageEngineMessages.STORAGE_LOG_THE_TSFILE_SHOULD_BE_SEALED_WHEN_SETTLING_8DBD716A,
          resourceToBeSettled.getTsFile().getAbsolutePath());
      return;
    }
    TsFileAndModSettleTool tsFileAndModSettleTool = TsFileAndModSettleTool.getInstance();
    try {
      if (tsFileAndModSettleTool.isSettledFileGenerated(resourceToBeSettled)) {
        logger.info(StorageEngineMessages.FIND_SETTLED_FILE, resourceToBeSettled.getTsFile());
        settledResources = tsFileAndModSettleTool.findSettledFile(resourceToBeSettled);
      } else {
        logger.info(StorageEngineMessages.GENERATE_SETTLED_FILE, resourceToBeSettled.getTsFile());
        // Write Settle Log, Status 1
        SettleLog.writeSettleLog(
            resourceToBeSettled.getTsFile().getAbsolutePath()
                + SettleLog.COMMA_SEPERATOR
                + SettleLog.SettleCheckStatus.BEGIN_SETTLE_FILE);
        tsFileAndModSettleTool.settleOneTsFileAndMod(resourceToBeSettled, settledResources);
        // Write Settle Log, Status 2
        SettleLog.writeSettleLog(
            resourceToBeSettled.getTsFile().getAbsolutePath()
                + SettleLog.COMMA_SEPERATOR
                + SettleLog.SettleCheckStatus.AFTER_SETTLE_FILE);
      }
    } catch (IllegalPathException
        | IOException
        | org.apache.tsfile.exception.write.WriteProcessException e) {
      resourceToBeSettled.readUnlock();
      throw new WriteProcessException(
          String.format(
              StorageEngineMessages
                  .STORAGE_EXCEPTION_EXCEPTION_TO_PARSE_THE_TSFILE_S_IN_SETTLING_D40564AD,
              resourceToBeSettled.getTsFile().getAbsolutePath()),
          e);
    }
    resourceToBeSettled.getSettleTsFileCallBack().call(resourceToBeSettled, settledResources);

    // Write Settle Log, Status 3
    SettleLog.writeSettleLog(
        resourceToBeSettled.getTsFile().getAbsolutePath()
            + SettleLog.COMMA_SEPERATOR
            + SettleLog.SettleCheckStatus.SETTLE_SUCCESS);
    logger.info(
        StorageEngineMessages
            .STORAGE_LOG_SETTLE_COMPLETES_FILE_PATH_THE_REMAINING_FILE_TO_BE_SETTLED_32DF95A7,
        resourceToBeSettled.getTsFile().getAbsolutePath(),
        SettleService.getINSTANCE().getFilesToBeSettledCount().get());

    if (SettleService.getINSTANCE().getFilesToBeSettledCount().get() == 0) {
      SettleLog.closeLogWriter();
      SettleService.getINSTANCE().stop();
      logger.info(StorageEngineMessages.ALL_FILES_SETTLED_SUCCESSFULLY);
    }
  }
}
