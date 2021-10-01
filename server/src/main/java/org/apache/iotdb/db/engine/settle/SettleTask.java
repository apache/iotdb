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

package org.apache.iotdb.db.engine.settle;

import org.apache.iotdb.db.concurrent.WrappedRunnable;
import org.apache.iotdb.db.engine.settle.SettleLog.SettleCheckStatus;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.service.SettleService;
import org.apache.iotdb.db.tools.settle.TsFileAndModSettleTool;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.fileSystem.fsFactory.FSFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class SettleTask extends WrappedRunnable {

  private static final Logger logger = LoggerFactory.getLogger(SettleTask.class);
  private final TsFileResource resourceToBeSettled;
  private final FSFactory fsFactory = FSFactoryProducer.getFSFactory();

  public SettleTask(TsFileResource resourceToBeSettled) {
    this.resourceToBeSettled = resourceToBeSettled;
  }

  @Override
  public void runMayThrow() {
    try {
      settleTsFile();
    } catch (Exception e) {
      logger.error("meet error when settling file:{}", resourceToBeSettled.getTsFilePath(), e);
    }
  }

  public void settleTsFile() throws Exception {
    List<TsFileResource> settledResources = new ArrayList<>();
    if (!resourceToBeSettled.isClosed()) {
      logger.warn(
          "The tsFile {} should be sealed when settling.", resourceToBeSettled.getTsFilePath());
      return;
    }
    TsFileAndModSettleTool tsFileAndModSettleTool = new TsFileAndModSettleTool();
    if (tsFileAndModSettleTool.isSettledFileGenerated(resourceToBeSettled)) {
      logger.info("find settled file for {}", resourceToBeSettled.getTsFile());
      settledResources = tsFileAndModSettleTool.findSettledFile(resourceToBeSettled);
    } else {
      logger.info("generate settled file for {}", resourceToBeSettled.getTsFile());
      // Write Settle Log, Status 1
      SettleLog.writeSettleLog(
          resourceToBeSettled.getTsFilePath()
              + SettleLog.COMMA_SEPERATOR
              + SettleCheckStatus.BEGIN_SETTLE_FILE);
      tsFileAndModSettleTool.settleOneTsFileAndMod(resourceToBeSettled, settledResources);
    }

    // Write Settle Log, Status 2
    SettleLog.writeSettleLog(
        resourceToBeSettled.getTsFilePath()
            + SettleLog.COMMA_SEPERATOR
            + SettleCheckStatus.AFTER_SETTLE_FILE);
    resourceToBeSettled.getSettleTsFileCallBack().call(resourceToBeSettled, settledResources);

    // Write Settle Log, Status 3
    SettleLog.writeSettleLog(
        resourceToBeSettled.getTsFilePath()
            + SettleLog.COMMA_SEPERATOR
            + SettleCheckStatus.SETTLE_SUCCESS);
    logger.info(
        "Settle completes, file path:{} , the remaining file to be settled num: {}",
        resourceToBeSettled.getTsFilePath(),
        SettleService.getFilesToBeSettledCount().get());

    if (SettleService.getFilesToBeSettledCount().get() == 0) {
      SettleLog.closeLogWriter();
      fsFactory.getFile(SettleLog.getSettleLogPath()).delete();
      SettleService.getINSTANCE().stop();
      logger.info("All files settled successfully! ");
    }
  }
}
