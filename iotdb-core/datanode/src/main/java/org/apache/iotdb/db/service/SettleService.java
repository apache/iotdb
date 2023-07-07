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

package org.apache.iotdb.db.service;

import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.service.IService;
import org.apache.iotdb.commons.service.ServiceType;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.WriteProcessException;
import org.apache.iotdb.db.storageengine.dataregion.compaction.settle.SettleLog;
import org.apache.iotdb.db.storageengine.dataregion.compaction.settle.SettleTask;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.tools.settle.TsFileAndModSettleTool;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

public class SettleService implements IService {
  private static final Logger logger = LoggerFactory.getLogger(SettleService.class);

  private ExecutorService settleThreadPool;
  private boolean isRecoverFinish;

  private static AtomicInteger filesToBeSettledCount = new AtomicInteger();

  public static SettleService getINSTANCE() {
    return InstanceHolder.INSTANCE;
  }

  public static class InstanceHolder {
    private static final SettleService INSTANCE = new SettleService();

    private InstanceHolder() {}
  }

  @Override
  public void start() {
    if (settleThreadPool == null) {
      int settleThreadNum = IoTDBDescriptor.getInstance().getConfig().getSettleThreadNum();
      settleThreadPool =
          IoTDBThreadPoolFactory.newFixedThreadPool(settleThreadNum, ThreadName.SETTLE.getName());
    }
    TsFileAndModSettleTool.findFilesToBeRecovered();

    /* Classify the file paths by the SG, and then call the methods of StorageGroupProcessor of each
    SG in turn to get the TsFileResources.*/
    Map<PartialPath, List<String>> tmpSgResourcesMap = new HashMap<>(); // sgPath -> tsFilePaths
    try {
      for (String filePath : TsFileAndModSettleTool.getInstance().recoverSettleFileMap.keySet()) {
        PartialPath sgPath = getSGByFilePath(filePath);
        if (tmpSgResourcesMap.containsKey(sgPath)) {
          List<String> filePaths = tmpSgResourcesMap.get(sgPath);
          filePaths.add(filePath);
          tmpSgResourcesMap.put(sgPath, filePaths);
        } else {
          List<String> tsFilePaths = new ArrayList<>();
          tsFilePaths.add(filePath);
          tmpSgResourcesMap.put(sgPath, tsFilePaths);
        }
      }

      List<TsFileResource> seqResourcesToBeSettled = new ArrayList<>();
      List<TsFileResource> unseqResourcesToBeSettled = new ArrayList<>();
      startSettling(seqResourcesToBeSettled, unseqResourcesToBeSettled);
      setRecoverFinish(true);
    } catch (WriteProcessException e) {
      logger.error("Start error", e);
    }
  }

  public void startSettling(
      List<TsFileResource> seqResourcesToBeSettled, List<TsFileResource> unseqResourcesToBeSettled)
      throws WriteProcessException {
    filesToBeSettledCount.addAndGet(
        seqResourcesToBeSettled.size() + unseqResourcesToBeSettled.size());
    if (!SettleLog.createSettleLog() || filesToBeSettledCount.get() == 0) {
      stop();
      return;
    }
    logger.info(
        "Totally find {} tsFiles to be settled.",
        seqResourcesToBeSettled.size() + unseqResourcesToBeSettled.size());
    // settle seqTsFile
    for (TsFileResource resource : seqResourcesToBeSettled) {
      resource.readLock();
      resource.setSeq(true);
      submitSettleTask(new SettleTask(resource));
    }
    // settle unseqTsFile
    for (TsFileResource resource : unseqResourcesToBeSettled) {
      resource.readLock();
      resource.setSeq(false);
      submitSettleTask(new SettleTask(resource));
    }
  }

  @Override
  public void stop() {
    SettleLog.closeLogWriter();
    TsFileAndModSettleTool.clearRecoverSettleFileMap();
    filesToBeSettledCount.set(0);
    if (settleThreadPool != null) {
      settleThreadPool.shutdownNow();
      logger.info("Waiting for settle task pool to shut down");
      settleThreadPool = null;
      logger.info("Settle service stopped");
    }
  }

  @Override
  public ServiceType getID() {
    return ServiceType.SETTLE_SERVICE;
  }

  public AtomicInteger getFilesToBeSettledCount() {
    return filesToBeSettledCount;
  }

  public PartialPath getSGByFilePath(String tsFilePath) throws WriteProcessException {
    PartialPath sgPath = null;
    try {
      sgPath =
          new PartialPath(
              new File(tsFilePath).getParentFile().getParentFile().getParentFile().getName());
    } catch (IllegalPathException e) {
      throw new WriteProcessException(
          "Fail to get sg of this tsFile while parsing the file path.", e);
    }
    return sgPath;
  }

  private void submitSettleTask(SettleTask settleTask) throws WriteProcessException {
    // settleThreadPool.submit(settleTask);
    settleTask.settleTsFile();
  }

  public boolean isRecoverFinish() {
    return isRecoverFinish;
  }

  public void setRecoverFinish(boolean recoverFinish) {
    isRecoverFinish = recoverFinish;
  }
}
