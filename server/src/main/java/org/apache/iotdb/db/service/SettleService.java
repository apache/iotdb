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

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.settle.SettleLog;
import org.apache.iotdb.db.engine.settle.SettleTask;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.WriteProcessException;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.tools.settle.TsFileAndModSettleTool;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class SettleService implements IService {
  private static final Logger logger = LoggerFactory.getLogger(SettleService.class);

  private AtomicInteger threadCnt = new AtomicInteger();
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
          Executors.newFixedThreadPool(
              settleThreadNum, r -> new Thread(r, "SettleThread-" + threadCnt.getAndIncrement()));
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
      while (!StorageEngine.getInstance().isAllSgReady()) {
        // wait for all sg ready
      }
      List<TsFileResource> seqResourcesToBeSettled = new ArrayList<>();
      List<TsFileResource> unseqResourcesToBeSettled = new ArrayList<>();
      for (Map.Entry<PartialPath, List<String>> entry : tmpSgResourcesMap.entrySet()) {
        try {
          StorageEngine.getInstance()
              .getResourcesToBeSettled(
                  entry.getKey(),
                  seqResourcesToBeSettled,
                  unseqResourcesToBeSettled,
                  entry.getValue());
        } catch (StorageEngineException e) {
          e.printStackTrace();
        } finally {
          StorageEngine.getInstance().setSettling(entry.getKey(), false);
        }
      }
      startSettling(seqResourcesToBeSettled, unseqResourcesToBeSettled);
      setRecoverFinish(true);
    } catch (WriteProcessException e) {
      e.printStackTrace();
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
        "Totally find "
            + (seqResourcesToBeSettled.size() + unseqResourcesToBeSettled.size())
            + " tsFiles to be settled.");
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
