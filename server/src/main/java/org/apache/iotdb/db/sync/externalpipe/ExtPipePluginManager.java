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

package org.apache.iotdb.db.sync.externalpipe;

import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.sync.datasource.PipeOpManager;
import org.apache.iotdb.db.sync.pipedata.DeletionPipeData;
import org.apache.iotdb.db.sync.pipedata.PipeData;
import org.apache.iotdb.db.sync.pipedata.TsFilePipeData;
import org.apache.iotdb.db.sync.sender.pipe.TsFilePipe;
import org.apache.iotdb.pipe.external.api.IExternalPipeSinkWriterFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class' tasks: 1) Manager all ExternalPipe. every ExternalPipe is responsible for 1 external
 * pipe plugin. 2) Manager the data flow between tsFilePipe and pipeOpManager.
 */
public class ExtPipePluginManager {
  private static final Logger logger = LoggerFactory.getLogger(ExtPipePluginManager.class);

  private TsFilePipe tsFilePipe;
  private PipeOpManager pipeOpManager;

  // externalPipeTypeName => ExtPipePlugin
  private Map<String, ExtPipePlugin> extPipePluginMap = new HashMap<>();

  private ExecutorService monitorService =
      IoTDBThreadPoolFactory.newFixedThreadPool(1, "ExtPipePluginManager-monitor");
  boolean alive = false;

  private long lastPipeDataSerialNumber = Long.MIN_VALUE;

  // Writer method name -> (exception message -> count)
  private Map<String, Map<String, AtomicInteger>> writerInvocationFailures;

  // condition-lock of commit and also record commit number
  private byte[] commitTriggerLocker = new byte[0];
  private long commitTriggerCounter = 0L;

  public ExtPipePluginManager(TsFilePipe tsFilePipe) {
    this.tsFilePipe = tsFilePipe;
    pipeOpManager = new PipeOpManager(tsFilePipe);

    pipeOpManager.setNewDataEventHandler(this::newDataEventHandler);
  }

  @TestOnly
  public ExtPipePluginManager(
      String Name,
      IExternalPipeSinkWriterFactory factory,
      ExtPipePluginConfiguration conf,
      TsFilePipe tsFilePipe) {
    this(null);
  }

  @TestOnly
  public void setPipeOpManager(PipeOpManager pipeOpManager) {
    this.pipeOpManager = pipeOpManager;
  }

  @TestOnly
  public ExtPipePluginManager setTsFilePipe(TsFilePipe tsFilePipe) {
    this.tsFilePipe = tsFilePipe;
    return this;
  }

  /**
   * Start 1 dedicated external Pipe
   *
   * @param pipeTypeName, External PIPE name that is from
   *     IExternalPipeSinkWriterFactory.getExternalPipeType()
   * @param sinkParams, input parameters in customer CMD.
   * @throws IOException
   */
  public void startExtPipe(String pipeTypeName, Map<String, String> sinkParams) throws IOException {
    logger.debug("Enter startExtPipe(), pipeTypeName={}, sinkParams={}.", pipeTypeName, sinkParams);

    ExtPipePlugin extPipePlugin =
        extPipePluginMap.computeIfAbsent(
            pipeTypeName, k -> new ExtPipePlugin(pipeTypeName, sinkParams, this, pipeOpManager));

    if (extPipePlugin.isAlive()) {
      String eMsg =
          "startExtPipe(), External Pipe "
              + pipeTypeName
              + "has been alive, can not be started again.";
      logger.error(eMsg);
      throw new IOException(eMsg);
    }

    extPipePlugin.start();

    // == Start monitor Pipe data thread
    alive = true;
    ThreadPoolExecutor tpe = ((ThreadPoolExecutor) monitorService);
    if ((tpe.getActiveCount() <= 0) && (tpe.getQueue().isEmpty())) {
      monitorService.submit(this::monitorPipeData);
    }

    logger.info("startExtPipe() finish. pipeTypeName={} ", pipeTypeName);
  }

  /**
   * Notify ExtPipePluginManager to do new commit checking.
   *
   * @param sgName
   * @param commitIndex
   * @throws IOException
   */
  public void triggerCommit(String sgName, long commitIndex) throws IOException {
    synchronized (commitTriggerLocker) {
      commitTriggerCounter++;
      commitTriggerLocker.notifyAll();
    }
  }

  /**
   * Summary all ExternalPipes' commit info, then do commit to pipeOpManager.
   *
   * @return the number of left, in-using(uncommitted) FilePipes/OpBlocks
   */
  public int checkCommitIndex() {
    Set<String> sgSet = pipeOpManager.getSgSet();
    for (String sgName : sgSet) {
      long finalCommitIndex = Long.MAX_VALUE;

      for (ExtPipePlugin extPipePlugin : extPipePluginMap.values()) {
        long commitIndex = extPipePlugin.getDataCommitIndex(sgName);
        if (commitIndex < 0) {
          continue;
        }
        if (commitIndex < finalCommitIndex) {
          finalCommitIndex = commitIndex;
        }
      }

      if ((finalCommitIndex < Long.MAX_VALUE) && (finalCommitIndex >= 0)) {
        pipeOpManager.commitData(sgName, finalCommitIndex);
      }
    }

    return pipeOpManager.getInUseOpBlockNum();
  }

  private void newDataEventHandler(String sgName, long newDataBeginIndex, long newDataCount) {
    for (ExtPipePlugin extPipePlugin : extPipePluginMap.values()) {
      extPipePlugin.notifyNewDataArrive(sgName, newDataBeginIndex, newDataCount);
    }
  }

  /**
   * This function will be run in 1 separated thread. It has 2 tasks: 1) Find tsFilePipe's new
   * Tsfile and put it into pipeOpManager. 2) Do data commit to pipeOpManager.
   */
  private void monitorPipeData() {
    Thread.currentThread()
        .setName("ExternalPipe-monitorPipeData-" + Thread.currentThread().getId());

    logger.info("monitorPipeData start. Thread={}", Thread.currentThread().getName());

    if (tsFilePipe == null) { // this step is for facilitating test.
      logger.info(
          "monitorPipeData(), Error! tsFilePipe is null. Thread exit, {}.",
          Thread.currentThread().getName());
      return;
    }

    while (alive) {
      try {
        // == pull Pipe src data and insert them to pipeOpManager
        List<PipeData> pipeDataList = tsFilePipe.pull(Long.MAX_VALUE);
        if ((pipeDataList != null)
            && (!pipeDataList.isEmpty())
            && (pipeDataList.get(pipeDataList.size() - 1).getSerialNumber()
                > lastPipeDataSerialNumber)) {
          for (PipeData pipeData : pipeDataList) {
            long pipeDataSerialNumber = pipeData.getSerialNumber();
            if (pipeDataSerialNumber <= lastPipeDataSerialNumber) {
              continue;
            }

            // == extract the Tsfile PipeData
            if (pipeData instanceof TsFilePipeData) {
              TsFilePipeData tsFilePipeData = (TsFilePipeData) pipeData;

              String sgName = tsFilePipeData.getDatabase();
              String tsFileFullName = tsFilePipeData.getTsFilePath();
              String modsFileFullName = tsFilePipeData.getModsFilePath();
              try {
                pipeOpManager.appendTsFileOpBlock(
                    sgName, tsFileFullName, modsFileFullName, pipeDataSerialNumber);
                lastPipeDataSerialNumber = pipeDataSerialNumber;
              } catch (IOException e) {
                logger.error("monitorPipeData(), Can not append TsFile: {}", tsFileFullName);
              }
              continue;
            } else if (pipeData instanceof DeletionPipeData) {
              // == handle delete PipeData
              if (pipeOpManager.isEmpty()) {
                DeletionPipeData deletionPipeData = (DeletionPipeData) pipeData;
                pipeOpManager.appendDeletionOpBlock(
                    deletionPipeData.getDatabase(),
                    deletionPipeData.getDeletion(),
                    pipeDataSerialNumber);
                lastPipeDataSerialNumber = pipeData.getSerialNumber();
              }
              break;
            }
          }
        }

        synchronized (commitTriggerLocker) {
          if (commitTriggerCounter <= 0L) {
            try {
              commitTriggerLocker.wait(2000); // 2 seconds
            } catch (InterruptedException ignored) {
            }
          }
          commitTriggerCounter = 0L;
        }
        checkCommitIndex();

      } catch (Throwable t) {
        logger.error("monitorPipeData() Exception: ", t);
      }
    }

    logger.info("monitorPipeData() exits. Thread={}", Thread.currentThread().getName());
  }

  /**
   * Stop dedicated External Pipe
   *
   * @param extPipeTypeName
   */
  public void stopExtPipe(String extPipeTypeName) {
    logger.info("ExtPipePluginManager stop({}).", extPipeTypeName);

    ExtPipePlugin extPipePlugin = extPipePluginMap.get(extPipeTypeName);
    if (extPipePlugin == null) {
      logger.error("ExtPipePluginManager stop(), invalid extPipeTypeName={}", extPipeTypeName);
      return;
    }

    extPipePlugin.stop();
  }

  private void stopAllThreadPool() {
    alive = false;
    monitorService.shutdown();

    boolean isTerminated = false;
    try {
      isTerminated = monitorService.awaitTermination(2, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      logger.error("stopAllThreadPool(), Interrupted when terminating monitorService, ", e);
    } finally {
      if (!isTerminated) {
        logger.warn(
            "stopAllThreadPool(), for monitorService. Graceful shutdown timed out, so force shutdown.");
        monitorService.shutdownNow();
      }
    }
  }

  /**
   * Close dedicated External Pipe
   *
   * @param pipeTypeName
   */
  public void dropExtPipe(String pipeTypeName) {
    logger.info("ExtPipePluginManager drop {}.", pipeTypeName);

    ExtPipePlugin extPipePlugin = extPipePluginMap.get(pipeTypeName);
    if (extPipePlugin == null) {
      logger.error("ExtPipePluginManager dropExtPipe(), invalid pipeTypeName={}", pipeTypeName);
      return;
    }

    if (extPipePlugin.isAlive()) {
      extPipePlugin.stop();
    }
    extPipePluginMap.remove(pipeTypeName);

    if (extPipePluginMap.size() <= 0) {
      stopAllThreadPool();

      if (pipeOpManager != null) {
        pipeOpManager.close();
        pipeOpManager = null;
      }
    }
  }

  @TestOnly
  public PipeOpManager getPipeOpManager() {
    return pipeOpManager;
  }

  public ExternalPipeStatus getExternalPipeStatus(String extPipeTypeName) {
    ExtPipePlugin extPipePlugin = extPipePluginMap.get(extPipeTypeName);
    if (extPipePlugin == null) {
      return null;
    }

    return extPipePlugin.getStatus();
  }
}
