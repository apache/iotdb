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

import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.sync.datasource.PipeOpManager;
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
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
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

  private ExecutorService monitorService = Executors.newFixedThreadPool(1);

  private long lastPipeDataSerialNumber = Long.MIN_VALUE;

  // Writer method name -> (exception message -> count)
  private Map<String, Map<String, AtomicInteger>> writerInvocationFailures;
  private final int timestampDivisor;

  public ExtPipePluginManager(TsFilePipe tsFilePipe) {
    this.tsFilePipe = tsFilePipe;

    String timePrecision = IoTDBDescriptor.getInstance().getConfig().getTimestampPrecision();
    switch (timePrecision) {
      case "ms":
        timestampDivisor = 1;
        break;
      case "us":
        timestampDivisor = 1_000;
        break;
      case "ns":
        timestampDivisor = 1_000_000;
        break;
      default:
        throw new IllegalArgumentException("Unrecognized time precision: " + timePrecision);
    }

    pipeOpManager = new PipeOpManager(tsFilePipe);
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
            pipeTypeName, k -> new ExtPipePlugin(pipeTypeName, sinkParams, pipeOpManager));

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
    ThreadPoolExecutor tpe = ((ThreadPoolExecutor) monitorService);
    if ((tpe.getActiveCount() <= 0) && (tpe.getQueue().size() <= 0)) {
      monitorService.submit(this::monitorPipeData);
    }

    logger.info("startExtPipe() finish. pipeTypeName={} ", pipeTypeName);
  }

  /** Summary all ExternalPipes' commit info, then do commit to pipeOpManager. */
  public void checkCommitIndex() {
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

    while (true) {
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
          lastPipeDataSerialNumber = pipeData.getSerialNumber();

          // extract the Tsfile PipeData
          if (pipeData instanceof TsFilePipeData) {
            TsFilePipeData tsFilePipeData = (TsFilePipeData) pipeData;

            String sgName = tsFilePipeData.getStorageGroupName();
            String tsFileFullName = tsFilePipeData.getTsFilePath();
            String modsFileFullName = tsFilePipeData.getModsFilePath();
            try {
              pipeOpManager.appendTsFile(
                  sgName, tsFileFullName, modsFileFullName, pipeDataSerialNumber);
            } catch (IOException e) {
              logger.error("monitorPipeData(), Can not append TsFile: {}" + tsFileFullName);
            }
          }
        }
      }

      checkCommitIndex();

      try {
        Thread.sleep(2_000); // 2 seconds
      } catch (InterruptedException e) {
        break;
      }
    }

    logger.info("monitorPipeData exits. Thread={}", Thread.currentThread().getName());
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
