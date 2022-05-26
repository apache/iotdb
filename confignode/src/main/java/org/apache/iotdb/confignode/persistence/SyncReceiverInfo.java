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
package org.apache.iotdb.confignode.persistence;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.commons.sync.SyncPathUtil;
import org.apache.iotdb.confignode.consensus.request.read.ShowPipeReq;
import org.apache.iotdb.confignode.consensus.request.write.OperatePipeReq;
import org.apache.iotdb.confignode.consensus.response.PipeInfoResp;
import org.apache.iotdb.db.sync.receiver.manager.PipeInfo;
import org.apache.iotdb.db.sync.receiver.manager.PipeMessage;
import org.apache.iotdb.db.sync.receiver.manager.ReceiverManager;
import org.apache.iotdb.db.sync.receiver.recovery.ReceiverLog;
import org.apache.iotdb.db.sync.receiver.recovery.ReceiverLogAnalyzer;
import org.apache.iotdb.db.sync.sender.pipe.Pipe.PipeStatus;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SyncReceiverInfo {

  private static final Logger LOGGER = LoggerFactory.getLogger(ReceiverManager.class);

  private boolean pipeServerEnable;
  // <pipeName, <remoteIp, <createTime, status>>>
  private Map<String, Map<String, Map<Long, PipeStatus>>> pipeInfos;
  // <pipeFolderName, pipeMsg>
  private Map<String, List<PipeMessage>> pipeMessageMap;
  private ReceiverLog log;

  public SyncReceiverInfo() {
    log = new ReceiverLog();
    ReceiverLogAnalyzer analyzer = new ReceiverLogAnalyzer();
    try {
      analyzer.scan();
      pipeInfos = analyzer.getPipeInfos();
      pipeServerEnable = analyzer.isPipeServerEnable();
      pipeMessageMap = analyzer.getPipeMessageMap();
    } catch (StartupException e) {
      e.printStackTrace();
      pipeInfos = new ConcurrentHashMap<>();
      pipeMessageMap = new ConcurrentHashMap<>();
      pipeServerEnable = false;
    }
  }

  public TSStatus operatePipe(OperatePipeReq req) {
    try {
      switch (req.getOperateType()) {
        case HEARTBEAT:
          PipeMessage message =
              getPipeMessage(req.getPipeName(), req.getRemoteIp(), req.getCreateTime(), true);
          switch (message.getType()) {
            case INFO:
              break;
            case WARN:
              return new TSStatus(TSStatusCode.SYNC_RECEIVER_WARN.getStatusCode());
            case ERROR:
              return new TSStatus(TSStatusCode.SYNC_RECEIVER_ERROR.getStatusCode());
            default:
              throw new UnsupportedOperationException("Wrong message type " + message.getType());
          }
          break;
        case CREATE:
          createPipe(req.getPipeName(), req.getRemoteIp(), req.getCreateTime());
          break;
        case START:
          startPipe(req.getPipeName(), req.getRemoteIp(), req.getCreateTime());
          break;
        case STOP:
          stopPipe(req.getPipeName(), req.getRemoteIp(), req.getCreateTime());
          break;
        case DROP:
          dropPipe(req.getPipeName(), req.getRemoteIp(), req.getCreateTime());
          break;
      }
    } catch (IOException e) {
      return new TSStatus(TSStatusCode.PERSISTENCE_FAILURE.getStatusCode());
    }
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  public void close() throws IOException {
    log.close();
  }

  public void startServer() throws IOException {
    log.startPipeServer();
    pipeServerEnable = true;
  }

  public void stopServer() throws IOException {
    log.stopPipeServer();
    pipeServerEnable = false;
  }

  private void createPipe(String pipeName, String remoteIp, long createTime) throws IOException {
    PipeInfo pipeInfo = getPipeInfo(pipeName, remoteIp, createTime);
    if (pipeInfo == null || pipeInfo.getStatus().equals(PipeStatus.DROP)) {
      LOGGER.info(
          "create Pipe name={}, remoteIp={}, createTime={}", pipeName, remoteIp, createTime);
      //            createDir(pipeName, remoteIp, createTime);
      log.createPipe(pipeName, remoteIp, createTime);
      pipeInfos.putIfAbsent(pipeName, new HashMap<>());
      pipeInfos.get(pipeName).putIfAbsent(remoteIp, new HashMap<>());
      pipeInfos.get(pipeName).get(remoteIp).put(createTime, PipeStatus.STOP);
    }
  }

  private void startPipe(String pipeName, String remoteIp, long createTime) throws IOException {
    PipeInfo pipeInfo = getPipeInfo(pipeName, remoteIp, createTime);
    if (pipeInfo != null && pipeInfo.getStatus().equals(PipeStatus.STOP)) {
      LOGGER.info("start Pipe name={}, remoteIp={}, createTime={}", pipeName, remoteIp, createTime);
      log.startPipe(pipeName, remoteIp, createTime);
      pipeInfos.get(pipeName).get(remoteIp).put(createTime, PipeStatus.RUNNING);
      //            collector.startPipe(pipeName, remoteIp, createTime);
    }
  }

  private void stopPipe(String pipeName, String remoteIp, long createTime) throws IOException {
    PipeInfo pipeInfo = getPipeInfo(pipeName, remoteIp, createTime);
    if (pipeInfo != null && pipeInfo.getStatus().equals(PipeStatus.RUNNING)) {
      LOGGER.info("stop Pipe name={}, remoteIp={}, createTime={}", pipeName, remoteIp, createTime);
      log.stopPipe(pipeName, remoteIp, createTime);
      pipeInfos.get(pipeName).get(remoteIp).put(createTime, PipeStatus.STOP);
      //            collector.stopPipe(pipeName, remoteIp, createTime);
    }
  }

  private void dropPipe(String pipeName, String remoteIp, long createTime) throws IOException {
    PipeInfo pipeInfo = getPipeInfo(pipeName, remoteIp, createTime);
    if (pipeInfo != null && !pipeInfo.getStatus().equals(PipeStatus.DROP)) {
      LOGGER.info("drop Pipe name={}, remoteIp={}, createTime={}", pipeName, remoteIp, createTime);
      log.dropPipe(pipeName, remoteIp, createTime);
      pipeInfos.get(pipeName).get(remoteIp).put(createTime, PipeStatus.DROP);
      //            collector.stopPipe(pipeName, remoteIp, createTime);
      //            PipeDataQueueFactory.removeBufferedPipeDataQueue(
      //                    SyncPathUtil.getReceiverPipeLogDir(pipeName, remoteIp, createTime));
      File dir = new File(SyncPathUtil.getReceiverPipeDir(pipeName, remoteIp, createTime));
      FileUtils.deleteDirectory(dir);
    }
  }

  public List<PipeInfo> getPipeInfosByPipeName(String pipeName) {
    if (!pipeInfos.containsKey(pipeName)) {
      return Collections.emptyList();
    }
    List<PipeInfo> res = new ArrayList<>();
    for (Map.Entry<String, Map<Long, PipeStatus>> remoteIpEntry :
        pipeInfos.get(pipeName).entrySet()) {
      for (Map.Entry<Long, PipeStatus> createTimeEntry : remoteIpEntry.getValue().entrySet()) {
        res.add(
            new PipeInfo(
                pipeName,
                remoteIpEntry.getKey(),
                createTimeEntry.getValue(),
                createTimeEntry.getKey()));
      }
    }
    return res;
  }

  public PipeInfo getPipeInfo(String pipeName, String remoteIp, long createTime) {
    if (pipeInfos.containsKey(pipeName)
        && pipeInfos.get(pipeName).containsKey(remoteIp)
        && pipeInfos.get(pipeName).get(remoteIp).containsKey(createTime)) {
      return new PipeInfo(
          pipeName, remoteIp, pipeInfos.get(pipeName).get(remoteIp).get(createTime), createTime);
    }
    return null;
  }

  public List<PipeInfo> getAllPipeInfos() {
    List<PipeInfo> res = new ArrayList<>();
    for (String pipeName : pipeInfos.keySet()) {
      res.addAll(getPipeInfosByPipeName(pipeName));
    }
    return res;
  }

  //    private void createDir(String pipeName, String remoteIp, long createTime) {
  //        File f = new File(SyncPathUtil.getReceiverFileDataDir(pipeName, remoteIp, createTime));
  //        if (!f.exists()) {
  //            f.mkdirs();
  //        }
  //        f = new File(SyncPathUtil.getReceiverPipeLogDir(pipeName, remoteIp, createTime));
  //        if (!f.exists()) {
  //            f.mkdirs();
  //        }
  //    }

  /**
   * write a single message and serialize to disk
   *
   * @param pipeName name of pipe
   * @param remoteIp remoteIp of pipe
   * @param createTime createTime of pipe
   * @param message pipe message
   */
  public synchronized void writePipeMessage(
      String pipeName, String remoteIp, long createTime, PipeMessage message) {
    String pipeIdentifier = SyncPathUtil.getReceiverPipeDirName(pipeName, remoteIp, createTime);
    try {
      log.writePipeMsg(pipeIdentifier, message);
    } catch (IOException e) {
      LOGGER.error(
          "Can not write pipe message {} from {} to disk because {}",
          message,
          pipeIdentifier,
          e.getMessage());
    }
    pipeMessageMap.computeIfAbsent(pipeIdentifier, i -> new ArrayList<>()).add(message);
  }

  /**
   * read recent messages about one pipe
   *
   * @param pipeName name of pipe
   * @param remoteIp remoteIp of pipe
   * @param createTime createTime of pipe
   * @param consume if consume is true, these messages will not be deleted. Otherwise, these
   *     messages can be read next time.
   * @return recent messages
   */
  public synchronized List<PipeMessage> getPipeMessages(
      String pipeName, String remoteIp, long createTime, boolean consume) {
    List<PipeMessage> pipeMessageList = new ArrayList<>();
    String pipeIdentifier = SyncPathUtil.getReceiverPipeDirName(pipeName, remoteIp, createTime);
    if (consume) {
      try {
        log.comsumePipeMsg(pipeIdentifier);
      } catch (IOException e) {
        LOGGER.error(
            "Can not read pipe message about {} from disk because {}",
            pipeIdentifier,
            e.getMessage());
      }
    }
    if (pipeMessageMap.containsKey(pipeIdentifier)) {
      pipeMessageList = pipeMessageMap.get(pipeIdentifier);
      if (consume) {
        pipeMessageMap.remove(pipeIdentifier);
      }
    }
    return pipeMessageList;
  }

  /**
   * read the most important message about one pipe. ERROR > WARN > INFO.
   *
   * @param pipeName name of pipe
   * @param remoteIp remoteIp of pipe
   * @param createTime createTime of pipe
   * @param consume if consume is true, recent messages will not be deleted. Otherwise, these
   *     messages can be read next time.
   * @return the most important message
   */
  public PipeMessage getPipeMessage(
      String pipeName, String remoteIp, long createTime, boolean consume) {
    List<PipeMessage> pipeMessageList = getPipeMessages(pipeName, remoteIp, createTime, consume);
    PipeMessage message = new PipeMessage(PipeMessage.MsgType.INFO, "");
    if (!pipeMessageList.isEmpty()) {
      for (PipeMessage pipeMessage : pipeMessageList) {
        if (pipeMessage.getType().getValue() > message.getType().getValue()) {
          message = pipeMessage;
        }
      }
    }
    return message;
  }

  public boolean isPipeServerEnable() {
    return pipeServerEnable;
  }

  public void setPipeServerEnable(boolean pipeServerEnable) {
    this.pipeServerEnable = pipeServerEnable;
  }

  public PipeInfoResp showPipe(ShowPipeReq req) {
    PipeInfoResp pipeInfoResp = new PipeInfoResp();
    if (StringUtils.isEmpty(req.getPipeName())) {
      pipeInfoResp.setPipeInfoList(getAllPipeInfos());
    } else {
      pipeInfoResp.setPipeInfoList(getPipeInfosByPipeName(req.getPipeName()));
    }
    pipeInfoResp.setStatus(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));
    return pipeInfoResp;
  }
}
