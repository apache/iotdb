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
package org.apache.iotdb.db.sync.receiver.manager;

import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.db.sync.conf.SyncPathUtil;
import org.apache.iotdb.db.sync.receiver.recovery.ReceiverLog;
import org.apache.iotdb.db.sync.receiver.recovery.ReceiverLogAnalyzer;
import org.apache.iotdb.db.sync.sender.pipe.Pipe.PipeStatus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ReceiverManager {

  private static final Logger logger = LoggerFactory.getLogger(ReceiverManager.class);

  private boolean pipeServerEnable;
  // <pipeName, <remoteIp, <createTime, status>>>
  private Map<String, Map<String, Map<Long, PipeStatus>>> pipeInfos;
  // <pipeFolderName, pipeMsg>
  private Map<String, List<PipeMessage>> pipeMessageMap;
  private ReceiverLog log;

  public void init() throws StartupException {
    log = new ReceiverLog();
    ReceiverLogAnalyzer analyzer = new ReceiverLogAnalyzer();
    analyzer.scan();
    pipeInfos = analyzer.getPipeInfos();
    pipeServerEnable = analyzer.isPipeServerEnable();
    pipeMessageMap = analyzer.getPipeMessageMap();
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

  public void createPipe(String pipeName, String remoteIp, long createTime) throws IOException {
    log.createPipe(pipeName, remoteIp, createTime);
    pipeInfos.putIfAbsent(pipeName, new HashMap<>());
    pipeInfos.get(pipeName).putIfAbsent(remoteIp, new HashMap<>());
    pipeInfos.get(pipeName).get(remoteIp).put(createTime, PipeStatus.STOP);
  }

  public void startPipe(String pipeName, String remoteIp, long createTime) throws IOException {
    log.startPipe(pipeName, remoteIp, createTime);
    pipeInfos.get(pipeName).get(remoteIp).put(createTime, PipeStatus.RUNNING);
  }

  public void stopPipe(String pipeName, String remoteIp, long createTime) throws IOException {
    log.stopPipe(pipeName, remoteIp, createTime);
    pipeInfos.get(pipeName).get(remoteIp).put(createTime, PipeStatus.STOP);
  }

  public void dropPipe(String pipeName, String remoteIp, long createTime) throws IOException {
    log.dropPipe(pipeName, remoteIp, createTime);
    pipeInfos.get(pipeName).get(remoteIp).put(createTime, PipeStatus.DROP);
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
      logger.error(
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
        logger.error(
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

  public static ReceiverManager getInstance() {
    return ReceiverManagerHolder.INSTANCE;
  }

  private ReceiverManager() {}

  private static class ReceiverManagerHolder {
    private static final ReceiverManager INSTANCE = new ReceiverManager();

    private ReceiverManagerHolder() {}
  }
}
