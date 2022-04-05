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
package org.apache.iotdb.db.newsync.receiver.manager;

import org.apache.iotdb.db.exception.StartupException;
import org.apache.iotdb.db.newsync.conf.SyncPathUtil;
import org.apache.iotdb.db.newsync.receiver.recovery.ReceiverLog;
import org.apache.iotdb.db.newsync.receiver.recovery.ReceiverLogAnalyzer;
import org.apache.iotdb.db.newsync.sender.pipe.Pipe.PipeStatus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

public class ReceiverManager {

  private static final Logger logger = LoggerFactory.getLogger(ReceiverManager.class);

  private boolean pipeServerEnable;
  // <pipeName, <remoteIp, pipeInfo>>
  private Map<String, Map<String, PipeInfo>> pipeInfoMap;
  // <pipeFolderName, pipeMsg>
  private Map<String, List<PipeMessage>> pipeMessageMap;
  private ReceiverLog log;

  public void init() throws StartupException {
    log = new ReceiverLog();
    ReceiverLogAnalyzer.scan();
    pipeInfoMap = ReceiverLogAnalyzer.getPipeInfoMap();
    pipeServerEnable = ReceiverLogAnalyzer.isPipeServerEnable();
    pipeMessageMap = ReceiverLogAnalyzer.getPipeMessageMap();
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
    if (!pipeInfoMap.containsKey(pipeName)) {
      pipeInfoMap.put(pipeName, new HashMap<>());
    }
    pipeInfoMap
        .get(pipeName)
        .put(remoteIp, new PipeInfo(pipeName, remoteIp, PipeStatus.STOP, createTime));
  }

  public void startPipe(String pipeName, String remoteIp) throws IOException {
    log.startPipe(pipeName, remoteIp);
    pipeInfoMap.get(pipeName).get(remoteIp).setStatus(PipeStatus.RUNNING);
  }

  public void stopPipe(String pipeName, String remoteIp) throws IOException {
    log.stopPipe(pipeName, remoteIp);
    pipeInfoMap.get(pipeName).get(remoteIp).setStatus(PipeStatus.STOP);
  }

  public void dropPipe(String pipeName, String remoteIp) throws IOException {
    log.dropPipe(pipeName, remoteIp);
    pipeInfoMap.get(pipeName).get(remoteIp).setStatus(PipeStatus.DROP);
  }

  public List<PipeInfo> getPipeInfos(String pipeName) {
    List<PipeInfo> res;
    if (pipeInfoMap.containsKey(pipeName)) {
      res = new ArrayList<>(pipeInfoMap.get(pipeName).values());
    } else {
      res = new ArrayList<>();
    }
    return res;
  }

  public List<PipeInfo> getAllPipeInfos() {
    List<PipeInfo> res = new ArrayList<>();
    for (String pipeName : pipeInfoMap.keySet()) {
      res.addAll(pipeInfoMap.get(pipeName).values());
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
  public void writePipeMessage(
      String pipeName, String remoteIp, long createTime, PipeMessage message) {
    if (pipeInfoMap.containsKey(pipeName) && pipeInfoMap.get(pipeName).containsKey(remoteIp)) {
      synchronized (pipeInfoMap.get(pipeName).get(remoteIp)) {
        String pipeIdentifier =
            SyncPathUtil.getReceiverPipeFolderName(pipeName, remoteIp, createTime);
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
    }
  }

  /**
   * read recent messages about one pipe
   *
   * @param pipeName name of pipe
   * @param remoteIp remoteIp of pipe
   * @param createTime createTime of pipe
   * @param del if del is true, these messages will not be deleted. Otherwise, these messages can be
   *     read next time.
   * @return recent messages
   */
  public List<PipeMessage> getPipeMessages(
      String pipeName, String remoteIp, long createTime, boolean del) {
    List<PipeMessage> pipeMessageList = new ArrayList<>();
    if (pipeInfoMap.containsKey(pipeName) && pipeInfoMap.get(pipeName).containsKey(remoteIp)) {
      synchronized (pipeInfoMap.get(pipeName).get(remoteIp)) {
        String pipeIdentifier =
            SyncPathUtil.getReceiverPipeFolderName(pipeName, remoteIp, createTime);
        if (del) {
          try {
            log.readPipeMsg(pipeIdentifier);
          } catch (IOException e) {
            logger.error(
                "Can not read pipe message about {} from disk because {}",
                pipeIdentifier,
                e.getMessage());
          }
        }
        if (pipeMessageMap.containsKey(pipeIdentifier)) {
          pipeMessageList = pipeMessageMap.get(pipeIdentifier);
          if (del) {
            pipeMessageMap.remove(pipeIdentifier);
          }
        }
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
   * @param del if del is true, recent messages will not be deleted. Otherwise, these messages can
   *     be read next time.
   * @return the most important message
   */
  public PipeMessage getPipeMessage(
      String pipeName, String remoteIp, long createTime, boolean del) {
    List<PipeMessage> pipeMessageList = getPipeMessages(pipeName, remoteIp, createTime, del);
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
