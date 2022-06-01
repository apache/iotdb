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
package org.apache.iotdb.db.sync.receiver;

import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.commons.sync.SyncPathUtil;
import org.apache.iotdb.db.sync.receiver.manager.PipeInfo;
import org.apache.iotdb.db.sync.receiver.manager.PipeMessage;
import org.apache.iotdb.db.sync.receiver.recovery.ReceiverLog;
import org.apache.iotdb.db.sync.receiver.recovery.ReceiverLogAnalyzer;
import org.apache.iotdb.db.sync.sender.pipe.Pipe;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.iotdb.commons.sync.SyncConstant.PIPE_MESSAGE_TYPE;
import static org.apache.iotdb.commons.sync.SyncConstant.PIPE_NAME_MAP_TYPE;

public abstract class AbstractReceiverInfo {

  protected static final Logger LOGGER = LoggerFactory.getLogger(AbstractReceiverInfo.class);

  protected boolean pipeServerEnable;
  // <pipeName, <remoteIp, <createTime, status>>>
  protected Map<String, Map<String, Map<Long, Pipe.PipeStatus>>> pipeInfos;
  // <pipeFolderName, pipeMsg>
  protected Map<String, List<PipeMessage>> pipeMessageMap;
  protected ReceiverLog log;

  public AbstractReceiverInfo() {
    log = new ReceiverLog();
    ReceiverLogAnalyzer analyzer = new ReceiverLogAnalyzer();
    try {
      analyzer.scan();
      pipeInfos = analyzer.getPipeInfos();
      pipeServerEnable = analyzer.isPipeServerEnable();
      pipeMessageMap = analyzer.getPipeMessageMap();
    } catch (StartupException e) {
      LOGGER.error(
          "Cannot recover ReceiverInfo because {}. Use default info values.", e.getMessage());
      pipeInfos = new ConcurrentHashMap<>();
      pipeMessageMap = new ConcurrentHashMap<>();
      pipeServerEnable = false;
    }
  }

  protected abstract void afterStartPipe(String pipeName, String remoteIp, long createTime);

  protected abstract void afterStopPipe(String pipeName, String remoteIp, long createTime);

  protected abstract void afterDropPipe(String pipeName, String remoteIp, long createTime);

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
    PipeInfo pipeInfo = getPipeInfo(pipeName, remoteIp, createTime);
    if (pipeInfo == null || pipeInfo.getStatus().equals(Pipe.PipeStatus.DROP)) {
      LOGGER.info(
          "create Pipe name={}, remoteIp={}, createTime={}", pipeName, remoteIp, createTime);
      createDir(pipeName, remoteIp, createTime);
      log.createPipe(pipeName, remoteIp, createTime);
      pipeInfos.putIfAbsent(pipeName, new HashMap<>());
      pipeInfos.get(pipeName).putIfAbsent(remoteIp, new HashMap<>());
      pipeInfos.get(pipeName).get(remoteIp).put(createTime, Pipe.PipeStatus.STOP);
    }
  }

  public void startPipe(String pipeName, String remoteIp, long createTime) throws IOException {
    PipeInfo pipeInfo = getPipeInfo(pipeName, remoteIp, createTime);
    if (pipeInfo != null && pipeInfo.getStatus().equals(Pipe.PipeStatus.STOP)) {
      LOGGER.info("start Pipe name={}, remoteIp={}, createTime={}", pipeName, remoteIp, createTime);
      log.startPipe(pipeName, remoteIp, createTime);
      pipeInfos.get(pipeName).get(remoteIp).put(createTime, Pipe.PipeStatus.RUNNING);
      afterStartPipe(pipeName, remoteIp, createTime);
    }
  }

  public void stopPipe(String pipeName, String remoteIp, long createTime) throws IOException {
    PipeInfo pipeInfo = getPipeInfo(pipeName, remoteIp, createTime);
    if (pipeInfo != null && pipeInfo.getStatus().equals(Pipe.PipeStatus.RUNNING)) {
      LOGGER.info("stop Pipe name={}, remoteIp={}, createTime={}", pipeName, remoteIp, createTime);
      log.stopPipe(pipeName, remoteIp, createTime);
      pipeInfos.get(pipeName).get(remoteIp).put(createTime, Pipe.PipeStatus.STOP);
      afterStopPipe(pipeName, remoteIp, createTime);
    }
  }

  public void dropPipe(String pipeName, String remoteIp, long createTime) throws IOException {
    PipeInfo pipeInfo = getPipeInfo(pipeName, remoteIp, createTime);
    if (pipeInfo != null && !pipeInfo.getStatus().equals(Pipe.PipeStatus.DROP)) {
      LOGGER.info("drop Pipe name={}, remoteIp={}, createTime={}", pipeName, remoteIp, createTime);
      log.dropPipe(pipeName, remoteIp, createTime);
      pipeInfos.get(pipeName).get(remoteIp).put(createTime, Pipe.PipeStatus.DROP);
      afterDropPipe(pipeName, remoteIp, createTime);
    }
  }

  public List<PipeInfo> getPipeInfosByPipeName(String pipeName) {
    if (!pipeInfos.containsKey(pipeName)) {
      return Collections.emptyList();
    }
    List<PipeInfo> res = new ArrayList<>();
    for (Map.Entry<String, Map<Long, Pipe.PipeStatus>> remoteIpEntry :
        pipeInfos.get(pipeName).entrySet()) {
      for (Map.Entry<Long, Pipe.PipeStatus> createTimeEntry : remoteIpEntry.getValue().entrySet()) {
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

  public void serialize(OutputStream outputStream) throws IOException {
    try (ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream)) {
      // serialize pipeServerEnable
      ReadWriteIOUtils.write(pipeServerEnable, objectOutputStream);
      // serialize pipeInfos
      for (Map.Entry<String, Map<String, Map<Long, Pipe.PipeStatus>>> pipeNameEntry :
          pipeInfos.entrySet()) {
        ReadWriteIOUtils.write(PIPE_NAME_MAP_TYPE, objectOutputStream);
        ReadWriteIOUtils.write(pipeNameEntry.getKey(), objectOutputStream);
        ReadWriteIOUtils.write(pipeNameEntry.getValue().size(), objectOutputStream);
        for (Map.Entry<String, Map<Long, Pipe.PipeStatus>> remoteIpEntry :
            pipeNameEntry.getValue().entrySet()) {
          ReadWriteIOUtils.write(remoteIpEntry.getKey(), objectOutputStream);
          ReadWriteIOUtils.write(remoteIpEntry.getValue().size(), objectOutputStream);
          for (Map.Entry<Long, Pipe.PipeStatus> createTimeEntry :
              remoteIpEntry.getValue().entrySet()) {
            ReadWriteIOUtils.write(createTimeEntry.getKey(), objectOutputStream);
            ReadWriteIOUtils.write(createTimeEntry.getValue().ordinal(), objectOutputStream);
          }
        }
      }
      // serialize pipeMessages
      ReadWriteIOUtils.write(PIPE_MESSAGE_TYPE, objectOutputStream);
      ReadWriteIOUtils.write(pipeMessageMap.size(), objectOutputStream);
      for (Map.Entry<String, List<PipeMessage>> pipeMessageEntry : pipeMessageMap.entrySet()) {
        ReadWriteIOUtils.write(pipeMessageEntry.getKey(), objectOutputStream);
        ReadWriteIOUtils.write(pipeMessageEntry.getValue().size(), objectOutputStream);
        for (PipeMessage pipeMessage : pipeMessageEntry.getValue()) {
          objectOutputStream.writeObject(pipeMessage);
        }
      }
    }
  }

  public void deserialize(InputStream inputStream) throws IOException {
    try (ObjectInputStream objectInputStream = new ObjectInputStream(inputStream)) {
      // deserialize pipeServerEnable
      pipeServerEnable = ReadWriteIOUtils.readBool(objectInputStream);
      // deserialize pipeInfos
      pipeInfos = new ConcurrentHashMap<>();
      while (ReadWriteIOUtils.readByte(objectInputStream) == PIPE_NAME_MAP_TYPE) {
        String pipeName = ReadWriteIOUtils.readString(objectInputStream);
        Map<String, Map<Long, Pipe.PipeStatus>> pipeNameMap = new ConcurrentHashMap<>();
        pipeInfos.put(pipeName, pipeNameMap);
        int remoteIpSize = ReadWriteIOUtils.readInt(objectInputStream);
        for (int i = 0; i < remoteIpSize; i++) {
          String remoteIp = ReadWriteIOUtils.readString(objectInputStream);
          Map<Long, Pipe.PipeStatus> remoteIpMap = new ConcurrentHashMap<>();
          pipeNameMap.put(remoteIp, remoteIpMap);
          int createTimeSize = ReadWriteIOUtils.readInt(objectInputStream);
          for (int j = 0; j < createTimeSize; j++) {
            Long createTime = ReadWriteIOUtils.readLong(objectInputStream);
            remoteIpMap.put(
                createTime,
                Pipe.PipeStatus.getByValue(ReadWriteIOUtils.readInt(objectInputStream)));
          }
        }
      }
      // deserialize pipeMessages
      pipeMessageMap = new ConcurrentHashMap<>();
      int mapSize = ReadWriteIOUtils.readInt(objectInputStream);
      for (int i = 0; i < mapSize; i++) {
        String pipeFolderName = ReadWriteIOUtils.readString(objectInputStream);
        List<PipeMessage> pipeMessageList = new ArrayList<>();
        pipeMessageMap.put(pipeFolderName, pipeMessageList);
        int messageSize = ReadWriteIOUtils.readInt(objectInputStream);
        for (int j = 0; j < messageSize; j++) {
          pipeMessageList.add((PipeMessage) objectInputStream.readObject());
        }
      }
    } catch (ClassNotFoundException e) {
      throw new IOException(e);
    }
    // update log
    log.close();
    if (pipeServerEnable) {
      log.startPipeServer();
    }
    List<PipeInfo> pipeInfos = getAllPipeInfos();
    for (PipeInfo pipeInfo : pipeInfos) {
      log.createPipe(pipeInfo.getPipeName(), pipeInfo.getRemoteIp(), pipeInfo.getCreateTime());
      switch (pipeInfo.getStatus()) {
        case RUNNING:
          log.startPipe(pipeInfo.getPipeName(), pipeInfo.getRemoteIp(), pipeInfo.getCreateTime());
          break;
        case STOP:
          log.stopPipe(pipeInfo.getPipeName(), pipeInfo.getRemoteIp(), pipeInfo.getCreateTime());
          break;
        case DROP:
          log.dropPipe(pipeInfo.getPipeName(), pipeInfo.getRemoteIp(), pipeInfo.getCreateTime());
          break;
        default:
          throw new UnsupportedOperationException();
      }
    }
    for (Map.Entry<String, List<PipeMessage>> entry : pipeMessageMap.entrySet()) {
      for (PipeMessage pipeMessage : entry.getValue()) {
        log.writePipeMsg(entry.getKey(), pipeMessage);
      }
    }
  }

  private void createDir(String pipeName, String remoteIp, long createTime) {
    File f = new File(SyncPathUtil.getReceiverFileDataDir(pipeName, remoteIp, createTime));
    if (!f.exists()) {
      f.mkdirs();
    }
    f = new File(SyncPathUtil.getReceiverPipeLogDir(pipeName, remoteIp, createTime));
    if (!f.exists()) {
      f.mkdirs();
    }
  }
}
