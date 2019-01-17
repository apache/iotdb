/**
 * Copyright © 2019 Apache IoTDB(incubating) (dev@iotdb.apache.org)
 *
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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.db.writelog.manager;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.iotdb.db.concurrent.ThreadName;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.RecoverException;
import org.apache.iotdb.db.exception.StartupException;
import org.apache.iotdb.db.service.IService;
import org.apache.iotdb.db.service.ServiceType;
import org.apache.iotdb.db.writelog.node.ExclusiveWriteLogNode;
import org.apache.iotdb.db.writelog.node.WriteLogNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MultiFileLogNodeManager implements WriteLogNodeManager, IService {

  private static final Logger logger = LoggerFactory.getLogger(MultiFileLogNodeManager.class);
  private Map<String, WriteLogNode> nodeMap;

  private Thread syncThread;
  private IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private final Runnable syncTask = new Runnable() {
    @Override
    public void run() {
      while (true) {
        if (Thread.interrupted()) {
          logger.info("WAL sync thread exits.");
          break;
        }
        logger.debug("Timed sync starts, {} nodes to be flushed", nodeMap.size());
        for (WriteLogNode node : nodeMap.values()) {
          try {
            node.forceSync();
          } catch (IOException e) {
            logger.error("Cannot sync {}, because {}", node.toString(), e.toString());
          }
        }
        logger.debug("Timed sync finished");
        try {
          Thread.sleep(config.flushWalPeriodInMs);
        } catch (InterruptedException e) {
          logger.info("WAL sync thread exits.");
          break;
        }
      }
    }
  };

  private MultiFileLogNodeManager() {
    nodeMap = new ConcurrentHashMap<>();
  }

  public static MultiFileLogNodeManager getInstance() {
    return InstanceHolder.instance;
  }

  @Override
  public WriteLogNode getNode(String identifier, String restoreFilePath,
      String processorStoreFilePath)
      throws IOException {
    WriteLogNode node = nodeMap.get(identifier);
    if (node == null && restoreFilePath != null && processorStoreFilePath != null) {
      node = new ExclusiveWriteLogNode(identifier, restoreFilePath, processorStoreFilePath);
      WriteLogNode oldNode = nodeMap.putIfAbsent(identifier, node);
      if (oldNode != null) {
        return oldNode;
      }
    }
    return node;
  }

  @Override
  public void deleteNode(String identifier) throws IOException {
    WriteLogNode node = nodeMap.remove(identifier);
    if (node != null) {
      node.delete();
    }
  }

  /*
   * Warning : caller must guarantee thread safety.
   */
  @Override
  public void recover() throws RecoverException {
    List<WriteLogNode> nodeList = new ArrayList<>(nodeMap.size());
    nodeList.addAll(nodeMap.values());
    nodeList.sort(null);
    for (WriteLogNode node : nodeList) {
      try {
        node.recover();
      } catch (RecoverException e) {
        logger.error("{} failed to recover because {}", node.toString(), e.getMessage());
        throw e;
      }
    }
  }

  @Override
  public void close() {
    if (syncThread == null || !syncThread.isAlive()) {
      logger.error("MultiFileLogNodeManager has not yet started");
      return;
    }

    logger.info("LogNodeManager starts closing..");
    syncThread.interrupt();
    logger.info("Waiting for sync thread to stop");
    while (syncThread.isAlive()) {
      // wait
    }
    logger.info("{} nodes to be closed", nodeMap.size());
    for (WriteLogNode node : nodeMap.values()) {
      try {
        node.close();
      } catch (IOException e) {
        logger.error("{} failed to close because {}", node.toString(), e.getMessage());
      }
    }
    nodeMap.clear();
    logger.info("LogNodeManager closed.");
  }

  @Override
  public boolean hasWAL(String fileNodeName) {
    return hasBufferWriteWAL(fileNodeName) || hasOverflowWAL(fileNodeName);
  }

  private boolean hasBufferWriteWAL(String fileNodeName) {
    File bufferWriteWALDir = new File(bufferWriteWALPath(fileNodeName));
    String[] files = bufferWriteWALDir.list();
    return files != null && files.length > 0;
  }

  private String bufferWriteWALPath(String fileNodeName) {
    return config.walFolder + File.separator + fileNodeName
        + IoTDBConstant.BUFFERWRITE_LOG_NODE_SUFFIX;
  }

  private boolean hasOverflowWAL(String fileNodeName) {
    File overflowWALDir = new File(overflowWALPath(fileNodeName));
    String[] files = overflowWALDir.list();
    return files != null && files.length > 0;
  }

  private String overflowWALPath(String fileNodeName) {
    return config.walFolder + File.separator + fileNodeName
        + IoTDBConstant.OVERFLOW_LOG_NODE_SUFFIX;
  }

  @Override
  public void start() throws StartupException {
    try {
      if (!config.enableWal) {
        return;
      }
      if (syncThread == null || !syncThread.isAlive()) {
        InstanceHolder.instance.syncThread = new Thread(InstanceHolder.instance.syncTask,
            ThreadName.WAL_DAEMON.getName());
        InstanceHolder.instance.syncThread.start();
      } else {
        logger.warn("MultiFileLogNodeManager has already started");
      }
    } catch (Exception e) {
      String errorMessage = String
          .format("Failed to start %s because of %s", this.getID().getName(),
              e.getMessage());
      throw new StartupException(errorMessage);
    }
  }

  @Override
  public void stop() {
    if (!config.enableWal) {
      return;
    }
    close();
  }

  @Override
  public ServiceType getID() {
    return ServiceType.WAL_SERVICE;
  }

  private static class InstanceHolder {

    private static MultiFileLogNodeManager instance = new MultiFileLogNodeManager();
  }

}
