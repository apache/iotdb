/**
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
package org.apache.iotdb.db.writelog.manager;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.iotdb.db.concurrent.ThreadName;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.exception.StartupException;
import org.apache.iotdb.db.service.IService;
import org.apache.iotdb.db.service.ServiceType;
import org.apache.iotdb.db.writelog.node.ExclusiveWriteLogNode;
import org.apache.iotdb.db.writelog.node.WriteLogNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MultiFileLogNodeManager manages all ExclusiveWriteLogNodes, each manages WALs of a TsFile
 * (either seq or unseq).
 */
public class MultiFileLogNodeManager implements WriteLogNodeManager, IService {

  private static final Logger logger = LoggerFactory.getLogger(MultiFileLogNodeManager.class);
  private Map<String, WriteLogNode> nodeMap;

  private Thread forceThread;
  private IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private final Runnable forceTask = () -> {
      while (true) {
        if (IoTDBDescriptor.getInstance().getConfig().isReadOnly()) {
          logger.warn("system mode is read-only, the force flush WAL task is stopped");
          return;
        }
        if (Thread.interrupted()) {
          logger.info("WAL force thread exits.");
          return;
        }

        for (WriteLogNode node : nodeMap.values()) {
          try {
            node.forceSync();
          } catch (IOException e) {
            logger.error("Cannot force {}, because ", node, e);
          }
        }
        try {
          Thread.sleep(config.getForceWalPeriodInMs());
        } catch (InterruptedException e) {
          logger.info("WAL force thread exits.");
          Thread.currentThread().interrupt();
          break;
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
  public WriteLogNode getNode(String identifier) {
    WriteLogNode node = nodeMap.get(identifier);
    if (node == null) {
      node = new ExclusiveWriteLogNode(identifier);
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

  @Override
  public void close() {
    if (!isActivated(forceThread)) {
      logger.warn("MultiFileLogNodeManager has not yet started");
      return;
    }
    logger.info("LogNodeManager starts closing..");
    if (isActivated(forceThread)) {
      forceThread.interrupt();
      logger.info("Waiting for force thread to stop");
      while (forceThread.isAlive()) {
        // wait for forceThread
      }
    }
    logger.info("{} nodes to be closed", nodeMap.size());
    for (WriteLogNode node : nodeMap.values()) {
      try {
        node.close();
      } catch (IOException e) {
        logger.error("failed to close {}", node, e);
      }
    }
    nodeMap.clear();
    logger.info("LogNodeManager closed.");
  }

  @Override
  public void start() throws StartupException {
    try {
      if (!config.isEnableWal()) {
        return;
      }
      if (!isActivated(forceThread)) {
        if (config.getForceWalPeriodInMs() > 0) {
          InstanceHolder.instance.forceThread = new Thread(InstanceHolder.instance.forceTask,
              ThreadName.WAL_FORCE_DAEMON.getName());
          InstanceHolder.instance.forceThread.start();
        }
      } else {
        logger.warn("MultiFileLogNodeManager has already started");
      }
    } catch (Exception e) {
      String errorMessage = String
          .format("Failed to start %s because of %s", this.getID().getName(),
              e.getMessage());
      throw new StartupException(errorMessage, e);
    }
  }

  @Override
  public void stop() {
    if (!config.isEnableWal()) {
      return;
    }
    close();
  }

  @Override
  public ServiceType getID() {
    return ServiceType.WAL_SERVICE;
  }

  private boolean isActivated(Thread thread) {
    return thread != null && thread.isAlive();
  }

  private static class InstanceHolder {
    private InstanceHolder(){}

    private static MultiFileLogNodeManager instance = new MultiFileLogNodeManager();
  }

}
