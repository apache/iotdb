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
package org.apache.iotdb.db.wal;

import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.commons.service.IService;
import org.apache.iotdb.commons.service.ServiceType;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.conf.directories.FolderManager;
import org.apache.iotdb.db.conf.directories.strategy.DirectoryStrategyType;
import org.apache.iotdb.db.exception.DiskSpaceInsufficientException;
import org.apache.iotdb.db.wal.node.IWALNode;
import org.apache.iotdb.db.wal.node.WALFakeNode;
import org.apache.iotdb.db.wal.node.WALNode;
import org.apache.iotdb.db.wal.utils.WALMode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/** This class is used to manage all wal nodes */
public class WALManager implements IService {
  private static final Logger logger = LoggerFactory.getLogger(WALManager.class);
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private static final int MAX_WAL_NODE_NUM =
      config.getMaxWalNodesNum() > 0 ? config.getMaxWalNodesNum() : config.getWalDirs().length * 2;

  /** manage wal folders */
  private FolderManager folderManager;
  /** protect concurrent safety of wal nodes, including walNodes, nodeCursor and nodeIdCounter */
  private final Lock nodesLock = new ReentrantLock();
  // region these variables should be protected by nodesLock
  /** wal nodes, the max number of wal nodes is MAX_WAL_NUM */
  private final List<WALNode> walNodes = new ArrayList<>(MAX_WAL_NODE_NUM);
  /** help allocate node for users */
  private int nodeCursor = -1;
  /** each wal node has a unique long value identifier */
  private long nodeIdCounter = -1;
  // endregion
  /** single thread to delete old .wal files */
  private ScheduledExecutorService walDeleteThread;

  private WALManager() {}

  /** Apply for a wal node */
  public IWALNode applyForWALNode() {
    if (config.getWalMode() == WALMode.DISABLE) {
      return WALFakeNode.getSuccessInstance();
    }

    WALNode selectedNode;
    nodesLock.lock();
    try {
      if (walNodes.size() < MAX_WAL_NODE_NUM) {
        nodeIdCounter++;
        String identifier = String.valueOf(nodeIdCounter);
        String folder;
        // get wal folder
        try {
          folder = folderManager.getNextFolder();
        } catch (DiskSpaceInsufficientException e) {
          logger.error("All disks of wal folders are full, change system mode to read-only.", e);
          config.setReadOnly(true);
          return WALFakeNode.getFailureInstance(e);
        }
        folder = folder + File.separator + identifier;
        // create new wal node
        try {
          selectedNode = new WALNode(identifier, folder);
        } catch (FileNotFoundException e) {
          logger.error("Fail to create wal node", e);
          return WALFakeNode.getFailureInstance(e);
        }
        walNodes.add(selectedNode);
      } else {
        // select next wal node by sequence order
        nodeCursor = (nodeCursor + 1) % MAX_WAL_NODE_NUM;
        selectedNode = walNodes.get(nodeCursor);
      }
    } finally {
      nodesLock.unlock();
    }
    return selectedNode;
  }

  @Override
  public void start() throws StartupException {
    if (config.getWalMode() == WALMode.DISABLE) {
      return;
    }

    try {
      folderManager =
          new FolderManager(
              Arrays.asList(config.getWalDirs()), DirectoryStrategyType.SEQUENCE_STRATEGY);
      walDeleteThread =
          IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor(ThreadName.WAL_DELETE.getName());
      walDeleteThread.scheduleWithFixedDelay(
          this::deleteOutdatedFiles,
          config.getDeleteWalFilesPeriodInMs(),
          config.getDeleteWalFilesPeriodInMs(),
          TimeUnit.MILLISECONDS);
    } catch (Exception e) {
      throw new StartupException(this.getID().getName(), e.getMessage());
    }
  }

  /** reboot wal delete thread to hot modify delete wal period */
  public void rebootWALDeleteThread() {
    if (config.getWalMode() == WALMode.DISABLE) {
      return;
    }

    logger.info("Start rebooting wal delete thread.");
    if (walDeleteThread != null) {
      shutdownThread(walDeleteThread, ThreadName.WAL_DELETE);
    }
    logger.info("Stop wal delete thread successfully, and now restart it.");
    walDeleteThread =
        IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor(ThreadName.WAL_DELETE.getName());
    walDeleteThread.scheduleWithFixedDelay(
        this::deleteOutdatedFiles, 0, config.getDeleteWalFilesPeriodInMs(), TimeUnit.MILLISECONDS);
    logger.info(
        "Reboot wal delete thread successfully, current period is {} ms",
        config.getDeleteWalFilesPeriodInMs());
  }

  /** submit delete outdated wal files task and wait for result */
  public void deleteOutdatedWALFiles() {
    if (config.getWalMode() == WALMode.DISABLE) {
      return;
    }

    Future<?> future = walDeleteThread.submit(this::deleteOutdatedFiles);
    try {
      future.get();
    } catch (ExecutionException e) {
      logger.warn("Exception occurs when deleting wal files", e);
    } catch (InterruptedException e) {
      logger.warn("Interrupted when deleting wal files", e);
      Thread.currentThread().interrupt();
    }
  }

  private void deleteOutdatedFiles() {
    for (WALNode walNode : getNodesSnapshot()) {
      walNode.deleteOutdatedFiles();
    }
  }

  private List<WALNode> getNodesSnapshot() {
    List<WALNode> snapshot;
    if (walNodes.size() < MAX_WAL_NODE_NUM) {
      nodesLock.lock();
      try {
        snapshot = new ArrayList<>(walNodes);
      } finally {
        nodesLock.unlock();
      }
    } else {
      snapshot = walNodes;
    }
    return snapshot;
  }

  @Override
  public void stop() {
    if (config.getWalMode() == WALMode.DISABLE) {
      return;
    }

    if (walDeleteThread != null) {
      shutdownThread(walDeleteThread, ThreadName.WAL_DELETE);
    }
    clear();
  }

  private void shutdownThread(ExecutorService thread, ThreadName threadName) {
    thread.shutdown();
    try {
      if (!thread.awaitTermination(30, TimeUnit.SECONDS)) {
        logger.warn("Waiting thread {} to be terminated is timeout", threadName.getName());
      }
    } catch (InterruptedException e) {
      logger.warn("Thread {} still doesn't exit after 30s", threadName.getName());
      Thread.currentThread().interrupt();
    }
  }

  @TestOnly
  public void clear() {
    nodesLock.lock();
    try {
      nodeCursor = -1;
      nodeIdCounter = -1;
      for (WALNode walNode : walNodes) {
        walNode.close();
      }
      walNodes.clear();
    } finally {
      nodesLock.unlock();
    }
  }

  @Override
  public ServiceType getID() {
    return ServiceType.WAL_SERVICE;
  }

  public static WALManager getInstance() {
    return InstanceHolder.INSTANCE;
  }

  private static class InstanceHolder {
    private InstanceHolder() {}

    private static final WALManager INSTANCE = new WALManager();
  }
}
