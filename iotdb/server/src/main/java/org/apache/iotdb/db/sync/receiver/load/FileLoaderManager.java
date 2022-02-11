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
package org.apache.iotdb.db.sync.receiver.load;

import org.apache.iotdb.db.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.db.concurrent.ThreadName;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.SyncDeviceOwnerConflictException;
import org.apache.iotdb.db.sync.conf.SyncConstant;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/** This class is to manage all FileLoader. */
public class FileLoaderManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(FileLoaderManager.class);

  private static final int WAIT_TIMEOUT = 2000;

  private ConcurrentHashMap<String, IFileLoader> fileLoaderMap;

  private ExecutorService loadTaskRunnerPool;

  private Map<String, String> deviceOwnerMap = new HashMap<>();

  private File deviceOwnerFile;

  private File deviceOwnerTmpFile;

  private FileLoaderManager() {
    String syncSystemDir = IoTDBDescriptor.getInstance().getConfig().getSyncDir();
    deviceOwnerFile = new File(syncSystemDir, SyncConstant.DEVICE_OWNER_FILE_NAME);
    deviceOwnerTmpFile = new File(syncSystemDir, SyncConstant.DEVICE_OWNER_TMP_FILE_NAME);
    try {
      recoverDeviceOwnerMap();
    } catch (IOException | ClassNotFoundException e) {
      LOGGER.error(
          "Can not recover device owner map from file {}",
          new File(syncSystemDir, SyncConstant.DEVICE_OWNER_FILE_NAME).getAbsolutePath());
    }
  }

  public static FileLoaderManager getInstance() {
    return FileLoaderManagerHolder.INSTANCE;
  }

  private void recoverDeviceOwnerMap() throws IOException, ClassNotFoundException {
    if (deviceOwnerTmpFile.exists()) {
      deviceOwnerFile.delete();
      FileUtils.moveFile(deviceOwnerTmpFile, deviceOwnerFile);
    }
    if (deviceOwnerFile.exists()) {
      deSerializeDeviceOwnerMap(deviceOwnerFile);
    }
  }

  /**
   * Check whether there have conflicts about the device owner. If there have conflicts, reject the
   * sync process of the sg. Otherwise, update the device owners and deserialize.
   *
   * @param tsFileResource tsfile resource
   */
  public synchronized void checkAndUpdateDeviceOwner(TsFileResource tsFileResource)
      throws SyncDeviceOwnerConflictException, IOException {
    String curOwner =
        tsFileResource.getTsFile().getParentFile().getParentFile().getParentFile().getName();
    Set<String> deviceSet = tsFileResource.getDevices();
    checkDeviceConflict(curOwner, deviceSet);
    updateDeviceOwner(curOwner, deviceSet);
  }

  /**
   * Check whether there have conflicts about the device owner.
   *
   * @param curOwner sender name that want to be owner.
   * @param deviceSet device set
   */
  private void checkDeviceConflict(String curOwner, Set<String> deviceSet)
      throws SyncDeviceOwnerConflictException {
    for (String device : deviceSet) {
      if (deviceOwnerMap.containsKey(device) && !deviceOwnerMap.get(device).equals(curOwner)) {
        throw new SyncDeviceOwnerConflictException(device, deviceOwnerMap.get(device), curOwner);
      }
    }
  }

  /**
   * Update the device owners and deserialize.
   *
   * @param curOwner sender name that want to be owner.
   * @param deviceSet device set.
   */
  private void updateDeviceOwner(String curOwner, Set<String> deviceSet) throws IOException {
    boolean modify = false;
    for (String device : deviceSet) {
      if (!deviceOwnerMap.containsKey(device)) {
        deviceOwnerMap.put(device, curOwner);
        modify = true;
      }
    }
    if (modify) {
      serializeDeviceOwnerMap(deviceOwnerTmpFile);
      deviceOwnerFile.delete();
      FileUtils.moveFile(deviceOwnerTmpFile, deviceOwnerFile);
    }
  }

  private void deSerializeDeviceOwnerMap(File deviceOwnerFile)
      throws IOException, ClassNotFoundException {
    try (FileInputStream fis = new FileInputStream(deviceOwnerFile);
        ObjectInputStream deviceOwnerInput = new ObjectInputStream(fis)) {
      deviceOwnerMap = (Map<String, String>) deviceOwnerInput.readObject();
    }
  }

  private void serializeDeviceOwnerMap(File deviceOwnerFile) throws IOException {
    if (!deviceOwnerFile.getParentFile().exists()) {
      deviceOwnerFile.getParentFile().mkdirs();
    }
    if (!deviceOwnerFile.exists()) {
      deviceOwnerFile.createNewFile();
    }
    try (FileOutputStream fos = new FileOutputStream(deviceOwnerFile, false);
        ObjectOutputStream deviceOwnerOutput = new ObjectOutputStream(fos)) {
      deviceOwnerOutput.writeObject(deviceOwnerMap);
    }
  }

  public void addFileLoader(String senderName, IFileLoader fileLoader) {
    fileLoaderMap.put(senderName, fileLoader);
  }

  public void removeFileLoader(String senderName) {
    fileLoaderMap.remove(senderName);
  }

  public IFileLoader getFileLoader(String senderName) {
    return fileLoaderMap.get(senderName);
  }

  public boolean containsFileLoader(String senderName) {
    return fileLoaderMap.containsKey(senderName);
  }

  public void addLoadTaskRunner(Runnable taskRunner) {
    loadTaskRunnerPool.submit(taskRunner);
  }

  public void start() {
    if (fileLoaderMap == null) {
      fileLoaderMap = new ConcurrentHashMap<>();
    }
    if (loadTaskRunnerPool == null) {
      loadTaskRunnerPool =
          IoTDBThreadPoolFactory.newCachedThreadPool(ThreadName.LOAD_TSFILE.getName());
    }
  }

  public void stop() {
    fileLoaderMap = null;
    loadTaskRunnerPool.shutdownNow();
    int totalWaitTime = WAIT_TIMEOUT;
    while (!loadTaskRunnerPool.isTerminated()) {
      try {
        if (!loadTaskRunnerPool.awaitTermination(WAIT_TIMEOUT, TimeUnit.MILLISECONDS)) {
          LOGGER.info("File load manager thread pool doesn't exit after {}ms.", +totalWaitTime);
        }
        totalWaitTime += WAIT_TIMEOUT;
      } catch (InterruptedException e) {
        LOGGER.error("Interrupted while waiting file load manager thread pool to exit. ", e);
        Thread.currentThread().interrupt();
      }
    }
    loadTaskRunnerPool = null;
  }

  private static class FileLoaderManagerHolder {

    private static final FileLoaderManager INSTANCE = new FileLoaderManager();
  }
}
