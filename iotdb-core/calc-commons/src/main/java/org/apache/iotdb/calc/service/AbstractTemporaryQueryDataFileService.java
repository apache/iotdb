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

package org.apache.iotdb.calc.service;

import org.apache.iotdb.calc.transformation.datastructure.SerializableList.SerializationRecorder;
import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.commons.file.SystemFileFactory;
import org.apache.iotdb.commons.service.IService;
import org.apache.iotdb.commons.service.ServiceType;

import org.apache.tsfile.external.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public abstract class AbstractTemporaryQueryDataFileService implements IService {

  private static final Logger logger =
      LoggerFactory.getLogger(AbstractTemporaryQueryDataFileService.class);
  private static final AbstractTemporaryQueryDataFileService INSTANCE = loadService();

  private final AtomicLong uniqueDataId = new AtomicLong(0);
  private final Map<String, List<SerializationRecorder>> recorders = new ConcurrentHashMap<>();

  public static AbstractTemporaryQueryDataFileService getInstance() {
    return INSTANCE;
  }

  private static AbstractTemporaryQueryDataFileService loadService() {
    AbstractTemporaryQueryDataFileService service = null;
    ServiceLoader<ITemporaryQueryDataFileServiceProvider> loader =
        ServiceLoader.load(ITemporaryQueryDataFileServiceProvider.class);
    for (ITemporaryQueryDataFileServiceProvider provider : loader) {
      if (service != null) {
        throw new IllegalStateException("Multiple ITemporaryQueryDataFileServiceProvider found");
      }
      service = provider.getService();
    }
    if (service == null) {
      throw new IllegalStateException("No ITemporaryQueryDataFileServiceProvider found");
    }
    return service;
  }

  protected abstract String getTemporaryFileDir();

  public String register(SerializationRecorder recorder) throws IOException {
    String queryId = recorder.getQueryId();
    recorders
        .computeIfAbsent(queryId, k -> Collections.synchronizedList(new ArrayList<>()))
        .add(recorder);

    String dirName = getDirName(queryId);
    makeDirIfNecessary(dirName);
    return getFileName(dirName, uniqueDataId.getAndIncrement());
  }

  public void deregister(String queryId) {
    List<SerializationRecorder> recorderList = recorders.remove(queryId);
    if (recorderList == null) {
      return;
    }
    for (SerializationRecorder recorder : recorderList) {
      try {
        recorder.closeFile();
      } catch (IOException e) {
        logger.warn(
            String.format("Failed to close file in method deregister(%s), because %s", queryId, e));
      }
    }
    try {
      FileUtils.deleteDirectory(SystemFileFactory.INSTANCE.getFile(getDirName(queryId)));
    } catch (IOException e) {
      logger.warn(
          String.format("Failed to clean dir in method deregister(%s), because %s", queryId, e));
    }
  }

  private void makeDirIfNecessary(String dir) throws IOException {
    File file = SystemFileFactory.INSTANCE.getFile(dir);
    if (file.exists() && file.isDirectory()) {
      return;
    }
    FileUtils.forceMkdir(file);
  }

  private String getDirName(String queryId) {
    return getTemporaryFileDir() + File.separator + queryId + File.separator;
  }

  private String getFileName(String dir, long index) {
    return dir + index;
  }

  @Override
  public void start() throws StartupException {
    try {
      // Clean up stale temp directories left from previous runs (e.g., after a crash)
      File tmpDir = SystemFileFactory.INSTANCE.getFile(getTemporaryFileDir());
      if (tmpDir.exists()) {
        FileUtils.deleteDirectory(tmpDir);
      }
      makeDirIfNecessary(getTemporaryFileDir());
    } catch (IOException e) {
      throw new StartupException(e);
    }
  }

  @Override
  public void stop() {
    recorders.clear();
    try {
      FileUtils.deleteDirectory(SystemFileFactory.INSTANCE.getFile(getTemporaryFileDir()));
    } catch (IOException e) {
      logger.warn("Failed to delete temp dir {}.", getTemporaryFileDir(), e);
    }
  }

  @Override
  public ServiceType getID() {
    return ServiceType.TEMPORARY_QUERY_DATA_FILE_SERVICE;
  }
}
