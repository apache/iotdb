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

package org.apache.iotdb.db.query.udf.service;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.io.FileUtils;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.db.exception.StartupException;
import org.apache.iotdb.db.query.udf.datastructure.SerializableList.SerializationRecorder;
import org.apache.iotdb.db.service.IService;
import org.apache.iotdb.db.service.ServiceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TemporaryQueryDataFileService implements IService {

  private static final Logger logger = LoggerFactory.getLogger(TemporaryQueryDataFileService.class);

  private static final String TEMPORARY_FILE_DIR =
      IoTDBDescriptor.getInstance().getConfig().getQueryDir()
          + File.separator + "udf" + File.separator + "tmp" + File.separator;

  private final Map<Long, Map<String, SerializationRecorder>> recorders;

  private TemporaryQueryDataFileService() {
    recorders = new ConcurrentHashMap<>();
  }

  public RandomAccessFile register(SerializationRecorder recorder) throws IOException {
    long queryId = recorder.getQueryId();
    String dirName = getDirName(queryId, recorder.getDataId());
    makeDirIfNecessary(dirName);
    String fileName = getFileName(dirName, recorder.getIndex());
    if (!recorders.containsKey(queryId)) {
      recorders.put(queryId, new ConcurrentHashMap<>());
    }
    recorders.get(queryId).putIfAbsent(fileName, recorder);
    return new RandomAccessFile(SystemFileFactory.INSTANCE.getFile(fileName), "rw");
  }

  public void deregister(long queryId) {
    Map<String, SerializationRecorder> dataId2Recorders = recorders.remove(queryId);
    if (dataId2Recorders == null) {
      return;
    }
    for (SerializationRecorder recorder : dataId2Recorders.values()) {
      try {
        recorder.closeFile();
      } catch (IOException e) {
        logger.warn(String.format("Failed to close file in method deregister(%d), because %s",
            queryId, e.toString()));
      }
    }
    try {
      FileUtils.cleanDirectory(SystemFileFactory.INSTANCE.getFile(getDirName(queryId)));
    } catch (IOException e) {
      logger.warn(String.format("Failed to clean dir in method deregister(%d), because %s",
          queryId, e.toString()));
    }
  }

  private void makeDirIfNecessary(String dir) throws IOException {
    File file = SystemFileFactory.INSTANCE.getFile(dir);
    if (file.exists() && file.isDirectory()) {
      return;
    }
    FileUtils.forceMkdir(file);
  }

  private String getDirName(long queryId) {
    return TEMPORARY_FILE_DIR + File.separator + queryId + File.separator;
  }

  private String getDirName(long queryId, String dataId) {
    return TEMPORARY_FILE_DIR + File.separator + queryId + File.separator + dataId + File.separator;
  }

  private String getFileName(String dir, int index) {
    return dir + index;
  }

  @Override
  public void start() throws StartupException {
    try {
      makeDirIfNecessary(TEMPORARY_FILE_DIR);
    } catch (IOException e) {
      throw new StartupException(e);
    }
  }

  @Override
  public void stop() {
    for (Object queryId : recorders.keySet().toArray()) {
      deregister((Long) queryId);
    }
  }

  @Override
  public ServiceType getID() {
    return ServiceType.TEMPORARY_QUERY_DATA_FILE_SERVICE;
  }

  public static TemporaryQueryDataFileService getInstance() {
    return TemporaryQueryDataFileServiceHelper.INSTANCE;
  }

  private static class TemporaryQueryDataFileServiceHelper {

    private static final TemporaryQueryDataFileService INSTANCE = new TemporaryQueryDataFileService();

    private TemporaryQueryDataFileServiceHelper() {
    }
  }
}
