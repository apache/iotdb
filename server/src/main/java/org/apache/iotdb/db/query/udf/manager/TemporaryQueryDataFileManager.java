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

package org.apache.iotdb.db.query.udf.manager;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.io.FileUtils;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.StartupException;
import org.apache.iotdb.db.exception.runtime.StorageEngineFailureException;
import org.apache.iotdb.db.query.udf.datastructure.SerializableList.SerializationRecorder;
import org.apache.iotdb.db.service.IService;
import org.apache.iotdb.db.service.ServiceType;
import org.apache.iotdb.tsfile.read.common.Path;

public class TemporaryQueryDataFileManager implements IService {

  private final Map<Long, Map<String, SerializationRecorder>> recorders;
  private final String temporaryFileDir;

  private TemporaryQueryDataFileManager() {
    recorders = new ConcurrentHashMap<>();
    temporaryFileDir =
        IoTDBDescriptor.getInstance().getConfig().getQueryDir() + File.separator + "udf"
            + File.separator;
    try {
      FileUtils.forceMkdir(new File(temporaryFileDir));
    } catch (IOException e) {
      throw new StorageEngineFailureException(e);
    }
  }

  public RandomAccessFile register(SerializationRecorder recorder)
      throws FileNotFoundException {
    String fileName = getFileName(recorder.getQueryId(), recorder.getDataId(), recorder.getIndex());
    recorders.putIfAbsent(recorder.getQueryId(), new ConcurrentHashMap<>())
        .putIfAbsent(fileName, recorder);
    return new RandomAccessFile(new File(fileName), "rw");
  }

  public void deregister(long queryId) {
    Map<String, SerializationRecorder> dataId2Recorders = recorders.remove(queryId);
    if (dataId2Recorders == null) {
      return;
    }
    for (SerializationRecorder recorder : dataId2Recorders.values()) {
      try {
        recorder.closeFile();
      } catch (IOException ignored) {
      }
    }
  }

  private String getFileName(long queryId, Path path, int index) {
    return getFileName(queryId, path.toString(), index);
  }

  private String getFileName(long queryId, String dataId, int index) {
    String dir = getDirName(queryId, dataId);
    makeDirIfNecessary(dir);
    return dir + index;
  }

  private String getDirName(long queryId, String dataId) {
    return temporaryFileDir + File.separator + queryId + File.separator + dataId + File.separator;
  }

  private void makeDirIfNecessary(String dir) {
    try {
      FileUtils.forceMkdir(new File(dir));
    } catch (IOException e) {
      throw new StorageEngineFailureException(e);
    }
  }

  @Override
  public void start() throws StartupException {
    // do nothing here
  }

  @Override
  public void stop() {
    try {
      for (Long queryId : recorders.keySet()) {
        deregister(queryId);
      }
      FileUtils.deleteDirectory(new File(temporaryFileDir));
    } catch (IOException ignored) {
    }
  }

  @Override
  public ServiceType getID() {
    return ServiceType.TEMPORARY_QUERY_DATA_FILE_SERVICE;
  }

  public static TemporaryQueryDataFileManager getInstance() {
    return TemporaryQueryDataFileManagerHelper.INSTANCE;
  }

  private static class TemporaryQueryDataFileManagerHelper {

    private static final TemporaryQueryDataFileManager INSTANCE = new TemporaryQueryDataFileManager();

    private TemporaryQueryDataFileManagerHelper() {
    }
  }
}
