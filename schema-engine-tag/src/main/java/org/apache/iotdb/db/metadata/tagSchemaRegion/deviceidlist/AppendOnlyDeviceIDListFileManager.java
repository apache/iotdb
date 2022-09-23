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
package org.apache.iotdb.db.metadata.tagSchemaRegion.deviceidlist;

import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.metadata.idtable.entry.DeviceIDFactory;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

public class AppendOnlyDeviceIDListFileManager {

  private static final Logger logger =
      LoggerFactory.getLogger(AppendOnlyDeviceIDListFileManager.class);

  private static final String DEVICE_ID_LIST_FILE_NAME = "device_id.list";

  private boolean isRecover;

  private String schemaDirPath;

  private File deviceIDSFile;

  private FileOutputStream outputStream;

  public AppendOnlyDeviceIDListFileManager(String schemaDirPath) {
    this.schemaDirPath = schemaDirPath;
    isRecover = true;
    try {
      initFile();
      outputStream = new FileOutputStream(deviceIDSFile, true);
    } catch (IOException e) {
      logger.error(e.getMessage());
      throw new IllegalArgumentException(
          "can't initialize device id list file at " + deviceIDSFile);
    }
  }

  private void initFile() throws IOException {
    File schemaDir = new File(schemaDirPath);
    schemaDir.mkdirs();
    deviceIDSFile = new File(schemaDir, DEVICE_ID_LIST_FILE_NAME);
    if (!deviceIDSFile.exists()) {
      // create new file
      deviceIDSFile.createNewFile();
    }
  }

  public void serialize(String deviceID) {
    try {
      if (!isRecover) {
        ReadWriteIOUtils.write(deviceID, outputStream);
      }
    } catch (IOException e) {
      logger.error("failed to serialize device id: " + deviceID);
      throw new IllegalArgumentException("can't serialize device id of " + deviceID);
    }
  }

  public void recover(DeviceIDList deviceIDList) {
    logger.info("recover device id list using file {}", deviceIDSFile);
    try (FileInputStream inputStream = new FileInputStream(deviceIDSFile)) {
      while (inputStream.available() > 0) {
        String deviceID = ReadWriteIOUtils.readString(inputStream);
        deviceIDList.add(DeviceIDFactory.getInstance().getDeviceID(deviceID));
      }
    } catch (IOException e) {
      logger.info("device id list is incomplete, we will recover as much as we can.");
    }
    isRecover = false;
  }

  @TestOnly
  public void close() throws IOException {
    try {
      outputStream.close();
    } catch (IOException e) {
      logger.error("close device id list file failed");
      throw e;
    }
  }
}
