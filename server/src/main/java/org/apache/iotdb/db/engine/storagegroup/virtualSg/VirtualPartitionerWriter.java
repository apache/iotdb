/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.engine.storagegroup.virtualSg;


import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.db.utils.FilePathUtils;
import org.apache.iotdb.db.utils.TestOnly;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VirtualPartitionerWriter {

  private static final Logger logger = LoggerFactory.getLogger(VirtualPartitionerWriter.class);
  private static final String MAPPING_SEPARATOR = ",";
  private static final String LINE_SEPARATOR = System.lineSeparator();
  private static final String LOG_FILE_NAME = "VirtualSGMapping.log";
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private File logFile;
  private FileOutputStream fileOutputStream;
  private FileChannel channel;

  VirtualPartitionerWriter() {
    String systemDir = FilePathUtils.regularizePath(config.getSystemDir()) + "storage_groups";
    logFile = SystemFileFactory.INSTANCE.getFile(systemDir + File.separator + LOG_FILE_NAME);

    try {
      fileOutputStream = new FileOutputStream(logFile, true);
      channel = fileOutputStream.getChannel();
    } catch (FileNotFoundException e) {
      logger.error("can not create virtual storage group mapping because: ", e);
    }
  }

  /**
   * write the mapping between virtual storage group id and device id
   *
   * @param storageGroupId virtual storage group id
   * @param deviceId       device id
   */
  public void writeMapping(String storageGroupId, String deviceId) {
    String line = storageGroupId
        + MAPPING_SEPARATOR
        + deviceId
        + LINE_SEPARATOR;

    try {
      channel.write(ByteBuffer.wrap(line.getBytes()));
      channel.force(false);
    } catch (IOException e) {
      logger.error("can not write virtual storage group mapping because: ", e);
    }
  }

  public void close() {
    try {
      channel.force(true);
      channel.close();
      fileOutputStream.close();
    } catch (IOException e) {
      logger.error("can not close virtual storage group mapping file because: ", e);

    }
  }

  @TestOnly
  public void clear() {
    close();
    logFile.delete();
  }
}
