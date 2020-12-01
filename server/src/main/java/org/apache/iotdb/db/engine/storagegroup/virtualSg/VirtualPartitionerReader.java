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


import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.db.utils.FilePathUtils;
import org.apache.iotdb.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VirtualPartitionerReader {

  private static final Logger logger = LoggerFactory.getLogger(VirtualPartitionerReader.class);
  private static final String MAPPING_SEPARATOR = ",";
  private static final String LOG_FILE_NAME = "VirtualSGMapping.log";
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private BufferedReader reader;

  VirtualPartitionerReader() {
    String systemDir = FilePathUtils.regularizePath(config.getSystemDir()) + "storage_groups";
    File logFile = SystemFileFactory.INSTANCE.getFile(systemDir + File.separator + LOG_FILE_NAME);
    // no log file, create it
    if (!logFile.exists()){
      try {
        logFile.createNewFile();
      } catch (IOException e) {
        logger.error("can not create virtual storage group mapping file because: ", e);
      }
    }

    try {
      reader = new BufferedReader(new FileReader(logFile));
    } catch (FileNotFoundException e) {
      logger.error("can not create virtual storage group mapping because: ", e);
    }
  }

  /**
   * read the mapping between virtual storage group id and device id
   * @return mapping line
   */
  public Pair<String, String> readMapping() {
    String line = null;
    try {
      line = reader.readLine();
    } catch (IOException e) {
      logger.error("can not read virtual storage group mapping because: ", e);
    }
    if(line == null){
      return null;
    }

    String[] part = line.split(MAPPING_SEPARATOR);

    return new Pair<>(part[0], part[1]);
  }

  public void close() throws IOException {
    reader.close();
  }
}
