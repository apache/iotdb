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
package org.apache.iotdb.db.conf;

import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Properties;

public class IoTDBConfigCheck {

  // this file is located in data/system/schema/system_properties.
  // If user delete folder "data", system_properties can reset.
  public static final String PROPERTIES_FILE_NAME = "system.properties";
  public static final String SCHEMA_DIR =
          IoTDBDescriptor.getInstance().getConfig().getSchemaDir();
  private static final IoTDBConfigCheck INSTANCE = new IoTDBConfigCheck();
  private static final Logger logger = LoggerFactory.getLogger(IoTDBDescriptor.class);
  // this is a initial parameter.
  private static String TIMESTAMP_PRECISION = "ms";
  private static long PARTITION_INTERVAL = 86400;
  private Properties properties = new Properties();

  public static final IoTDBConfigCheck getInstance() {
    return IoTDBConfigCheck.INSTANCE;
  }

  public void checkConfig() {
    TIMESTAMP_PRECISION = IoTDBDescriptor.getInstance().getConfig().getTimestampPrecision();

    // check time stamp precision
    if (!(TIMESTAMP_PRECISION.equals("ms") || TIMESTAMP_PRECISION.equals("us")
            || TIMESTAMP_PRECISION.equals("ns"))) {
      logger.error("Wrong timestamp precision, please set as: ms, us or ns ! Current is: "
              + TIMESTAMP_PRECISION);
      System.exit(-1);
    }

    PARTITION_INTERVAL = IoTDBDescriptor.getInstance().getConfig()
            .getPartitionInterval();

    // check partition interval
    if (PARTITION_INTERVAL <= 0) {
      logger.error("Partition interval must larger than 0!");
      System.exit(-1);
    }

    createDir(SCHEMA_DIR);
    checkFile(SCHEMA_DIR);
    logger.info("System configuration is ok.");
  }

  private void createDir(String filepath) {
    File dir = SystemFileFactory.INSTANCE.getFile(filepath);
    if (!dir.exists()) {
      dir.mkdirs();
      logger.info(" {} dir has been created.", SCHEMA_DIR);
    }
  }

  private void checkFile(String filepath) {
    // create file : read timestamp precision from engine.properties, create system_properties.txt
    // use output stream to write timestamp precision to file.
    File file = SystemFileFactory.INSTANCE
            .getFile(filepath + File.separator + PROPERTIES_FILE_NAME);
    try {
      if (!file.exists()) {
        file.createNewFile();
        logger.info(" {} has been created.", file.getAbsolutePath());
        try (FileOutputStream outputStream = new FileOutputStream(file.toString())) {
          properties.setProperty("timestamp_precision", TIMESTAMP_PRECISION);
          properties.setProperty("storage_group_time_range", String.valueOf(PARTITION_INTERVAL));
          properties.store(outputStream, "System properties:");
        }
      }
    } catch (IOException e) {
      logger.error("Can not create {}.", file.getAbsolutePath(), e);
    }
    // get existed properties from system_properties.txt
    File inputFile = SystemFileFactory.INSTANCE
            .getFile(filepath + File.separator + PROPERTIES_FILE_NAME);
    try (FileInputStream inputStream = new FileInputStream(inputFile.toString())) {
      properties.load(new InputStreamReader(inputStream, TSFileConfig.STRING_CHARSET));
      if (!properties.getProperty("timestamp_precision").equals(TIMESTAMP_PRECISION)) {
        logger.error("Wrong timestamp precision, please set as: " + properties
                .getProperty("timestamp_precision") + " !");
        System.exit(-1);
      }
      if (!(Long.parseLong(properties.getProperty("storage_group_time_range"))
              == PARTITION_INTERVAL)) {
        logger.error("Wrong storage group time range, please set as: " + properties
                .getProperty("storage_group_time_range") + " !");
        System.exit(-1);
      }
    } catch (IOException e) {
      logger.error("Load system.properties from {} failed.", file.getAbsolutePath(), e);
    }
  }
}


