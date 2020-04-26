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
import org.apache.iotdb.db.metadata.MLogWriter;
import org.apache.iotdb.db.metadata.MetadataConstant;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.util.Properties;

public class IoTDBConfigCheck {

  // this file is located in data/system/schema/system.properties
  // If user delete folder "data", system.properties can reset.
  public static final String PROPERTIES_FILE_NAME = "system.properties";
  public static final String SCHEMA_DIR =
          IoTDBDescriptor.getInstance().getConfig().getSchemaDir();
  private static final Logger logger = LoggerFactory.getLogger(IoTDBDescriptor.class);
  // this is a initial parameter.
  private static String timestampPrecision = "ms";
  private static long partitionInterval = 86400;
  private static String tsfileFileSystem = "LOCAL";
  private static String iotdbVersion = "0.10.0";
  private Properties properties = new Properties();

  public static IoTDBConfigCheck getInstance() {
    return IoTDBConfigCheckHolder.INSTANCE;
  }

  public void checkConfig() {
    timestampPrecision = IoTDBDescriptor.getInstance().getConfig().getTimestampPrecision();

    // check time stamp precision
    if (!(timestampPrecision.equals("ms") || timestampPrecision.equals("us")
            || timestampPrecision.equals("ns"))) {
      logger.error("Wrong timestamp precision, please set as: ms, us or ns ! Current is: "
              + timestampPrecision);
      System.exit(-1);
    }

    partitionInterval = IoTDBDescriptor.getInstance().getConfig()
            .getPartitionInterval();

    // check partition interval
    if (partitionInterval <= 0) {
      logger.error("Partition interval must larger than 0!");
      System.exit(-1);
    }

    tsfileFileSystem = IoTDBDescriptor.getInstance().getConfig().getTsFileStorageFs().toString();

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
    // create file : read timestamp precision from engine.properties, create system.properties
    // use output stream to write timestamp precision to file.
    File file = SystemFileFactory.INSTANCE
            .getFile(filepath + File.separator + PROPERTIES_FILE_NAME);
    File tmpPropertiesFile = new File(file.getAbsoluteFile() + ".tmp");
    try {
      if (!file.exists() && !tmpPropertiesFile.exists()) {
        file.createNewFile();
        logger.info(" {} has been created.", file.getAbsolutePath());
        try (FileOutputStream outputStream = new FileOutputStream(file.toString())) {
          properties.setProperty("timestamp_precision", timestampPrecision);
          properties.setProperty("storage_group_time_range", String.valueOf(partitionInterval));
          properties.setProperty("tsfile_storage_fs", tsfileFileSystem);
          properties.setProperty("iotdb_version", iotdbVersion);
          properties.store(outputStream, "System properties:");
        }
        checkProperties();
        return;
      }
      else if (!file.exists() && tmpPropertiesFile.exists()) {
        // rename upgraded system.properties.tmp to system.properties
        FSFactoryProducer.getFSFactory().moveFile(tmpPropertiesFile, file);
        logger.info(" {} has been upgraded.", file.getAbsolutePath());
        checkProperties();
        return;
      }
    } catch (IOException e) {
      logger.error("Can not create {}.", file.getAbsolutePath(), e);
    }
    
    // get existed properties from system_properties.txt
    File inputFile = SystemFileFactory.INSTANCE
            .getFile(filepath + File.separator + PROPERTIES_FILE_NAME);
    try (FileInputStream inputStream = new FileInputStream(inputFile.toString())) {
      properties.load(new InputStreamReader(inputStream, TSFileConfig.STRING_CHARSET));
      // need to upgrade
      if (!properties.containsKey("iotdb_version")) {
        upgradeMlog();
      } else {
        checkProperties();
        return;
      }
    } catch (IOException e) {
      logger.error("Load system.properties from {} failed.", file.getAbsolutePath(), e);
    }

    // if tmpPropertiesFile exists, remove it
    if (tmpPropertiesFile.exists()) {
      try {
        Files.delete(tmpPropertiesFile.toPath());
      } catch (IOException e) {
        logger.error("Fail to remove broken file {}", tmpPropertiesFile);
      }
    }
    // create an empty tmpPropertiesFile
    try {
      if (tmpPropertiesFile.createNewFile()) {
        logger.info("Create system.properties.tmp {}.", tmpPropertiesFile);
      }
    } catch (IOException e) {
      logger.error("Create system.properties.tmp {} failed.", tmpPropertiesFile, e);
    }
    // try to add the storage_group_time_range, tsfile_storage_fs 
    // and iotdb_version property in system.properties.tmp
    try (FileOutputStream outputStream = new FileOutputStream(tmpPropertiesFile.toString())) {
      properties.setProperty("storage_group_time_range", String.valueOf(partitionInterval));
      properties.setProperty("tsfile_storage_fs", tsfileFileSystem);
      properties.setProperty("iotdb_version", iotdbVersion);
      properties.store(outputStream, "System properties:");
      checkProperties();
      // upgrade finished, delete old system.properties file
      if (file.exists()) {
        Files.delete(file.toPath());
      }
      // rename system.properties.tmp to system.properties
      FSFactoryProducer.getFSFactory().moveFile(tmpPropertiesFile, file);
    }  catch (IOException e) {
      logger.error("Something went wrong while upgrading teh system.properties. The file is {}.", file.getAbsolutePath(), e);
    }

  }

  private void checkProperties() {
    if (!properties.getProperty("timestamp_precision").equals(timestampPrecision)) {
      logger.error("Wrong timestamp precision, please set as: " + properties
              .getProperty("timestamp_precision") + " !");
      System.exit(-1);
    }
    if (!(Long.parseLong(properties.getProperty("storage_group_time_range"))
            == partitionInterval)) {
      logger.error("Wrong storage group time range, please set as: " + properties
              .getProperty("storage_group_time_range") + " !");
      System.exit(-1);
    }
    if (!(properties.getProperty("tsfile_storage_fs").equals(tsfileFileSystem))) {
      logger.error("Wrong tsfile file system, please set as: " + properties
              .getProperty("tsfile_storage_fs") + " !");
      System.exit(-1);
    }
  }

  private void upgradeMlog() {
    try {
      MLogWriter.upgradeMLog(SCHEMA_DIR, MetadataConstant.METADATA_LOG);
    } catch (IOException e) {
      logger.error("Upgrade mlog.txt from {} failed.", SCHEMA_DIR, e);
    }
  }

  private static class IoTDBConfigCheckHolder {

    private static final IoTDBConfigCheck INSTANCE = new IoTDBConfigCheck();
  }
}


