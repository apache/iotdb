/**
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
package org.apache.iotdb.db.utils;

import java.io.File;
import java.io.IOException;
import org.apache.iotdb.db.auth.AuthException;
import org.apache.iotdb.db.auth.authorizer.IAuthorizer;
import org.apache.iotdb.db.auth.authorizer.LocalFileAuthorizer;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.conf.directories.DirectoryManager;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.cache.DeviceMetaDataCache;
import org.apache.iotdb.db.engine.cache.TsFileMetaDataCache;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.StartupException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.monitor.StatMonitor;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.FileReaderManager;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.writelog.manager.MultiFileLogNodeManager;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * This class is used for cleaning test environment in unit test and integration test
 * </p>
 *
 * @author liukun
 *
 */
public class EnvironmentUtils {

  private static final Logger logger = LoggerFactory.getLogger(EnvironmentUtils.class);

  private static IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private static DirectoryManager directoryManager = DirectoryManager.getInstance();

  public static long TEST_QUERY_JOB_ID = QueryResourceManager.getInstance().assignJobId();
  public static QueryContext TEST_QUERY_CONTEXT = new QueryContext(TEST_QUERY_JOB_ID);

  public static void cleanEnv() throws IOException, StorageEngineException {

    QueryResourceManager.getInstance().endQueryForGivenJob(TEST_QUERY_JOB_ID);

    // clear opened file streams
    FileReaderManager.getInstance().closeAndRemoveAllOpenedReaders();

    // tsFileConfig.duplicateIncompletedPage = false;
    // clean storage group manager
    if (!StorageEngine.getInstance().deleteAll()) {
      logger.error("Can't close the storage group manager in EnvironmentUtils");
      Assert.fail();
    }
    StatMonitor.getInstance().close();
    StorageEngine.getInstance().reset();
    // clean wal
    MultiFileLogNodeManager.getInstance().stop();
    // clean cache
    TsFileMetaDataCache.getInstance().clear();
    DeviceMetaDataCache.getInstance().clear();
    // close metadata
    MManager.getInstance().clear();
    MManager.getInstance().flushObjectToFile();
    // delete all directory
    cleanAllDir();
    StorageEngine.getInstance().setReadOnly(false);
    StorageEngine.getInstance().reset();
  }

  private static void cleanAllDir() throws IOException {
    // delete sequential files
    for (String path : directoryManager.getAllSequenceFileFolders()) {
      cleanDir(path);
    }
    // delete unsequence files
    for (String path : directoryManager.getAllUnSequenceFileFolders()) {
      cleanDir(path);
    }
    // delete system info
    cleanDir(config.getSystemInfoDir());
    // delete metadata
    cleanDir(config.getMetadataDir());
    // delete wal
    cleanDir(config.getWalFolder());
    // delete index
    cleanDir(config.getIndexFileDir());
    // delete data
    cleanDir(config.getDataDir());
  }

  public static void cleanDir(String dir) throws IOException {
    File file = new File(dir);
    if (file.exists()) {
      if (file.isDirectory()) {
        for (File subFile : file.listFiles()) {
          cleanDir(subFile.getAbsolutePath());
        }
        if(file.listFiles().length != 0){
          System.out.println(String.format("The file %s has file.", dir));
          for (File f: file.listFiles()) {
            System.out.println(f.getAbsolutePath());
          }
        }
      }
      if (!file.delete()) {
        throw new IOException(String.format("The file %s can't be deleted", dir));
      }
    }
  }

  /**
   * disable the system monitor</br>
   * this function should be called before all code in the setup
   */
  public static void closeStatMonitor() {
    config.setEnableStatMonitor(false);
  }

  /**
   * disable memory control</br>
   * this function should be called before all code in the setup
   */

  public static void envSetUp() throws StartupException, IOException {
    createAllDir();
    // disable the system monitor
    config.setEnableStatMonitor(false);
    IAuthorizer authorizer;
    try {
      authorizer = LocalFileAuthorizer.getInstance();
    } catch (AuthException e) {
      throw new StartupException(e.getMessage());
    }
    try {
      authorizer.reset();
    } catch (AuthException e) {
      throw new StartupException(e.getMessage());
    }
    StorageEngine.getInstance().reset();
    MultiFileLogNodeManager.getInstance().start();
    TEST_QUERY_JOB_ID = QueryResourceManager.getInstance().assignJobId();
    TEST_QUERY_CONTEXT = new QueryContext(TEST_QUERY_JOB_ID);
  }

  private static void createAllDir() throws IOException {
    // create sequential files
    for (String path : directoryManager.getAllSequenceFileFolders()) {
      createDir(path);
    }
    // create unsequential files
    for (String path : directoryManager.getAllUnSequenceFileFolders()) {
      cleanDir(path);
    }
    // create storage group
    createDir(config.getSystemInfoDir());
    // create metadata
    createDir(config.getMetadataDir());
    // create wal
    createDir(config.getWalFolder());
    // create index
    createDir(config.getIndexFileDir());
    // create data
    createDir("data");
  }

  private static void createDir(String dir) {
    File file = new File(dir);
    file.mkdirs();
  }
}
