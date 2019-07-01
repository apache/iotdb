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
import org.apache.iotdb.db.engine.cache.RowGroupBlockMetaDataCache;
import org.apache.iotdb.db.engine.cache.TsFileMetaDataCache;
import org.apache.iotdb.db.engine.filenodeV2.FileNodeManagerV2;
import org.apache.iotdb.db.engine.memcontrol.BasicMemController;
import org.apache.iotdb.db.exception.FileNodeManagerException;
import org.apache.iotdb.db.exception.StartupException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.monitor.StatMonitor;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.FileReaderManager;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.writelog.manager.MultiFileLogNodeManager;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
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

  private static final Logger LOGGER = LoggerFactory.getLogger(EnvironmentUtils.class);

  private static IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private static DirectoryManager directoryManager = DirectoryManager.getInstance();
  private static TSFileConfig tsfileConfig = TSFileDescriptor.getInstance().getConfig();

  public static long TEST_QUERY_JOB_ID = QueryResourceManager.getInstance().assignJobId();
  public static QueryContext TEST_QUERY_CONTEXT = new QueryContext(TEST_QUERY_JOB_ID);

  public static void cleanEnv() throws IOException, FileNodeManagerException {

    QueryResourceManager.getInstance().endQueryForGivenJob(TEST_QUERY_JOB_ID);

    // clear opened file streams
    FileReaderManager.getInstance().closeAndRemoveAllOpenedReaders();

    // tsFileConfig.duplicateIncompletedPage = false;
    // clean filenode manager
    if (!FileNodeManagerV2.getInstance().deleteAll()) {
      LOGGER.error("Can't close the filenode manager in EnvironmentUtils");
      Assert.fail();
    }
    StatMonitor.getInstance().close();
    FileNodeManagerV2.getInstance().resetFileNodeManager();
    // clean wal
    MultiFileLogNodeManager.getInstance().stop();
    // clean cache
    TsFileMetaDataCache.getInstance().clear();
    RowGroupBlockMetaDataCache.getInstance().clear();
    // close metadata
    MManager.getInstance().clear();
    MManager.getInstance().flushObjectToFile();
    // delete all directory
    cleanAllDir();
    // FileNodeManagerV2.getInstance().clear();
    // clear MemController
    BasicMemController.getInstance().close();
  }

  private static void cleanAllDir() throws IOException {
    // delete bufferwrite
    for (String path : directoryManager.getAllTsFileFolders()) {
      cleanDir(path);
    }
    // delete overflow
    for (String path : directoryManager.getAllOverflowFileFolders()) {
      cleanDir(path);
    }
    // delete filenode
    cleanDir(config.getFileNodeDir());
    // delete metadata
    cleanDir(config.getMetadataDir());
    // delete wal
    cleanDir(config.getWalFolder());
    // delete derby
    cleanDir(config.getDerbyHome());
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
  public static void closeMemControl() {
    config.setEnableMemMonitor(false);
  }

  public static void envSetUp() throws StartupException, IOException {
    createAllDir();
    // disable the memory control
    config.setEnableMemMonitor(false);
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
    FileNodeManagerV2.getInstance().resetFileNodeManager();
    MultiFileLogNodeManager.getInstance().start();
    TEST_QUERY_JOB_ID = QueryResourceManager.getInstance().assignJobId();
    TEST_QUERY_CONTEXT = new QueryContext(TEST_QUERY_JOB_ID);
    try {
      BasicMemController.getInstance().start();
    } catch (StartupException e) {
      LOGGER.error("", e);
    }
  }

  private static void createAllDir() throws IOException {
    // create bufferwrite
    for (String path : directoryManager.getAllTsFileFolders()) {
      createDir(path);
    }
    // create overflow
    for (String path : directoryManager.getAllOverflowFileFolders()) {
      cleanDir(path);
    }
    // create filenode
    createDir(config.getFileNodeDir());
    // create metadata
    createDir(config.getMetadataDir());
    // create wal
    createDir(config.getWalFolder());
    // create derby
    createDir(config.getDerbyHome());
    // create index
    createDir(config.getIndexFileDir());
    // create data
    createDir("data");
    // delte derby log
    // cleanDir("derby.log");
  }

  public static void createDir(String dir) throws IOException {
    File file = new File(dir);
    file.mkdirs();
  }
}
