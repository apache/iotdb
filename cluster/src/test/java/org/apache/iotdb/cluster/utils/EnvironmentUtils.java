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
package org.apache.iotdb.cluster.utils;

import java.io.File;
import java.io.IOException;
import org.apache.iotdb.cluster.config.ClusterConfig;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.db.auth.AuthException;
import org.apache.iotdb.db.auth.authorizer.IAuthorizer;
import org.apache.iotdb.db.auth.authorizer.LocalFileAuthorizer;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.conf.directories.Directories;
import org.apache.iotdb.db.engine.cache.RowGroupBlockMetaDataCache;
import org.apache.iotdb.db.engine.cache.TsFileMetaDataCache;
import org.apache.iotdb.db.engine.filenode.FileNodeManager;
import org.apache.iotdb.db.engine.memcontrol.BasicMemController;
import org.apache.iotdb.db.exception.FileNodeManagerException;
import org.apache.iotdb.db.exception.StartupException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.monitor.StatMonitor;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.FileReaderManager;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.writelog.manager.MultiFileLogNodeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is used for cleaning cluster test environment in unit test and integration test
 */
public class EnvironmentUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(EnvironmentUtils.class);

  private static ClusterConfig clusterConfig = ClusterDescriptor.getInstance().getConfig();
  private static IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private static Directories directories = Directories.getInstance();

  public static long TEST_QUERY_JOB_ID = QueryResourceManager.getInstance().assignJobId();
  public static QueryContext TEST_QUERY_CONTEXT = new QueryContext(TEST_QUERY_JOB_ID);

  public static void cleanEnv() throws IOException, FileNodeManagerException {

    QueryResourceManager.getInstance().endQueryForGivenJob(TEST_QUERY_JOB_ID);

    // clear opened file streams
    FileReaderManager.getInstance().closeAndRemoveAllOpenedReaders();

    // tsFileConfig.duplicateIncompletedPage = false;
    // clean filenode manager
    try {
      if (!FileNodeManager.getInstance().deleteAll()) {
        LOGGER.error("Can't close the filenode manager in EnvironmentUtils");
        System.exit(1);
      }
    } catch (FileNodeManagerException e) {
      throw new IOException(e);
    }
    StatMonitor.getInstance().close();
    FileNodeManager.getInstance().resetFileNodeManager();
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
    // FileNodeManager.getInstance().reset();
    // reset MemController
    BasicMemController.getInstance().close();
    try {
      BasicMemController.getInstance().start();
    } catch (StartupException e) {
      LOGGER.error("", e);
    }
  }

  private static void cleanAllDir() throws IOException {
    // delete bufferwrite
    for (String path : directories.getAllTsFileFolders()) {
      cleanDir(path);
    }
    // delete overflow
    cleanDir(config.getOverflowDataDir());
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
    // delete raft
    clusterConfig.deleteAllPath();
    // delte data
    cleanDir("data");
    // delte derby log
    // cleanDir("derby.log");
  }

  public static void cleanDir(String dir) throws IOException {
    File file = new File(dir);
    if (file.exists()) {
      if (file.isDirectory()) {
        for (File subFile : file.listFiles()) {
          cleanDir(subFile.getAbsolutePath());
        }
      }
      if (!file.delete()) {
        throw new IOException(String.format("The file %s can't be deleted", dir));
      }
    }
  }

  public static void envSetUp() throws StartupException {
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
    FileNodeManager.getInstance().resetFileNodeManager();
    MultiFileLogNodeManager.getInstance().start();
    TEST_QUERY_JOB_ID = QueryResourceManager.getInstance().assignJobId();
    TEST_QUERY_CONTEXT = new QueryContext(TEST_QUERY_JOB_ID);
  }
}
