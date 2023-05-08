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
package org.apache.iotdb.spark.db;

import org.apache.commons.io.FileUtils;
import org.apache.iotdb.commons.auth.AuthException;
import org.apache.iotdb.commons.auth.authorizer.BasicAuthorizer;
import org.apache.iotdb.commons.auth.authorizer.IAuthorizer;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.conf.directories.DirectoryManager;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.cache.BloomFilterCache;
import org.apache.iotdb.db.engine.cache.ChunkCache;
import org.apache.iotdb.db.engine.cache.TimeSeriesMetadataCache;
import org.apache.iotdb.db.engine.flush.FlushManager;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.FileReaderManager;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.commons.service.metric.MetricService;
import org.apache.iotdb.db.wal.WALManager;
import org.apache.iotdb.jdbc.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Locale;

/** This class is used for cleaning test environment in unit test and integration test */
public class EnvironmentUtils {

  private static String[] creationSqls =
      new String[] {
        "CREATE DATABASE root.vehicle.d0",
        "CREATE DATABASE root.vehicle.d1",
        "CREATE TIMESERIES root.vehicle.d0.s0 WITH DATATYPE=INT32, ENCODING=RLE",
        "CREATE TIMESERIES root.vehicle.d0.s1 WITH DATATYPE=INT64, ENCODING=RLE",
        "CREATE TIMESERIES root.vehicle.d0.s2 WITH DATATYPE=FLOAT, ENCODING=RLE",
        "CREATE TIMESERIES root.vehicle.d0.s3 WITH DATATYPE=TEXT, ENCODING=PLAIN",
        "CREATE TIMESERIES root.vehicle.d0.s4 WITH DATATYPE=BOOLEAN, ENCODING=PLAIN"
      };

  private static String[] dataSet2 =
      new String[] {
        "CREATE DATABASE root.ln.wf01.wt01",
        "CREATE TIMESERIES root.ln.wf01.wt01.status WITH DATATYPE=BOOLEAN, ENCODING=PLAIN",
        "CREATE TIMESERIES root.ln.wf01.wt01.temperature WITH DATATYPE=FLOAT, ENCODING=PLAIN",
        "CREATE TIMESERIES root.ln.wf01.wt01.hardware WITH DATATYPE=INT32, ENCODING=PLAIN",
        "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) "
            + "values(1, 1.1, false, 11)",
        "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) "
            + "values(2, 2.2, true, 22)",
        "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) "
            + "values(3, 3.3, false, 33 )",
        "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) "
            + "values(4, 4.4, false, 44)",
        "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) "
            + "values(5, 5.5, false, 55)"
      };

  private static String insertTemplate =
      "INSERT INTO root.vehicle.d0(timestamp,s0,s1,s2,s3,s4)" + " VALUES(%d,%d,%d,%f,%s,%s)";

  private static final Logger logger = LoggerFactory.getLogger(EnvironmentUtils.class);

  private static IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private static final CommonConfig commonConfig = CommonDescriptor.getInstance().getConfig();
  private static DirectoryManager directoryManager = DirectoryManager.getInstance();

  public static long TEST_QUERY_JOB_ID = QueryResourceManager.getInstance().assignQueryId();
  public static QueryContext TEST_QUERY_CONTEXT = new QueryContext(TEST_QUERY_JOB_ID);

  private static long oldSeqTsFileSize = config.getSeqTsFileSize();
  private static long oldUnSeqTsFileSize = config.getUnSeqTsFileSize();

  private static long oldGroupSizeInByte = config.getMemtableSizeThreshold();

  public static void cleanEnv() throws IOException, StorageEngineException {

    QueryResourceManager.getInstance().endQuery(TEST_QUERY_JOB_ID);

    // clear opened file streams
    FileReaderManager.getInstance().closeAndRemoveAllOpenedReaders();

    // clean database manager
    StorageEngine.getInstance().reset();
    CommonDescriptor.getInstance().getConfig().setNodeStatus(NodeStatus.Running);

    // clean wal
    WALManager.getInstance().stop();
    // clean cache
    if (config.isMetaDataCacheEnable()) {
      ChunkCache.getInstance().clear();
      TimeSeriesMetadataCache.getInstance().clear();
      BloomFilterCache.getInstance().clear();
    }
    // close metadata
    MetricService.getInstance().stop();
    // delete all directory
    cleanAllDir();

    config.setSeqTsFileSize(oldSeqTsFileSize);
    config.setUnSeqTsFileSize(oldUnSeqTsFileSize);
    config.setMemtableSizeThreshold(oldGroupSizeInByte);
  }

  public static void cleanAllDir() throws IOException {
    // delete sequential files
    for (String path : directoryManager.getAllSequenceFileFolders()) {
      cleanDir(path);
    }
    // delete unsequence files
    for (String path : directoryManager.getAllUnSequenceFileFolders()) {
      cleanDir(path);
    }
    // delete system info
    cleanDir(config.getSystemDir());
    // delete wal
    for (String walDir : commonConfig.getWalDirs()) {
      cleanDir(walDir);
    }
    // delete data files
    for (String dataDir : config.getDataDirs()) {
      cleanDir(dataDir);
    }
  }

  public static void cleanDir(String dir) throws IOException {
    FileUtils.deleteDirectory(new File(dir));
  }

  /** disable memory control</br> this function should be called before all code in the setup */
  public static void envSetUp() throws StartupException {
    createAllDir();
    IAuthorizer authorizer;
    try {
      authorizer = BasicAuthorizer.getInstance();
    } catch (AuthException e) {
      throw new StartupException(e);
    }
    try {
      authorizer.reset();
    } catch (AuthException e) {
      throw new StartupException(e);
    }
    StorageEngine.getInstance().reset();
    WALManager.getInstance().start();
    FlushManager.getInstance().start();
    TEST_QUERY_JOB_ID = QueryResourceManager.getInstance().assignQueryId();
    TEST_QUERY_CONTEXT = new QueryContext(TEST_QUERY_JOB_ID);
  }

  private static void createAllDir() {
    // create sequential files
    for (String path : directoryManager.getAllSequenceFileFolders()) {
      createDir(path);
    }
    // create unsequential files
    for (String path : directoryManager.getAllUnSequenceFileFolders()) {
      createDir(path);
    }
    // create database
    createDir(config.getSystemDir());
    // create wal
    for (String walDir : commonConfig.getWalDirs()) {
      createDir(walDir);
    }
    // create data
    for (String dataDir : config.getDataDirs()) {
      createDir(dataDir);
    }
  }

  private static void createDir(String dir) {
    File file = new File(dir);
    file.mkdirs();
  }

  public static void prepareData() throws SQLException {
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      for (String sql : creationSqls) {
        statement.execute(sql);
      }

      for (String sql : dataSet2) {
        statement.execute(sql);
      }

      // prepare BufferWrite file
      for (int i = 5000; i < 7000; i++) {
        statement.execute(
            String.format(
                Locale.ENGLISH, insertTemplate, i, i, i, (double) i, "'" + i + "'", "true"));
      }
      statement.execute("flush");
      for (int i = 7500; i < 8500; i++) {
        statement.execute(
            String.format(
                Locale.ENGLISH, insertTemplate, i, i, i, (double) i, "'" + i + "'", "false"));
      }
      statement.execute("flush");
      // prepare Unseq-File
      for (int i = 500; i < 1500; i++) {
        statement.execute(
            String.format(
                Locale.ENGLISH, insertTemplate, i, i, i, (double) i, "'" + i + "'", "true"));
      }
      statement.execute("flush");
      for (int i = 3000; i < 6500; i++) {
        statement.execute(
            String.format(
                Locale.ENGLISH, insertTemplate, i, i, i, (double) i, "'" + i + "'", "false"));
      }
      statement.execute("merge");

      // prepare BufferWrite cache
      for (int i = 9000; i < 10000; i++) {
        statement.execute(
            String.format(
                Locale.ENGLISH, insertTemplate, i, i, i, (double) i, "'" + i + "'", "true"));
      }
      // prepare Overflow cache
      for (int i = 2000; i < 2500; i++) {
        statement.execute(
            String.format(
                Locale.ENGLISH, insertTemplate, i, i, i, (double) i, "'" + i + "'", "false"));
      }

    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
