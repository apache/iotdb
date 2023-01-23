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
package org.apache.iotdb.db.utils;

import org.apache.iotdb.commons.auth.AuthException;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.commons.udf.service.UDFManagementService;
import org.apache.iotdb.db.auth.AuthorizerManager;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.conf.directories.DirectoryManager;
import org.apache.iotdb.db.constant.TestConstant;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.cache.BloomFilterCache;
import org.apache.iotdb.db.engine.cache.ChunkCache;
import org.apache.iotdb.db.engine.cache.TimeSeriesMetadataCache;
import org.apache.iotdb.db.engine.compaction.schedule.CompactionTaskManager;
import org.apache.iotdb.db.engine.flush.FlushManager;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.metadata.idtable.IDTableManager;
import org.apache.iotdb.db.metadata.idtable.entry.DeviceIDFactory;
import org.apache.iotdb.db.metadata.schemaregion.SchemaEngine;
import org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.FileReaderManager;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.rescon.MemTableManager;
import org.apache.iotdb.db.rescon.PrimitiveArrayManager;
import org.apache.iotdb.db.rescon.SystemInfo;
import org.apache.iotdb.db.rescon.TsFileResourceManager;
import org.apache.iotdb.db.sync.common.LocalSyncInfoFetcher;
import org.apache.iotdb.db.wal.WALManager;
import org.apache.iotdb.db.wal.recover.WALRecoverManager;
import org.apache.iotdb.rpc.TConfigurationConst;
import org.apache.iotdb.rpc.TSocketWrapper;
import org.apache.iotdb.tsfile.utils.FilePathUtils;
import org.apache.iotdb.udf.api.exception.UDFManagementException;

import org.apache.commons.io.FileUtils;
import org.apache.thrift.TConfiguration;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.fail;

/** This class is used for cleaning test environment in unit test and integration test */
public class EnvironmentUtils {

  private static final Logger logger = LoggerFactory.getLogger(EnvironmentUtils.class);

  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private static final CommonConfig commonConfig = CommonDescriptor.getInstance().getConfig();
  private static final DirectoryManager directoryManager = DirectoryManager.getInstance();

  public static long TEST_QUERY_JOB_ID = 1;
  public static QueryContext TEST_QUERY_CONTEXT = new QueryContext(TEST_QUERY_JOB_ID);
  public static FragmentInstanceContext TEST_QUERY_FI_CONTEXT =
      FragmentInstanceContext.createFragmentInstanceContextForCompaction(TEST_QUERY_JOB_ID);

  private static final long oldSeqTsFileSize = config.getSeqTsFileSize();
  private static final long oldUnSeqTsFileSize = config.getUnSeqTsFileSize();

  private static final long oldGroupSizeInByte = config.getMemtableSizeThreshold();

  private static TConfiguration tConfiguration = TConfigurationConst.defaultTConfiguration;

  public static boolean examinePorts =
      Boolean.parseBoolean(System.getProperty("test.port.closed", "false"));

  public static void cleanEnv() throws IOException, StorageEngineException {
    // wait all compaction finished
    CompactionTaskManager.getInstance().waitAllCompactionFinish();
    // deregister all user defined classes
    try {
      if (UDFManagementService.getInstance() != null) {
        UDFManagementService.getInstance().deregisterAll();
      }
    } catch (UDFManagementException e) {
      fail(e.getMessage());
    }

    logger.debug("EnvironmentUtil cleanEnv...");

    QueryResourceManager.getInstance().endQuery(TEST_QUERY_JOB_ID);
    // clear opened file streams
    FileReaderManager.getInstance().closeAndRemoveAllOpenedReaders();

    if (examinePorts) {
      // TODO: this is just too slow, especially on Windows, consider a better way
      boolean closed = examinePorts();
      if (!closed) {
        // sleep 10 seconds
        try {
          TimeUnit.SECONDS.sleep(10);
        } catch (InterruptedException e) {
          // do nothing
        }

        if (!examinePorts()) {
          fail("failed to close some ports");
        }
      }
    }
    // clean wal manager
    WALManager.getInstance().stop();
    WALRecoverManager.getInstance().clear();
    StorageEngine.getInstance().stop();
    SchemaEngine.getInstance().clear();
    FlushManager.getInstance().stop();
    CommonDescriptor.getInstance().getConfig().setNodeStatus(NodeStatus.Running);
    // We must disable MQTT service as it will cost a lot of time to be shutdown, which may slow our
    // unit tests.
    IoTDBDescriptor.getInstance().getConfig().setEnableMQTTService(false);

    // clean cache
    if (config.isMetaDataCacheEnable()) {
      ChunkCache.getInstance().clear();
      TimeSeriesMetadataCache.getInstance().clear();
      BloomFilterCache.getInstance().clear();
    }

    // close array manager
    PrimitiveArrayManager.close();

    // clear system info
    SystemInfo.getInstance().close();

    // clear memtable manager info
    MemTableManager.getInstance().close();

    // clear tsFileResource manager info
    TsFileResourceManager.getInstance().clear();

    // clear id table manager
    IDTableManager.getInstance().clear();

    // clear SyncLogger
    LocalSyncInfoFetcher.getInstance().close();

    // sleep to wait other background threads to exit
    try {
      TimeUnit.MILLISECONDS.sleep(100);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }

    // delete all directory
    cleanAllDir();
    config.setSeqTsFileSize(oldSeqTsFileSize);
    config.setUnSeqTsFileSize(oldUnSeqTsFileSize);
    config.setMemtableSizeThreshold(oldGroupSizeInByte);
  }

  private static boolean examinePorts() {
    TTransport transport = TSocketWrapper.wrap(tConfiguration, "127.0.0.1", 6667, 100);
    if (!transport.isOpen()) {
      try {
        transport.open();
        logger.error("stop daemon failed. 6667 can be connected now.");
        transport.close();
        return false;
      } catch (TTransportException e) {
        // do nothing
      }
    }
    // try sync service
    transport = TSocketWrapper.wrap(tConfiguration, "127.0.0.1", 5555, 100);
    if (!transport.isOpen()) {
      try {
        transport.open();
        logger.error("stop Sync daemon failed. 5555 can be connected now.");
        transport.close();
        return false;
      } catch (TTransportException e) {
        // do nothing
      }
    }
    // try jmx connection
    try {
      JMXServiceURL url = new JMXServiceURL("service:jmx:rmi:///jndi/rmi://localhost:31999/jmxrmi");
      JMXConnector jmxConnector = JMXConnectorFactory.connect(url);
      logger.error("stop JMX failed. 31999 can be connected now.");
      jmxConnector.close();
      return false;
    } catch (IOException e) {
      // do nothing
    }
    // try MetricService
    try (Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("127.0.0.1", 9091), 100);
      logger.error("stop MetricService failed. 9091 can be connected now.");
      return false;
    } catch (Exception e) {
      // do nothing
    }
    // do nothing
    return true;
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
    // delete query
    cleanDir(config.getQueryDir());
    // delete ulog
    cleanDir(config.getUdfDir());
    // delete tlog
    cleanDir(config.getTriggerDir());
    // delete extPipe
    cleanDir(config.getExtPipeDir());
    // delete ext
    cleanDir(config.getExtDir());
    // delete mqtt dir
    cleanDir(config.getMqttDir());
    // delete wal
    for (String walDir : commonConfig.getWalDirs()) {
      cleanDir(walDir);
    }
    // delete sync dir
    cleanDir(commonConfig.getSyncDir());
    // delete data files
    for (String dataDir : config.getDataDirs()) {
      cleanDir(dataDir);
    }
  }

  public static void cleanDir(String dir) throws IOException {
    FileUtils.deleteDirectory(new File(dir));
  }

  /** disable memory control</br> this function should be called before all code in the setup */
  public static void envSetUp() {
    logger.debug("EnvironmentUtil setup...");
    config.setThriftServerAwaitTimeForStopService(60);
    // we do not start 9091 port in test.
    config.setAvgSeriesPointNumberThreshold(Integer.MAX_VALUE);
    // use async wal mode in test
    config.setAvgSeriesPointNumberThreshold(Integer.MAX_VALUE);

    createAllDir();

    StorageEngine.getInstance().start();

    SchemaEngine.getInstance().init();

    CompactionTaskManager.getInstance().start();

    try {
      WALManager.getInstance().start();
      FlushManager.getInstance().start();
    } catch (StartupException e) {
      throw new RuntimeException(e);
    }

    // reset id method
    DeviceIDFactory.getInstance().reset();

    TEST_QUERY_JOB_ID = QueryResourceManager.getInstance().assignQueryId();
    TEST_QUERY_CONTEXT = new QueryContext(TEST_QUERY_JOB_ID);
  }

  public static void stopDaemon() {}

  public static void shutdownDaemon() throws Exception {}

  public static void activeDaemon() {}

  public static void reactiveDaemon() {}

  public static void restartDaemon() throws Exception {
    shutdownDaemon();
    stopDaemon();
    IDTableManager.getInstance().clear();
    TsFileResourceManager.getInstance().clear();
    WALManager.getInstance().clear();
    WALRecoverManager.getInstance().clear();
    reactiveDaemon();
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
    // create sg dir
    String sgDir = FilePathUtils.regularizePath(config.getSystemDir()) + "databases";
    createDir(sgDir);
    // create sync
    createDir(commonConfig.getSyncDir());
    // create query
    createDir(config.getQueryDir());
    createDir(TestConstant.OUTPUT_DATA_DIR);
    // create wal
    for (String walDir : commonConfig.getWalDirs()) {
      createDir(walDir);
    }
    // create data
    for (String dataDir : config.getDataDirs()) {
      createDir(dataDir);
    }
    // create user and roles folder
    try {
      AuthorizerManager.getInstance().reset();
    } catch (AuthException e) {
      logger.error("create user and role folders failed", e);
      fail(e.getMessage());
    }
  }

  private static void createDir(String dir) {
    File file = new File(dir);
    file.mkdirs();
  }

  public static void recursiveDeleteFolder(String path) throws IOException {
    File file = new File(path);
    if (file.isDirectory()) {
      File[] files = file.listFiles();
      if (files == null || files.length == 0) {
        FileUtils.deleteDirectory(file);
      } else {
        for (File f : files) {
          recursiveDeleteFolder(f.getAbsolutePath());
        }
        FileUtils.deleteDirectory(file);
      }
    } else {
      FileUtils.delete(file);
    }
  }
}
