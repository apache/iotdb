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

import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.commons.queryengine.plan.udf.UDFManagementService;
import org.apache.iotdb.db.conf.DataNodeMemoryConfig;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.queryengine.execution.fragment.QueryContext;
import org.apache.iotdb.db.schemaengine.SchemaEngine;
import org.apache.iotdb.db.storageengine.StorageEngine;
import org.apache.iotdb.db.storageengine.buffer.BloomFilterCache;
import org.apache.iotdb.db.storageengine.buffer.ChunkCache;
import org.apache.iotdb.db.storageengine.buffer.TimeSeriesMetadataCache;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.CompactionTaskManager;
import org.apache.iotdb.db.storageengine.dataregion.flush.FlushManager;
import org.apache.iotdb.db.storageengine.dataregion.read.control.FileReaderManager;
import org.apache.iotdb.db.storageengine.dataregion.read.control.QueryResourceManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.timeindex.FileTimeIndexCacheRecorder;
import org.apache.iotdb.db.storageengine.dataregion.utils.tableDiskUsageIndex.TableDiskUsageIndex;
import org.apache.iotdb.db.storageengine.dataregion.wal.WALManager;
import org.apache.iotdb.db.storageengine.dataregion.wal.recover.WALRecoverManager;
import org.apache.iotdb.db.storageengine.rescon.disk.TierManager;
import org.apache.iotdb.db.storageengine.rescon.memory.MemTableManager;
import org.apache.iotdb.db.storageengine.rescon.memory.PrimitiveArrayManager;
import org.apache.iotdb.db.storageengine.rescon.memory.SystemInfo;
import org.apache.iotdb.db.storageengine.rescon.memory.TsFileResourceManager;
import org.apache.iotdb.db.utils.constant.TestConstant;
import org.apache.iotdb.rpc.TConfigurationConst;
import org.apache.iotdb.rpc.TSocketWrapper;
import org.apache.iotdb.udf.api.exception.UDFManagementException;

import org.apache.thrift.TConfiguration;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.tsfile.external.commons.io.FileUtils;
import org.apache.tsfile.fileSystem.FSFactoryProducer;
import org.apache.tsfile.utils.FilePathUtils;
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
  private static final DataNodeMemoryConfig memoryConfig =
      IoTDBDescriptor.getInstance().getMemoryConfig();
  private static final TierManager tierManager = TierManager.getInstance();

  public static long TEST_QUERY_JOB_ID = 1;
  public static QueryContext TEST_QUERY_CONTEXT = new QueryContext(TEST_QUERY_JOB_ID, false);
  public static FragmentInstanceContext TEST_QUERY_FI_CONTEXT =
      FragmentInstanceContext.createFragmentInstanceContextForCompaction(TEST_QUERY_JOB_ID);

  private static final long oldSeqTsFileSize = config.getSeqTsFileSize();
  private static final long oldUnSeqTsFileSize = config.getUnSeqTsFileSize();
  private static TConfiguration tConfiguration = TConfigurationConst.defaultTConfiguration;

  public static boolean examinePorts =
      Boolean.parseBoolean(System.getProperty("test.port.closed", "false"));

  // Per-fork port namespace. Each surefire fork shifts every test-bound port
  // (and the corresponding examinePorts() check) by FORK_PORT_OFFSET so that
  // parallel forks live in disjoint port spaces. This keeps the leak-detection
  // power of examinePorts() (a fork still sees its own un-closed ports) while
  // removing the cross-fork false positive (one fork's bound port no longer
  // looks like another fork's leak). surefire.forkNumber starts at 1; defaults
  // to 1 outside surefire (IDE). Modulo 30 caps the offset so the highest
  // checked port (31999 + 30*1000 = 61999) stays within range.
  public static final int FORK_PORT_OFFSET =
      ((Integer.parseInt(System.getProperty("surefire.forkNumber", "1")) - 1) % 30 + 1) * 1000;

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
    if (memoryConfig.isMetaDataCacheEnable()) {
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
  }

  private static boolean examinePorts() {
    // All checks use this fork's port namespace (base + FORK_PORT_OFFSET) so
    // that we only ever see ports bound by THIS JVM — sibling forks live in
    // disjoint namespaces.
    int rpcPort = 6667 + FORK_PORT_OFFSET;
    int syncPort = 5555 + FORK_PORT_OFFSET;
    int jmxPort = 31999 + FORK_PORT_OFFSET;
    int metricPort = 9091 + FORK_PORT_OFFSET;

    TTransport transport = TSocketWrapper.wrap(tConfiguration, "127.0.0.1", rpcPort, 100);
    if (transport != null && !transport.isOpen()) {
      try {
        transport.open();
        logger.error("stop daemon failed. {} can be connected now.", rpcPort);
        transport.close();
        return false;
      } catch (TTransportException e) {
        // do nothing
      }
    }
    // try sync service
    transport = TSocketWrapper.wrap(tConfiguration, "127.0.0.1", syncPort, 100);
    if (transport != null && !transport.isOpen()) {
      try {
        transport.open();
        logger.error("stop Sync daemon failed. {} can be connected now.", syncPort);
        transport.close();
        return false;
      } catch (TTransportException e) {
        // do nothing
      }
    }
    // try jmx connection
    try {
      JMXServiceURL url =
          new JMXServiceURL("service:jmx:rmi:///jndi/rmi://localhost:" + jmxPort + "/jmxrmi");
      JMXConnector jmxConnector = JMXConnectorFactory.connect(url);
      logger.error("stop JMX failed. {} can be connected now.", jmxPort);
      jmxConnector.close();
      return false;
    } catch (IOException e) {
      // do nothing
    }
    // try MetricService
    try (Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("127.0.0.1", metricPort), 100);
      logger.error("stop MetricService failed. {} can be connected now.", metricPort);
      return false;
    } catch (Exception e) {
      // do nothing
    }
    // do nothing
    return true;
  }

  public static void cleanAllDir() throws IOException {
    // delete sequential files
    for (String path : tierManager.getAllLocalSequenceFileFolders()) {
      cleanDir(path);
    }
    // delete unsequence files
    for (String path : tierManager.getAllLocalUnSequenceFileFolders()) {
      cleanDir(path);
    }
    FileTimeIndexCacheRecorder.getInstance().close();
    TableDiskUsageIndex.getInstance().close();
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
    FSFactoryProducer.getFSFactory().deleteDirectory(dir);
  }

  /** disable memory control</br> this function should be called before all code in the setup */
  public static void envSetUp() {
    logger.debug("EnvironmentUtil setup...");
    config.setThriftServerAwaitTimeForStopService(60);

    createAllDir();

    try {
      StorageEngine.getInstance().start();

      SchemaEngine.getInstance().init();

      CompactionTaskManager.getInstance().start();
      WALManager.getInstance().start();
      FlushManager.getInstance().start();
    } catch (StartupException e) {
      throw new RuntimeException(e);
    }

    TEST_QUERY_JOB_ID = QueryResourceManager.getInstance().assignQueryId();
    TEST_QUERY_CONTEXT = new QueryContext(TEST_QUERY_JOB_ID, false);
  }

  private static void createAllDir() {
    // create sequential files
    for (String path : tierManager.getAllLocalSequenceFileFolders()) {
      createDir(path);
    }
    // create unsequential files
    for (String path : tierManager.getAllLocalUnSequenceFileFolders()) {
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
  }

  private static void createDir(String dir) {
    File file = FSFactoryProducer.getFSFactory().getFile(dir);
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
