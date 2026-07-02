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
import org.apache.iotdb.commons.udf.service.UDFManagementService;
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
import org.apache.iotdb.db.storageengine.dataregion.memtable.DeviceIDFactory;
import org.apache.iotdb.db.storageengine.dataregion.read.control.FileReaderManager;
import org.apache.iotdb.db.storageengine.dataregion.read.control.QueryResourceManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.timeindex.FileTimeIndexCacheRecorder;
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
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.fail;

/** This class is used for cleaning test environment in unit test and integration test */
public class EnvironmentUtils {

  private static final Logger logger = LoggerFactory.getLogger(EnvironmentUtils.class);

  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private static final CommonConfig commonConfig = CommonDescriptor.getInstance().getConfig();
  private static final TierManager tierManager = TierManager.getInstance();
  private static final int DELETE_RETRY_TIMES = 5;
  private static final long DELETE_RETRY_INTERVAL_MS = 100;

  public static long TEST_QUERY_JOB_ID = 1;
  public static QueryContext TEST_QUERY_CONTEXT = new QueryContext(TEST_QUERY_JOB_ID);
  public static FragmentInstanceContext TEST_QUERY_FI_CONTEXT =
      FragmentInstanceContext.createFragmentInstanceContextForCompaction(TEST_QUERY_JOB_ID);

  private static final long oldSeqTsFileSize = config.getSeqTsFileSize();
  private static final long oldUnSeqTsFileSize = config.getUnSeqTsFileSize();
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
    TTransport transport = TSocketWrapper.wrap(tConfiguration, "127.0.0.1", 6667, 100);
    if (transport != null && !transport.isOpen()) {
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
    if (transport != null && !transport.isOpen()) {
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
    for (String path : tierManager.getAllLocalSequenceFileFolders()) {
      cleanDir(path);
    }
    // delete unsequence files
    for (String path : tierManager.getAllLocalUnSequenceFileFolders()) {
      cleanDir(path);
    }
    FileTimeIndexCacheRecorder.getInstance().close();
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
    Path path = FSFactoryProducer.getFSFactory().getFile(dir).toPath();
    if (!Files.exists(path)) {
      return;
    }

    IOException lastException = null;
    for (int i = 0; i < DELETE_RETRY_TIMES; i++) {
      try {
        deleteRecursively(path);
        return;
      } catch (NoSuchFileException e) {
        return;
      } catch (IOException e) {
        lastException = e;
        if (i + 1 == DELETE_RETRY_TIMES) {
          break;
        }
        try {
          TimeUnit.MILLISECONDS.sleep(DELETE_RETRY_INTERVAL_MS);
        } catch (InterruptedException interruptedException) {
          Thread.currentThread().interrupt();
          IOException ioException =
              new IOException("Interrupted while deleting " + dir, interruptedException);
          ioException.addSuppressed(e);
          throw ioException;
        }
      }
    }
    throw lastException;
  }

  private static void deleteRecursively(Path path) throws IOException {
    if (!Files.exists(path)) {
      return;
    }

    Files.walkFileTree(
        path,
        new SimpleFileVisitor<Path>() {
          @Override
          public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
              throws IOException {
            Files.deleteIfExists(file);
            return FileVisitResult.CONTINUE;
          }

          @Override
          public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
            if (exc instanceof NoSuchFileException) {
              return FileVisitResult.CONTINUE;
            }
            throw exc;
          }

          @Override
          public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
            if (exc != null) {
              throw exc;
            }
            Files.deleteIfExists(dir);
            return FileVisitResult.CONTINUE;
          }
        });
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

    // reset id method
    DeviceIDFactory.getInstance().reset();

    TEST_QUERY_JOB_ID = QueryResourceManager.getInstance().assignQueryId();
    TEST_QUERY_CONTEXT = new QueryContext(TEST_QUERY_JOB_ID);
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
    cleanDir(path);
  }
}
