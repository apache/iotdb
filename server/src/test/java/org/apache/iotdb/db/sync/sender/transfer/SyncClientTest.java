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
package org.apache.iotdb.db.sync.sender.transfer;

import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.directories.DirectoryManager;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.DiskSpaceInsufficientException;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.sync.conf.SyncConstant;
import org.apache.iotdb.db.sync.conf.SyncSenderConfig;
import org.apache.iotdb.db.sync.conf.SyncSenderDescriptor;
import org.apache.iotdb.db.sync.receiver.transfer.SyncServiceImpl;
import org.apache.iotdb.db.sync.sender.recover.ISyncSenderLogAnalyzer;
import org.apache.iotdb.db.sync.sender.recover.ISyncSenderLogger;
import org.apache.iotdb.db.sync.sender.recover.SyncSenderLogAnalyzer;
import org.apache.iotdb.db.sync.sender.recover.SyncSenderLogger;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.service.sync.thrift.ConfirmInfo;
import org.apache.iotdb.service.sync.thrift.SyncService;
import org.apache.iotdb.service.sync.thrift.SyncStatus;
import org.apache.iotdb.tsfile.utils.FilePathUtils;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.Map.Entry;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SyncClientTest {

  private static final Logger logger = LoggerFactory.getLogger(SyncClientTest.class);
  private ISyncClient manager = SyncClient.getInstance();
  private SyncSenderConfig config = SyncSenderDescriptor.getInstance().getConfig();
  private String dataDir;
  private ISyncSenderLogAnalyzer senderLogAnalyzer;

  private String tsfileDataDir;
  private SyncService.Client serviceClient;
  private Map<String, Map<Long, Set<Long>>> allSG = new HashMap<>();
  private Map<String, Map<Long, Map<Long, Set<File>>>> toBeSyncedFilesMap = new HashMap<>();
  private Map<String, Map<Long, Map<Long, Set<File>>>> deletedFilesMap = new HashMap<>();
  private Map<String, Map<Long, Map<Long, Set<File>>>> lastLocalFilesMap = new HashMap<>();
  private ISyncSenderLogger syncLog;
  private Map<String, List<String>> allFiles = new HashMap<>();

  private String sg1 = "root.sg1";
  private String sg2 = "root.sg_2";

  @Before
  public void setUp() throws DiskSpaceInsufficientException {
    EnvironmentUtils.envSetUp();
    dataDir =
        tsfileDataDir =
            new File(DirectoryManager.getInstance().getNextFolderForSequenceFile())
                .getParentFile()
                .getAbsolutePath();
    config.update(dataDir);
    senderLogAnalyzer = new SyncSenderLogAnalyzer(config.getSenderFolderPath());
  }

  @After
  public void tearDown() throws IOException, StorageEngineException {
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void makeFileSnapshot() throws IOException {
    Map<String, Map<Long, Map<Long, Set<File>>>> allFileList = new HashMap<>();

    Random r = new Random(0);
    for (int i = 0; i < 3; i++) {
      for (int j = 0; j < 5; j++) {
        if (!allFileList.containsKey(String.valueOf(i))) {
          allFileList
              .computeIfAbsent(String.valueOf(i), k -> new HashMap<>())
              .computeIfAbsent(0L, k -> new HashMap<>())
              .computeIfAbsent(0L, k -> new HashSet<>());
        }
        String rand = String.valueOf(r.nextInt(10000));
        String fileName =
            FilePathUtils.regularizePath(dataDir)
                + IoTDBConstant.SEQUENCE_FLODER_NAME
                + File.separator
                + i
                + File.separator
                + "0"
                + File.separator
                + "0"
                + File.separator
                + rand;
        File file = new File(fileName);
        allFileList.get(String.valueOf(i)).get(0L).get(0L).add(file);
        if (!file.getParentFile().exists()) {
          file.getParentFile().mkdirs();
        }
        if (!file.exists() && !file.createNewFile()) {
          logger.error("Can not create new file {}", file.getPath());
        }
        if (!new File(file.getAbsolutePath() + TsFileResource.RESOURCE_SUFFIX).exists()
            && !new File(file.getAbsolutePath() + TsFileResource.RESOURCE_SUFFIX).createNewFile()) {
          logger.error("Can not create new file {}", file.getPath());
        }
      }
    }

    Map<String, Set<String>> dataFileMap = new HashMap<>();
    File sequenceFile = new File(dataDir, IoTDBConstant.SEQUENCE_FLODER_NAME);
    for (File sgFile : sequenceFile.listFiles()) {
      dataFileMap.putIfAbsent(sgFile.getName(), new HashSet<>());
      for (File vgFile : sgFile.listFiles()) {
        for (File trFile : vgFile.listFiles()) {
          for (File tsfile : trFile.listFiles()) {
            if (!tsfile.getName().endsWith(TsFileResource.RESOURCE_SUFFIX)) {
              ((SyncClient) manager).makeFileSnapshot(tsfile);
            }
            dataFileMap.get(sgFile.getName()).add(tsfile.getName());
          }
        }
      }
    }

    assertTrue(new File(config.getSenderFolderPath()).exists());
    assertTrue(new File(config.getSnapshotPath()).exists());

    Map<String, Set<String>> snapFileMap = new HashMap<>();
    for (File sgFile : new File(config.getSnapshotPath()).listFiles()) {
      snapFileMap.putIfAbsent(sgFile.getName(), new HashSet<>());
      for (File vgFile : sgFile.listFiles()) {
        for (File trFile : vgFile.listFiles()) {
          for (File snapshotTsfile : trFile.listFiles()) {
            snapFileMap.get(sgFile.getName()).add(snapshotTsfile.getName());
          }
        }
      }
    }

    assertEquals(dataFileMap.size(), snapFileMap.size());
    for (Entry<String, Set<String>> entry : dataFileMap.entrySet()) {
      String sg = entry.getKey();
      Set<String> tsfiles = entry.getValue();
      assertTrue(snapFileMap.containsKey(sg));
      assertEquals(snapFileMap.get(sg).size(), tsfiles.size());
      assertTrue(snapFileMap.get(sg).containsAll(tsfiles));
    }

    assertFalse(new File(config.getLastFileInfoPath()).exists());
    senderLogAnalyzer.recover();
    assertFalse(new File(config.getSnapshotPath()).exists());
    assertTrue(new File(config.getLastFileInfoPath()).exists());
  }

  private void prepareMap(Map<String, Map<Long, Map<Long, Set<File>>>> map) {
    map.put(sg1, new HashMap<>());
    map.put(sg2, new HashMap<>());
    map.get(sg1).put(0l, new HashMap<>());
    map.get(sg2).put(0l, new HashMap<>());
    map.get(sg1).get(0l).put(0l, new HashSet<>());
    map.get(sg2).get(0l).put(0l, new HashSet<>());
  }

  private File getTsFile(String sgName, Integer vg, Integer partition, String tsFileName) {
    String fileName =
        tsfileDataDir
            + File.separator
            + sgName
            + File.separator
            + vg
            + File.separator
            + partition
            + File.separator
            + tsFileName;
    File file = new File(fileName);
    File resource = new File(fileName + ".resource");
    try {
      file.createNewFile();
      resource.createNewFile();
    } catch (IOException e) {
      logger.warn("Can not create tsfile: ", file.getPath());
    }
    return file;
  }

  private void prepare() throws Exception {
    serviceClient = new MockSyncClient(null);
    syncLog = new SyncSenderLogger(new File(dataDir + "sync.log"));

    allSG.put(sg1, new HashMap<>());
    allSG.put(sg2, new HashMap<>());
    allSG.get(sg1).put(0l, new HashSet<>());
    allSG.get(sg2).put(0l, new HashSet<>());
    allSG.get(sg1).get(0l).add(0l);
    allSG.get(sg2).get(0l).add(0l);

    prepareMap(toBeSyncedFilesMap);
    prepareMap(deletedFilesMap);
    prepareMap(lastLocalFilesMap);

    lastLocalFilesMap.get(sg1).get(0l).get(0l).add(getTsFile(sg1, 0, 0, "1-1-0-0.tsfile"));
    lastLocalFilesMap.get(sg1).get(0l).get(0l).add(getTsFile(sg1, 0, 0, "2-2-0-0.tsfile"));
    lastLocalFilesMap.get(sg1).get(0l).get(0l).add(getTsFile(sg1, 0, 0, "3-3-0-0.tsfile"));
    lastLocalFilesMap.get(sg1).get(0l).get(0l).add(getTsFile(sg1, 0, 0, "4-4-0-0.tsfile"));
    lastLocalFilesMap.get(sg2).get(0l).get(0l).add(getTsFile(sg2, 0, 0, "1-1-0-0.tsfile"));

    deletedFilesMap.get(sg1).get(0l).get(0l).add(getTsFile(sg1, 0, 0, "1-1-0-0.tsfile"));
    deletedFilesMap.get(sg1).get(0l).get(0l).add(getTsFile(sg1, 0, 0, "2-2-0-0.tsfile"));
    deletedFilesMap.get(sg2).get(0l).get(0l).add(getTsFile(sg2, 0, 0, "1-1-0-0.tsfile"));

    toBeSyncedFilesMap.get(sg1).get(0l).get(0l).add(getTsFile(sg1, 0, 0, "5-5-0-0.tsfile"));
    toBeSyncedFilesMap.get(sg1).get(0l).get(0l).add(getTsFile(sg1, 0, 0, "6-6-0-0.tsfile"));
    toBeSyncedFilesMap.get(sg2).get(0l).get(0l).add(getTsFile(sg2, 0, 0, "2-2-0-0.tsfile"));

    allFiles.put(sg1, new ArrayList<>());
    allFiles.put(sg2, new ArrayList<>());
    for (int i = 1; i <= 6; i++) {
      allFiles.get(sg1).add(getTsFile(sg1, 0, 0, i + "-" + i + "-0-0.tsfile").getPath());
    }
    for (int i = 1; i <= 2; i++) {
      allFiles.get(sg2).add(getTsFile(sg2, 0, 0, i + "-" + i + "-0-0.tsfile").getPath());
    }

    ((SyncClient) manager)
        .setContent(
            serviceClient,
            true,
            allSG,
            toBeSyncedFilesMap,
            deletedFilesMap,
            lastLocalFilesMap,
            syncLog);
  }

  @Test
  public void testGetFileInfoWithVgAndPartition() {
    try {
      prepare();
      List<String> serviceClientStrings;
      SyncServiceImpl syncServiceImpl = new SyncServiceImpl();

      manager.syncDeletedFilesNameInOneGroup(sg1, 0l, 0l, deletedFilesMap.get(sg1).get(0l).get(0l));
      manager.syncDeletedFilesNameInOneGroup(sg2, 0l, 0l, deletedFilesMap.get(sg2).get(0l).get(0l));

      manager.syncDataFilesInOneGroup(sg1, 0l, 0l, toBeSyncedFilesMap.get(sg1).get(0l).get(0l));
      manager.syncDataFilesInOneGroup(sg2, 0l, 0l, toBeSyncedFilesMap.get(sg2).get(0l).get(0l));

      serviceClientStrings = ((MockSyncClient) serviceClient).getString();

      for (String string : serviceClientStrings) {
        string = syncServiceImpl.getFilePathByFileInfo(string);
        if (string.endsWith(".tsfile")) {
          boolean in = false;
          for (String path : allFiles.get(sg1)) {
            if (path.contains(string)) in = true;
          }
          for (String path : allFiles.get(sg2)) {
            if (path.contains(string)) in = true;
          }
          assertTrue(in);
        }
      }
    } catch (Exception e) {
      Assert.fail();
    }
  }

  static class MockSyncClient extends SyncService.Client {
    boolean isConnected = true;
    boolean isError = false;

    List<String> strings = new ArrayList<>();
    List<ByteBuffer> byteBuffers = new ArrayList<>();

    public void setConnected(boolean connected) {
      isConnected = connected;
    }

    public void setError(boolean error) {
      isError = error;
    }

    public List<String> getString() {
      return strings;
    }

    public void setStrings(List<String> strings) {
      this.strings = strings;
    }

    public List<ByteBuffer> getByteBuffer() {
      return byteBuffers;
    }

    public void setByteBuffers(List<ByteBuffer> byteBuffers) {
      this.byteBuffers = byteBuffers;
    }

    public MockSyncClient(TProtocol prot) {
      super(prot);
    }

    private SyncStatus getSuccessStatus() {
      return new SyncStatus(SyncConstant.SUCCESS_CODE, "");
    }

    private SyncStatus getSuccessStatus(String msg) {
      return new SyncStatus(SyncConstant.SUCCESS_CODE, msg);
    }

    private SyncStatus getErrorStatus() {
      return new SyncStatus(SyncConstant.ERROR_CODE, "");
    }

    private SyncStatus getErrorStatus(String msg) {
      return new SyncStatus(SyncConstant.ERROR_CODE, msg);
    }

    private SyncStatus getMockSyncStatus() throws TException {
      if (!isConnected) throw new TException("Read time out");
      return isError ? getErrorStatus() : getSuccessStatus();
    }

    private SyncStatus getMockSyncStatus(String msg) throws TException {
      if (!isConnected) throw new TException("Read time out");
      return isError ? getErrorStatus(msg) : getSuccessStatus(msg);
    }

    @Override
    public SyncStatus check(ConfirmInfo info) throws TException {
      return getMockSyncStatus();
    }

    @Override
    public SyncStatus startSync() throws TException {
      return getMockSyncStatus();
    }

    @Override
    public SyncStatus init(String storageGroupName) throws TException {
      strings.add(storageGroupName);
      return getMockSyncStatus();
    }

    @Override
    public SyncStatus syncDeletedFileName(String fileName) throws TException {
      strings.add(fileName);
      return getMockSyncStatus();
    }

    @Override
    public SyncStatus initSyncData(String filename) throws TException {
      strings.add(filename);
      return getMockSyncStatus();
    }

    @Override
    public SyncStatus syncData(ByteBuffer buff) throws TException {
      byteBuffers.add(buff);
      return getMockSyncStatus();
    }

    @Override
    public SyncStatus checkDataDigest(String md5) throws TException {
      strings.add(md5);
      return getMockSyncStatus(md5);
    }

    @Override
    public SyncStatus endSync() throws TException {
      return getMockSyncStatus();
    }
  }
}
