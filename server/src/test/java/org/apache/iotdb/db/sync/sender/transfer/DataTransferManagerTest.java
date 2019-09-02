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
package org.apache.iotdb.db.sync.sender.transfer;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.directories.DirectoryManager;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.DiskSpaceInsufficientException;
import org.apache.iotdb.db.exception.StartupException;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.sync.sender.conf.SyncSenderConfig;
import org.apache.iotdb.db.sync.sender.conf.SyncSenderDescriptor;
import org.apache.iotdb.db.sync.sender.recover.SyncSenderLogAnalyzer;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.db.utils.FilePathUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataTransferManagerTest {

  private static final Logger logger = LoggerFactory.getLogger(DataTransferManagerTest.class);
  private DataTransferManager manager = DataTransferManager.getInstance();
  private SyncSenderConfig config = SyncSenderDescriptor.getInstance().getConfig();
  private String dataDir;
  private SyncSenderLogAnalyzer senderLogAnalyzer;

  @Before
  public void setUp()
      throws IOException, InterruptedException, StartupException, DiskSpaceInsufficientException {
    EnvironmentUtils.envSetUp();
    dataDir = new File(DirectoryManager.getInstance().getNextFolderForSequenceFile())
        .getParentFile().getAbsolutePath();
    config.update(dataDir);
    senderLogAnalyzer = new SyncSenderLogAnalyzer(config.getSenderFolderPath());
  }

  @After
  public void tearDown() throws InterruptedException, IOException, StorageEngineException {
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void makeFileSnapshot() throws IOException {
    Map<String, Set<File>> allFileList = new HashMap<>();

    Random r = new Random(0);
    for (int i = 0; i < 3; i++) {
      for (int j = 0; j < 5; j++) {
        if (!allFileList.containsKey(String.valueOf(i))) {
          allFileList.put(String.valueOf(i), new HashSet<>());
        }
        String rand = String.valueOf(r.nextInt(10000));
        String fileName = FilePathUtils.regularizePath(dataDir) + IoTDBConstant.SEQUENCE_FLODER_NAME
            + File.separator + i
            + File.separator + rand;
        File file = new File(fileName);
        allFileList.get(String.valueOf(i)).add(file);
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
    for(File sgFile: sequenceFile.listFiles()){
      dataFileMap.putIfAbsent(sgFile.getName(), new HashSet<>());
      for (File tsfile : sgFile.listFiles()) {
        if (!tsfile.getName().endsWith(TsFileResource.RESOURCE_SUFFIX)) {
          manager.makeFileSnapshot(tsfile);
        }
        dataFileMap.get(sgFile.getName()).add(tsfile.getName());
      }
    }

    assert new File(config.getSenderFolderPath()).exists();
    assert new File(config.getSnapshotPath()).exists();

    Map<String, Set<String>> snapFileMap = new HashMap<>();
    for(File sgFile: new File(config.getSnapshotPath()).listFiles()){
      snapFileMap.putIfAbsent(sgFile.getName(), new HashSet<>());
      for(File snapshotTsfile: sgFile.listFiles()){
        snapFileMap.get(sgFile.getName()).add(snapshotTsfile.getName());
      }
    }

    assert dataFileMap.size() == snapFileMap.size();
    for(Entry<String, Set<String>> entry: dataFileMap.entrySet()){
      String sg = entry.getKey();
      Set<String> tsfiles = entry.getValue();
      assert snapFileMap.containsKey(sg);
      assert snapFileMap.get(sg).size() == tsfiles.size();
      assert snapFileMap.get(sg).containsAll(tsfiles);
    }

    assert !new File(config.getLastFileInfo()).exists();
    senderLogAnalyzer.recover();
    assert !new File(config.getSnapshotPath()).exists();
    assert new File(config.getLastFileInfo()).exists();
  }
}