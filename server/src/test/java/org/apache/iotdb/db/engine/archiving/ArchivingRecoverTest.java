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

package org.apache.iotdb.db.engine.archiving;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.StorageGroupProcessorException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.query.LogicalOperatorException;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.db.utils.FileUtils;
import org.apache.iotdb.tsfile.utils.FilePathUtils;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ArchivingRecoverTest {
  private static IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private static final File ARCHIVING_LOG_DIR =
      SystemFileFactory.INSTANCE.getFile(
          Paths.get(
                  FilePathUtils.regularizePath(config.getSystemDir()),
                  IoTDBConstant.ARCHIVING_FOLDER_NAME,
                  IoTDBConstant.ARCHIVING_LOG_FOLDER_NAME)
              .toString());
  private long testTaskId = 99;
  private File testLogFile;
  private List<File> testFiles;
  private File testTargetDir;

  @Before
  public void setUp()
      throws MetadataException, StorageGroupProcessorException, LogicalOperatorException {
    EnvironmentUtils.envSetUp();

    testLogFile = SystemFileFactory.INSTANCE.getFile(ARCHIVING_LOG_DIR, testTaskId + ".log");

    testTargetDir = new File("testTargetDir");
    testTargetDir.mkdirs();

    testFiles = new ArrayList<>();
    testFiles.add(new File("test.tsfile"));
    testFiles.add(new File("test.tsfile.resource"));
    testFiles.add(new File("test.tsfile.mods"));
  }

  @After
  public void tearDown() throws IOException, StorageEngineException {
    EnvironmentUtils.cleanEnv();

    FileUtils.deleteDirectory(testTargetDir);
    deleteTestFiles();
  }

  private void setupTestFiles() throws IOException {
    for (File file : testFiles) {
      if (!file.exists()) {
        file.createNewFile();
      }
    }
  }

  private void deleteTestFiles() {
    for (File file : testFiles) {
      if (file.exists()) {
        file.delete();
      }
    }
  }

  private void cleanupTargetDir() {
    for (File file : testTargetDir.listFiles()) {
      file.delete();
    }
  }

  private File getTsFile() {
    return testFiles.get(0);
  }

  @Test
  public void testWriteAndRead() throws Exception {
    // create test files
    setupTestFiles();

    // test write
    ArchivingTask task = new ArchivingTask(testTaskId, null, testTargetDir, 0, 0);
    task.startTask();
    task.startFile(getTsFile());
    task.close();

    FileInputStream fileInputStream = new FileInputStream(testLogFile);

    assertTrue(testLogFile.exists());
    assertEquals(testTargetDir.getAbsolutePath(), ReadWriteIOUtils.readString(fileInputStream));
    assertEquals(getTsFile().getAbsolutePath(), ReadWriteIOUtils.readString(fileInputStream));
    assertEquals(0, fileInputStream.available());

    fileInputStream.close();

    // test read
    ArchivingRecover recover = new ArchivingRecover();
    recover.recover();

    for (File file : testFiles) {
      assertFalse(file.exists());
    }

    assertFalse(testLogFile.exists());

    assertEquals(3, testTargetDir.listFiles().length);
    cleanupTargetDir();
  }

  @Test
  public void testMissingFiles() throws IOException {
    // test missing .tsfile .resource .mods
    File missingTsFile = new File("testMissing.tsfile");
    File missingTsFileRes = new File("testMissing.tsfile.resource");
    File missingTsFileMods = new File("testMissing.tsfile.mods");

    assertFalse(missingTsFile.exists());
    assertFalse(missingTsFileRes.exists());
    assertFalse(missingTsFileMods.exists());

    testLogFile.createNewFile();

    FileOutputStream logOutput = new FileOutputStream(testLogFile);

    ReadWriteIOUtils.write(testTargetDir.getAbsolutePath(), logOutput);
    ReadWriteIOUtils.write(missingTsFile.getAbsolutePath(), logOutput);

    logOutput.close();

    // test read
    ArchivingRecover recover = new ArchivingRecover();
    recover.recover();

    assertEquals(0, testTargetDir.listFiles().length);
  }
}
