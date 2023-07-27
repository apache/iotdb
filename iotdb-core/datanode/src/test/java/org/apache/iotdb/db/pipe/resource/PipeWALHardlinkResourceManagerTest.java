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

package org.apache.iotdb.db.pipe.resource;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.db.pipe.resource.wal.hardlink.PipeWALHardlinkResourceManager;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

public class PipeWALHardlinkResourceManagerTest {
  private static final String ROOT_DIR = "target" + File.separator + "PipeWALHolderTest";

  private static final String WAL_DIR = ROOT_DIR + File.separator + IoTDBConstant.WAL_FOLDER_NAME;

  private static final String WAL_NAME = WAL_DIR + File.separator + "test.wal";

  private PipeWALHardlinkResourceManager pipeWALHardlinkResourceManager;

  @Before
  public void setUp() throws Exception {
    pipeWALHardlinkResourceManager = new PipeWALHardlinkResourceManager();

    createWAL();
  }

  private void createWAL() {
    File file = new File(WAL_NAME);
    if (file.exists()) {
      boolean ignored = file.delete();
    }

    try {
      file.getParentFile().mkdirs();
      file.createNewFile();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @After
  public void tearDown() throws Exception {
    File pipeFolder = new File(ROOT_DIR);
    if (pipeFolder.exists()) {
      FileUtils.deleteDirectory(pipeFolder);
    }
  }

  @Test
  public void testIncreaseTsfile() throws IOException {
    File originWALFile = new File(WAL_NAME);
    Assert.assertEquals(0, pipeWALHardlinkResourceManager.getFileReferenceCount(originWALFile));

    File pipeWALFile = pipeWALHardlinkResourceManager.increaseFileReference(originWALFile);
    Assert.assertEquals(1, pipeWALHardlinkResourceManager.getFileReferenceCount(pipeWALFile));
    Assert.assertTrue(Files.exists(originWALFile.toPath()));
    Assert.assertTrue(Files.exists(pipeWALFile.toPath()));

    // test use hardlinkTsFile to increase reference counts
    pipeWALHardlinkResourceManager.increaseFileReference(pipeWALFile);
    Assert.assertEquals(2, pipeWALHardlinkResourceManager.getFileReferenceCount(pipeWALFile));
    Assert.assertTrue(Files.exists(originWALFile.toPath()));
    Assert.assertTrue(Files.exists(pipeWALFile.toPath()));
  }

  @Test
  public void testDecreaseTsfile() throws IOException {
    File originFile = new File(WAL_NAME);

    pipeWALHardlinkResourceManager.decreaseFileReference(originFile);
    Assert.assertEquals(0, pipeWALHardlinkResourceManager.getFileReferenceCount(originFile));

    File pipeWALFile = pipeWALHardlinkResourceManager.increaseFileReference(originFile);
    Assert.assertEquals(1, pipeWALHardlinkResourceManager.getFileReferenceCount(pipeWALFile));
    Assert.assertTrue(Files.exists(pipeWALFile.toPath()));
    Assert.assertTrue(Files.exists(pipeWALFile.toPath()));

    Assert.assertTrue(originFile.delete());
    Assert.assertFalse(Files.exists(originFile.toPath()));

    Assert.assertEquals(1, pipeWALHardlinkResourceManager.getFileReferenceCount(pipeWALFile));
    Assert.assertFalse(Files.exists(originFile.toPath()));
    Assert.assertTrue(Files.exists(pipeWALFile.toPath()));

    pipeWALHardlinkResourceManager.decreaseFileReference(pipeWALFile);
    Assert.assertEquals(0, pipeWALHardlinkResourceManager.getFileReferenceCount(pipeWALFile));
    Assert.assertFalse(Files.exists(originFile.toPath()));
    Assert.assertFalse(Files.exists(pipeWALFile.toPath()));
  }
}
