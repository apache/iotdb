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
package org.apache.iotdb.commons.sync.metedata;

import org.apache.iotdb.commons.exception.sync.PipeAlreadyExistException;
import org.apache.iotdb.commons.exception.sync.PipeException;
import org.apache.iotdb.commons.exception.sync.PipeNotExistException;
import org.apache.iotdb.commons.exception.sync.PipeSinkAlreadyExistException;
import org.apache.iotdb.commons.exception.sync.PipeSinkBeingUsedException;
import org.apache.iotdb.commons.exception.sync.PipeSinkException;
import org.apache.iotdb.commons.exception.sync.PipeSinkNotExistException;
import org.apache.iotdb.commons.sync.metadata.SyncMetadata;
import org.apache.iotdb.commons.sync.pipe.PipeInfo;
import org.apache.iotdb.commons.sync.pipe.PipeMessage;
import org.apache.iotdb.commons.sync.pipe.PipeStatus;
import org.apache.iotdb.commons.sync.pipe.TsFilePipeInfo;
import org.apache.iotdb.commons.sync.pipesink.IoTDBPipeSink;
import org.apache.iotdb.commons.sync.pipesink.PipeSink;

import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;

public class SyncMetadataTest {
  private static final File snapshotDir = new File("target", "snapshot");

  private static final String PIPESINK_NAME_1 = "pipeSink1";
  private static final String PIPESINK_NAME_2 = "pipeSink2";
  private static PipeSink ioTDBPipeSink1;
  private static PipeSink ioTDBPipeSink2;

  private static final String PIPE_NAME_1 = "pipe1";
  private static final String PIPE_NAME_2 = "pipe2";
  private static PipeInfo pipeInfo1;
  private static PipeInfo pipeInfo2;

  @BeforeClass
  public static void setUp() {
    ioTDBPipeSink1 = new IoTDBPipeSink(PIPESINK_NAME_1);
    ioTDBPipeSink2 = new IoTDBPipeSink(PIPESINK_NAME_2);
    pipeInfo1 =
        new TsFilePipeInfo(PIPE_NAME_1, PIPESINK_NAME_1, System.currentTimeMillis(), 0, true);
    pipeInfo2 =
        new TsFilePipeInfo(PIPE_NAME_2, PIPESINK_NAME_2, System.currentTimeMillis(), 99, false);
  }

  @Test
  public void testPipeSinkOperation() {
    SyncMetadata syncMetadata = new SyncMetadata();
    Assert.assertFalse(syncMetadata.isPipeSinkExist(PIPESINK_NAME_1));
    // check and add ioTDBPipeSink1
    try {
      syncMetadata.checkPipeSinkNoExist(PIPESINK_NAME_1);
    } catch (PipeSinkException e) {
      Assert.fail(e.getMessage());
    }
    syncMetadata.addPipeSink(ioTDBPipeSink1);
    Assert.assertTrue(syncMetadata.isPipeSinkExist(PIPESINK_NAME_1));
    try {
      syncMetadata.checkPipeSinkNoExist(PIPESINK_NAME_1);
      Assert.fail();
    } catch (PipeSinkException e) {
      Assert.assertTrue(e instanceof PipeSinkAlreadyExistException);
    }
    // add ioTDBPipeSink2
    try {
      syncMetadata.checkDropPipeSink(PIPESINK_NAME_2);
      Assert.fail();
    } catch (PipeSinkException e) {
      Assert.assertTrue(e instanceof PipeSinkNotExistException);
    }
    syncMetadata.addPipeSink(ioTDBPipeSink2);
    // get PipeSink
    Assert.assertEquals(ioTDBPipeSink1, syncMetadata.getPipeSink(PIPESINK_NAME_1));
    Assert.assertEquals(ioTDBPipeSink2, syncMetadata.getPipeSink(PIPESINK_NAME_2));
    Assert.assertEquals(2, syncMetadata.getAllPipeSink().size());
    // drop ioTDBPipeSink2
    syncMetadata.addPipe(pipeInfo2);
    try {
      syncMetadata.checkDropPipeSink(PIPESINK_NAME_2);
      Assert.fail();
    } catch (PipeSinkException e) {
      Assert.assertTrue(e instanceof PipeSinkBeingUsedException);
    }
    syncMetadata.dropPipe(PIPE_NAME_2);
    try {
      syncMetadata.checkDropPipeSink(PIPESINK_NAME_2);
    } catch (PipeSinkException e) {
      Assert.fail(e.getMessage());
    }
    syncMetadata.dropPipeSink(PIPESINK_NAME_2);
    Assert.assertNull(syncMetadata.getPipeSink(PIPESINK_NAME_2));
    Assert.assertEquals(1, syncMetadata.getAllPipeSink().size());
  }

  @Test
  public void testPipeOperation() {
    SyncMetadata syncMetadata = new SyncMetadata();
    // check add pipe
    try {
      syncMetadata.checkAddPipe(pipeInfo1);
      Assert.fail();
    } catch (Exception e) {
      Assert.assertTrue(e instanceof PipeSinkNotExistException);
    }
    try {
      syncMetadata.checkIfPipeExist(PIPE_NAME_1);
      Assert.fail();
    } catch (PipeException e) {
      Assert.assertTrue(e instanceof PipeNotExistException);
    }
    syncMetadata.addPipeSink(ioTDBPipeSink1);
    syncMetadata.addPipeSink(ioTDBPipeSink2);
    try {
      syncMetadata.checkAddPipe(pipeInfo1);
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
    syncMetadata.addPipe(pipeInfo1);
    try {
      syncMetadata.checkAddPipe(
          new TsFilePipeInfo(PIPE_NAME_1, PIPESINK_NAME_2, System.currentTimeMillis(), 99, false));
      Assert.fail();
    } catch (Exception e) {
      Assert.assertTrue(e instanceof PipeAlreadyExistException);
      Assert.assertTrue(e.getMessage().contains("please retry after drop it"));
    }
    syncMetadata.addPipe(pipeInfo2);
    // check get pipe
    Assert.assertEquals(pipeInfo1, syncMetadata.getPipeInfo(PIPE_NAME_1));
    Assert.assertEquals(pipeInfo2, syncMetadata.getPipeInfo(PIPE_NAME_2));
    Assert.assertEquals(2, syncMetadata.getAllPipeInfos().size());
    // check setter
    syncMetadata.setPipeStatus(PIPE_NAME_1, PipeStatus.RUNNING);
    syncMetadata.changePipeMessage(PIPE_NAME_1, PipeMessage.PipeMessageType.WARN);
    Assert.assertEquals(PipeStatus.RUNNING, syncMetadata.getPipeInfo(PIPE_NAME_1).getStatus());
    Assert.assertEquals(
        PipeMessage.PipeMessageType.WARN, syncMetadata.getPipeInfo(PIPE_NAME_1).getMessageType());
    // check drop
    syncMetadata.dropPipe(PIPE_NAME_1);
    Assert.assertNull(syncMetadata.getPipeInfo(PIPE_NAME_1));
    Assert.assertEquals(1, syncMetadata.getAllPipeInfos().size());
  }

  @Test
  public void testSnapshot() throws Exception {
    try {
      if (snapshotDir.exists()) {
        FileUtils.deleteDirectory(snapshotDir);
      }
      snapshotDir.mkdirs();
      SyncMetadata syncMetadata = new SyncMetadata();
      syncMetadata.addPipeSink(ioTDBPipeSink1);
      syncMetadata.addPipeSink(ioTDBPipeSink2);
      syncMetadata.addPipe(pipeInfo1);
      syncMetadata.addPipe(pipeInfo2);
      syncMetadata.processTakeSnapshot(snapshotDir);
      SyncMetadata syncMetadata1 = new SyncMetadata();
      syncMetadata1.processLoadSnapshot(snapshotDir);
      Assert.assertEquals(pipeInfo1, syncMetadata.getPipeInfo(PIPE_NAME_1));
      Assert.assertEquals(pipeInfo2, syncMetadata.getPipeInfo(PIPE_NAME_2));
      Assert.assertEquals(ioTDBPipeSink1, syncMetadata.getPipeSink(PIPESINK_NAME_1));
      Assert.assertEquals(ioTDBPipeSink2, syncMetadata.getPipeSink(PIPESINK_NAME_2));
    } finally {
      if (snapshotDir.exists()) {
        FileUtils.deleteDirectory(snapshotDir);
      }
    }
  }
}
