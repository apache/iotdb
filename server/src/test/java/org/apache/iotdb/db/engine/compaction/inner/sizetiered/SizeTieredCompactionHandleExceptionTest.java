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

package org.apache.iotdb.db.engine.compaction.inner.sizetiered;

import org.apache.iotdb.db.engine.compaction.inner.AbstractInnerSpaceCompactionTest;
import org.apache.iotdb.db.engine.storagegroup.TsFileNameGenerator;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;

public class SizeTieredCompactionHandleExceptionTest extends AbstractInnerSpaceCompactionTest {
  @Before
  public void setUp() throws IOException, MetadataException, WriteProcessException {
    this.seqFileNum = 10;
    super.setUp();
  }

  @After
  public void tearDown() throws StorageEngineException, IOException {
    super.tearDown();
  }

  @Test
  public void testHandleExceptionTargetCompleteAndSourceExists() {
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);
    SizeTieredCompactionTask task =
        new SizeTieredCompactionTask(
            COMPACTION_TEST_SG,
            "0",
            0,
            tsFileManager,
            tsFileManager.getSequenceListByTimePartition(0),
            seqResources,
            true,
            new AtomicInteger(0));
    tsFileManager.writeLock("test");
    try {
      new Thread(
              () -> {
                try {
                  task.call();
                } catch (Exception e) {

                }
              })
          .start();
      Thread.sleep(65_000);
    } catch (Exception e) {
    } finally {
      tsFileManager.writeUnlock();
    }
    Assert.assertTrue(tsFileManager.isAllowCompaction());
    Assert.assertEquals(10, tsFileManager.getTsFileList(true).size());
  }

  @Test
  public void testHandleExceptionTargetNotCompleteAndSourceNotExists() {
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);
    SizeTieredCompactionTask task =
        new SizeTieredCompactionTask(
            COMPACTION_TEST_SG,
            "0",
            0,
            tsFileManager,
            tsFileManager.getSequenceListByTimePartition(0),
            seqResources,
            true,
            new AtomicInteger(0));
    tsFileManager.writeLock("test");
    try {
      seqResources.get(seqResources.size() - 1).remove();
      new Thread(
              () -> {
                try {
                  task.call();
                } catch (Exception e) {

                }
              })
          .start();
      Thread.sleep(5_000);
    } catch (Exception e) {
    } finally {
      tsFileManager.writeUnlock();
    }
    Assert.assertFalse(tsFileManager.isAllowCompaction());
    Assert.assertEquals(10, tsFileManager.getTsFileList(true).size());
  }

  @Test
  public void testHandleExceptionTargetCompleteAndSourceNotExists() {
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);
    SizeTieredCompactionTask task =
        new SizeTieredCompactionTask(
            COMPACTION_TEST_SG,
            "0",
            0,
            tsFileManager,
            tsFileManager.getSequenceListByTimePartition(0),
            seqResources,
            true,
            new AtomicInteger(0));
    tsFileManager.writeLock("test");
    try {
      new Thread(
              () -> {
                try {
                  task.call();
                } catch (Exception e) {

                }
              })
          .start();
      Thread.sleep(10_000);
      seqResources.get(0).remove();
      tsFileManager.getTsFileList(true).remove(seqResources.get(0));
      Thread.sleep(60_000);
    } catch (Exception e) {
    } finally {
      tsFileManager.writeUnlock();
    }
    Assert.assertTrue(tsFileManager.isAllowCompaction());
    Assert.assertEquals(1, tsFileManager.getTsFileList(true).size());
  }

  @Test
  public void testHandleExceptionTargetNotCompleteAndSourceExists() {
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);
    SizeTieredCompactionTask task =
        new SizeTieredCompactionTask(
            COMPACTION_TEST_SG,
            "0",
            0,
            tsFileManager,
            tsFileManager.getSequenceListByTimePartition(0),
            seqResources,
            true,
            new AtomicInteger(0));
    tsFileManager.writeLock("test");
    try {
      new Thread(
              () -> {
                try {
                  task.call();
                } catch (Exception e) {

                }
              })
          .start();
      Thread.sleep(10_000);
      String targetFileName =
          TsFileNameGenerator.getInnerCompactionFileName(seqResources, true).getName();
      File targetFile = new File(seqResources.get(0).getTsFile().getParent(), targetFileName);
      FileChannel channel = new FileOutputStream(targetFile, true).getChannel();
      channel.truncate(10);
      channel.close();
      Thread.sleep(60_000);
    } catch (Exception e) {
    } finally {
      tsFileManager.writeUnlock();
    }
    Assert.assertTrue(tsFileManager.isAllowCompaction());
    Assert.assertEquals(10, tsFileManager.getTsFileList(true).size());
  }
}
