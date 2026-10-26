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

package org.apache.iotdb.db.storageengine.dataregion.compaction.utils;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.service.metrics.FileMetrics;
import org.apache.iotdb.db.storageengine.dataregion.compaction.AbstractCompactionTest;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.impl.FastCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.impl.ReadChunkCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.CrossSpaceCompactionTask;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.InnerSpaceCompactionTask;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import org.apache.tsfile.exception.write.WriteProcessException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class CompactionUpdateFileCountTest extends AbstractCompactionTest {

  @Before
  public void setUp()
      throws IOException, WriteProcessException, MetadataException, InterruptedException {
    super.setUp();
  }

  @After
  public void tearDown() throws IOException, StorageEngineException {
    super.tearDown();
  }

  @Test
  public void testSeqSpaceCompactionFileMetric()
      throws MetadataException, IOException, WriteProcessException {
    registerTimeseriesInMManger(2, 3, false);
    createFiles(1, 2, 3, 100, 1, 0, 50, 0, false, true);
    createFiles(1, 2, 3, 50, 200, 30000, 50, 50, false, true);
    seqResources.forEach(FileMetrics.getInstance()::addTsFile);
    long initSeqFileNum = FileMetrics.getInstance().getFileCount(true);
    long initUnSeqFileNum = FileMetrics.getInstance().getFileCount(false);
    tsFileManager.addAll(seqResources, true);
    InnerSpaceCompactionTask task =
        new InnerSpaceCompactionTask(
            0, tsFileManager, seqResources, true, new ReadChunkCompactionPerformer(), 0);
    Assert.assertTrue(task.start());
    Assert.assertEquals(initSeqFileNum - 1, FileMetrics.getInstance().getFileCount(true));
    Assert.assertEquals(initUnSeqFileNum, FileMetrics.getInstance().getFileCount(false));
  }

  @Test
  public void testUnSeqSpaceCompactionFileMetric()
      throws MetadataException, IOException, WriteProcessException {
    registerTimeseriesInMManger(2, 3, false);
    createFiles(1, 2, 3, 100, 1, 0, 50, 0, false, false);
    createFiles(1, 2, 3, 50, 20, 10000, 50, 50, false, false);
    unseqResources.forEach(FileMetrics.getInstance()::addTsFile);
    long initSeqFileNum = FileMetrics.getInstance().getFileCount(true);
    long initUnSeqFileNum = FileMetrics.getInstance().getFileCount(false);
    tsFileManager.addAll(unseqResources, false);
    InnerSpaceCompactionTask task =
        new InnerSpaceCompactionTask(
            0, tsFileManager, unseqResources, false, new FastCompactionPerformer(false), 0);
    Assert.assertTrue(task.start());
    Assert.assertEquals(initSeqFileNum, FileMetrics.getInstance().getFileCount(true));
    Assert.assertEquals(initUnSeqFileNum - 1, FileMetrics.getInstance().getFileCount(false));
  }

  @Test
  public void testCrossSpaceCompactionFileMetric()
      throws MetadataException, IOException, WriteProcessException {
    registerTimeseriesInMManger(2, 3, false);
    createFiles(1, 2, 3, 100, 1, 0, 50, 0, false, true);
    createFiles(3, 2, 3, 50, 20, 10000, 50, 50, false, false);
    seqResources.forEach(FileMetrics.getInstance()::addTsFile);
    unseqResources.forEach(FileMetrics.getInstance()::addTsFile);
    long initSeqFileNum = FileMetrics.getInstance().getFileCount(true);
    long initUnSeqFileNum = FileMetrics.getInstance().getFileCount(false);
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);
    CrossSpaceCompactionTask task =
        new CrossSpaceCompactionTask(
            0,
            tsFileManager,
            seqResources,
            unseqResources,
            new FastCompactionPerformer(true),
            0,
            0);
    Assert.assertTrue(task.start());
    Assert.assertEquals(initSeqFileNum, FileMetrics.getInstance().getFileCount(true));
    Assert.assertEquals(initUnSeqFileNum - 3, FileMetrics.getInstance().getFileCount(false));
  }

  @Test
  public void testRepeatedlyUpdateFileMetric()
      throws MetadataException, IOException, WriteProcessException {
    registerTimeseriesInMManger(2, 3, false);
    createFiles(1, 2, 3, 100, 1, 0, 50, 0, false, true);
    TsFileResource resource = seqResources.get(0);
    long initSeqFileNum = FileMetrics.getInstance().getFileCount(true);

    try {
      FileMetrics.getInstance().addTsFile(resource);
      FileMetrics.getInstance().addTsFile(resource);
      Assert.assertTrue(resource.isRecordedByMetric());
      Assert.assertEquals(initSeqFileNum + 1, FileMetrics.getInstance().getFileCount(true));

      FileMetrics.getInstance().deleteTsFile(Collections.singletonList(resource));
      FileMetrics.getInstance().deleteTsFile(Collections.singletonList(resource));
      Assert.assertFalse(resource.isRecordedByMetric());
      Assert.assertEquals(initSeqFileNum, FileMetrics.getInstance().getFileCount(true));
    } finally {
      FileMetrics.getInstance().deleteTsFile(Collections.singletonList(resource));
    }
  }

  @Test
  public void testDeleteFileMetricByResourceSequence()
      throws MetadataException, IOException, WriteProcessException {
    registerTimeseriesInMManger(2, 3, false);
    createFiles(1, 2, 3, 100, 1, 0, 50, 0, false, true);
    createFiles(1, 2, 3, 50, 200, 30000, 50, 50, false, false);
    List<TsFileResource> resources = Arrays.asList(seqResources.get(0), unseqResources.get(0));
    long initSeqFileNum = FileMetrics.getInstance().getFileCount(true);
    long initUnSeqFileNum = FileMetrics.getInstance().getFileCount(false);

    try {
      resources.forEach(FileMetrics.getInstance()::addTsFile);
      Assert.assertEquals(initSeqFileNum + 1, FileMetrics.getInstance().getFileCount(true));
      Assert.assertEquals(initUnSeqFileNum + 1, FileMetrics.getInstance().getFileCount(false));

      FileMetrics.getInstance().deleteTsFile(resources);
      Assert.assertEquals(initSeqFileNum, FileMetrics.getInstance().getFileCount(true));
      Assert.assertEquals(initUnSeqFileNum, FileMetrics.getInstance().getFileCount(false));
    } finally {
      FileMetrics.getInstance().deleteTsFile(resources);
    }
  }
}
