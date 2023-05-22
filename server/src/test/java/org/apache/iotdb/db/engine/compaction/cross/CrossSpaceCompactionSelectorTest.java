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
package org.apache.iotdb.db.engine.compaction.cross;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.compaction.AbstractCompactionTest;
import org.apache.iotdb.db.engine.compaction.selector.impl.RewriteCrossSpaceCompactionSelector;
import org.apache.iotdb.db.engine.compaction.selector.utils.CrossCompactionTaskResource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResourceStatus;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.query.control.FileReaderManager;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class CrossSpaceCompactionSelectorTest extends AbstractCompactionTest {

  @Before
  public void setUp()
      throws IOException, WriteProcessException, MetadataException, InterruptedException {
    super.setUp();
    IoTDBDescriptor.getInstance().getConfig().setMinCrossCompactionUnseqFileLevel(0);
  }

  @After
  public void tearDown() throws IOException, StorageEngineException {
    super.tearDown();
    for (TsFileResource tsFileResource : seqResources) {
      FileReaderManager.getInstance().closeFileAndRemoveReader(tsFileResource.getTsFilePath());
    }
    for (TsFileResource tsFileResource : unseqResources) {
      FileReaderManager.getInstance().closeFileAndRemoveReader(tsFileResource.getTsFilePath());
    }
  }

  @Test
  public void testSelectWithEmptySeqFileList()
      throws IOException, MetadataException, WriteProcessException {
    createFiles(5, 2, 3, 50, 0, 10000, 50, 50, false, false);
    RewriteCrossSpaceCompactionSelector selector =
        new RewriteCrossSpaceCompactionSelector("", "", 0, null);
    List<CrossCompactionTaskResource> selected =
        selector.selectCrossSpaceTask(seqResources, unseqResources);
    Assert.assertEquals(0, selected.size());
  }

  @Test
  public void testSelectWithOneUnclosedSeqFile()
      throws IOException, MetadataException, WriteProcessException {
    createFiles(1, 2, 3, 50, 0, 10000, 50, 50, false, true);
    createFiles(5, 2, 3, 50, 0, 10000, 50, 50, false, false);
    seqResources.get(0).setStatus(TsFileResourceStatus.UNCLOSED);
    RewriteCrossSpaceCompactionSelector selector =
        new RewriteCrossSpaceCompactionSelector("", "", 0, null);
    List<CrossCompactionTaskResource> selected =
        selector.selectCrossSpaceTask(seqResources, unseqResources);
    Assert.assertEquals(0, selected.size());
  }

  @Test
  public void testSelectWithClosedSeqFileAndUnOverlapUnseqFile()
      throws IOException, MetadataException, WriteProcessException {
    createFiles(1, 2, 3, 50, 0, 10000, 50, 50, false, true);
    createFiles(5, 2, 3, 50, 0, 10000, 50, 50, false, false);
    RewriteCrossSpaceCompactionSelector selector =
        new RewriteCrossSpaceCompactionSelector("", "", 0, null);
    List<CrossCompactionTaskResource> selected =
        selector.selectCrossSpaceTask(seqResources, unseqResources);
    Assert.assertEquals(1, selected.size());
    Assert.assertEquals(1, selected.get(0).getSeqFiles().size());
    Assert.assertEquals(5, selected.get(0).getUnseqFiles().size());

    createFiles(1, 2, 3, 50, 100, 10000, 50, 50, false, true);
    selected = selector.selectCrossSpaceTask(seqResources, unseqResources);
    Assert.assertEquals(1, selected.size());
    Assert.assertEquals(2, selected.get(0).getSeqFiles().size());
    Assert.assertEquals(5, selected.get(0).getUnseqFiles().size());
  }

  @Test
  public void testSelectWithClosedSeqFileAndUncloseSeqFile()
      throws IOException, MetadataException, WriteProcessException {
    createFiles(2, 2, 3, 50, 0, 10000, 50, 50, false, true);
    createFiles(5, 2, 3, 50, 0, 10000, 50, 50, false, false);
    seqResources.get(1).setStatus(TsFileResourceStatus.UNCLOSED);
    RewriteCrossSpaceCompactionSelector selector =
        new RewriteCrossSpaceCompactionSelector("", "", 0, null);
    List<CrossCompactionTaskResource> selected =
        selector.selectCrossSpaceTask(seqResources, unseqResources);
    Assert.assertEquals(1, selected.size());
    Assert.assertEquals(1, selected.get(0).getSeqFiles().size());
    Assert.assertEquals(1, selected.get(0).getUnseqFiles().size());

    createFiles(1, 2, 3, 200, 200, 10000, 50, 50, false, true);
    seqResources.get(1).setStatus(TsFileResourceStatus.NORMAL);
    seqResources.get(2).setStatus(TsFileResourceStatus.UNCLOSED);
    selected = selector.selectCrossSpaceTask(seqResources, unseqResources);
    Assert.assertEquals(1, selected.size());
    Assert.assertEquals(2, selected.get(0).getSeqFiles().size());
    Assert.assertEquals(2, selected.get(0).getUnseqFiles().size());

    createFiles(1, 2, 3, 200, 1000, 10000, 50, 50, false, true);
    createFiles(1, 2, 3, 200, 2000, 10000, 50, 50, false, true);
    seqResources.get(2).setStatus(TsFileResourceStatus.NORMAL);
    seqResources.get(4).setStatus(TsFileResourceStatus.UNCLOSED);
    selected = selector.selectCrossSpaceTask(seqResources, unseqResources);
    Assert.assertEquals(1, selected.size());
    Assert.assertEquals(4, selected.get(0).getSeqFiles().size());
    Assert.assertEquals(5, selected.get(0).getUnseqFiles().size());
  }

  @Test
  public void testSelectWithMultiUnseqFilesOverlapWithOneSeqFile()
      throws IOException, MetadataException, WriteProcessException {
    createFiles(3, 2, 3, 50, 0, 10000, 50, 50, false, true);
    createFiles(1, 2, 3, 50, 0, 10000, 50, 50, false, false);
    createFiles(1, 2, 3, 50, 0, 10000, 50, 50, false, false);
    createFiles(1, 2, 3, 50, 0, 10000, 50, 50, false, false);
    createFiles(1, 2, 3, 50, 0, 10000, 50, 50, false, false);
    createFiles(1, 2, 3, 50, 0, 10000, 50, 50, false, false);
    RewriteCrossSpaceCompactionSelector selector =
        new RewriteCrossSpaceCompactionSelector("", "", 0, null);
    List<CrossCompactionTaskResource> selected =
        selector.selectCrossSpaceTask(seqResources, unseqResources);
    Assert.assertEquals(1, selected.size());
    Assert.assertEquals(1, selected.get(0).getSeqFiles().size());
    Assert.assertEquals(5, selected.get(0).getUnseqFiles().size());
  }

  @Test
  public void testSelectWithTooLargeSeqFile()
      throws IOException, MetadataException, WriteProcessException {
    createFiles(2, 2, 3, 50, 0, 10000, 50, 50, false, true);
    createFiles(5, 2, 3, 50, 0, 10000, 50, 50, false, false);
    RewriteCrossSpaceCompactionSelector selector =
        new RewriteCrossSpaceCompactionSelector("", "", 0, null);
    List<CrossCompactionTaskResource> selected =
        selector.selectCrossSpaceTask(seqResources, unseqResources);
    Assert.assertEquals(1, selected.size());
    Assert.assertEquals(2, selected.get(0).getSeqFiles().size());
    Assert.assertEquals(5, selected.get(0).getUnseqFiles().size());

    IoTDBDescriptor.getInstance().getConfig().setTargetCompactionFileSize(1L);
    selected = selector.selectCrossSpaceTask(seqResources, unseqResources);
    Assert.assertEquals(0, selected.size());
  }
}
