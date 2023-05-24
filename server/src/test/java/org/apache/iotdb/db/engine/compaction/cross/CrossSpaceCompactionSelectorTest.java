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
import org.apache.iotdb.db.engine.compaction.execute.task.CrossSpaceCompactionTask;
import org.apache.iotdb.db.engine.compaction.schedule.CompactionTaskManager;
import org.apache.iotdb.db.engine.compaction.selector.impl.RewriteCrossSpaceCompactionSelector;
import org.apache.iotdb.db.engine.compaction.selector.utils.CrossCompactionTaskResource;
import org.apache.iotdb.db.engine.compaction.selector.utils.CrossSpaceCompactionCandidate;
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
import java.util.concurrent.atomic.AtomicBoolean;

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

  @Test
  public void testSeqFileWithDeviceIndexBeenDeletedBeforeSelection()
      throws IOException, MetadataException, WriteProcessException, InterruptedException {
    createFiles(5, 2, 3, 50, 0, 10000, 50, 50, false, true);
    createFiles(5, 2, 3, 50, 0, 10000, 50, 50, false, false);
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);
    AtomicBoolean fail = new AtomicBoolean(false);

    Thread thread1 =
        new Thread(
            () -> {
              try {
                // the file is deleted before selection
                Thread.sleep(1000);
                RewriteCrossSpaceCompactionSelector selector =
                    new RewriteCrossSpaceCompactionSelector("", "", 0, null);
                CrossSpaceCompactionCandidate candidate =
                    new CrossSpaceCompactionCandidate(
                        seqResources, unseqResources, System.currentTimeMillis() - Long.MAX_VALUE);
                candidate.releaseReadLock();
                CrossCompactionTaskResource crossCompactionTaskResource =
                    selector.selectOneTaskResources(candidate);
                if (!crossCompactionTaskResource.isValid()) {
                  throw new RuntimeException("compaction task resource is not valid");
                }
                if (crossCompactionTaskResource.getSeqFiles().size() != 1) {
                  throw new RuntimeException("selected seq file num is not 1");
                }
                if (crossCompactionTaskResource.getUnseqFiles().size() != 1) {
                  throw new RuntimeException("selected unseq file num is not 1");
                }
                candidate.releaseReadLock();
              } catch (Exception e) {
                fail.set(true);
                e.printStackTrace();
                Assert.fail(e.getMessage());
              }
            });

    // delete seq files
    Thread thread2 =
        new Thread(
            () -> {
              try {
                TsFileResource resource = seqResources.get(1);
                // try to delete file
                resource.writeLock();
                resource.remove();
                resource.writeUnlock();
              } catch (Exception e) {
                fail.set(true);
                e.printStackTrace();
                Assert.fail(e.getMessage());
              }
            });

    thread1.start();
    thread2.start();
    thread1.join(10000);
    thread2.join(10000);
    if (fail.get()) {
      Assert.fail();
    }
  }

  @Test
  public void testSeqFileWithDeviceIndexBeenDeletedDuringSelectionAndBeforeSettingCandidate()
      throws IOException, MetadataException, WriteProcessException, InterruptedException {
    createFiles(5, 2, 3, 50, 0, 10000, 50, 50, false, true);
    createFiles(5, 2, 3, 50, 0, 10000, 50, 50, false, false);
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);
    AtomicBoolean fail = new AtomicBoolean(false);

    // select files in cross compaction
    Thread thread1 =
        new Thread(
            () -> {
              try {
                RewriteCrossSpaceCompactionSelector selector =
                    new RewriteCrossSpaceCompactionSelector("", "", 0, null);
                // copy candidate source file list
                CrossSpaceCompactionCandidate candidate =
                    new CrossSpaceCompactionCandidate(
                        seqResources, unseqResources, System.currentTimeMillis() - Long.MAX_VALUE);
                candidate.addReadLock();

                // the other thread want to hold write lock to delete files, but is stuck
                Thread.sleep(1000);
                CrossCompactionTaskResource crossCompactionTaskResource =
                    selector.selectOneTaskResources(candidate);
                if (!crossCompactionTaskResource.isValid()) {
                  throw new RuntimeException("compaction task resource is not valid");
                }
                if (crossCompactionTaskResource.getSeqFiles().size() != 5) {
                  throw new RuntimeException("selected seq file num is not 5");
                }
                if (crossCompactionTaskResource.getUnseqFiles().size() != 5) {
                  throw new RuntimeException("selected unseq file num is not 5");
                }
                candidate.releaseReadLock();

                // the other thread holds write lock and delete file successfully before setting
                // file status to COMPACTION_CANDIDATE

                CrossSpaceCompactionTask crossSpaceCompactionTask =
                    new CrossSpaceCompactionTask(
                        0,
                        tsFileManager,
                        crossCompactionTaskResource.getSeqFiles(),
                        crossCompactionTaskResource.getUnseqFiles(),
                        IoTDBDescriptor.getInstance()
                            .getConfig()
                            .getCrossCompactionPerformer()
                            .createInstance(),
                        CompactionTaskManager.currentTaskNum,
                        crossCompactionTaskResource.getTotalMemoryCost(),
                        tsFileManager.getNextCompactionTaskId());
                // set file status to COMPACTION_CANDIDATE
                if (crossSpaceCompactionTask.setSourceFilesToCompactionCandidate()) {
                  throw new RuntimeException("set status should be false");
                }
                for (int i = 0; i < seqResources.size(); i++) {
                  TsFileResource resource = seqResources.get(i);
                  if (i == 1) {
                    if (resource.getStatus() != TsFileResourceStatus.DELETED) {
                      throw new RuntimeException("status should be DELETED");
                    }
                  } else if (resource.getStatus() != TsFileResourceStatus.NORMAL) {
                    throw new RuntimeException("status should be NORMAL");
                  }
                }
                for (int i = 0; i < unseqResources.size(); i++) {
                  TsFileResource resource = unseqResources.get(i);
                  if (resource.getStatus() != TsFileResourceStatus.NORMAL) {
                    throw new RuntimeException("status should be NORMAL");
                  }
                }
              } catch (Exception e) {
                fail.set(true);
                e.printStackTrace();
              }
            });

    // delete seq files
    Thread thread2 =
        new Thread(
            () -> {
              try {
                Thread.sleep(200);
                TsFileResource resource = seqResources.get(1);
                // try to delete file
                resource.writeLock();
                resource.remove();
                resource.writeUnlock();
              } catch (Exception e) {
                fail.set(true);
                e.printStackTrace();
              }
            });

    thread1.start();
    thread2.start();
    thread1.join(10000);
    thread2.join(10000);
    if (fail.get()) {
      Assert.fail();
    }
  }

  @Test
  public void testSeqFileWithDeviceIndexBeenDeletedDuringSelectionAndBeforeSettingCompacting()
      throws IOException, MetadataException, WriteProcessException, InterruptedException {
    createFiles(5, 2, 3, 50, 0, 10000, 50, 50, false, true);
    createFiles(5, 2, 3, 50, 0, 10000, 50, 50, false, false);
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);
    AtomicBoolean fail = new AtomicBoolean(false);

    // select files in cross compaction
    Thread thread1 =
        new Thread(
            () -> {
              try {
                RewriteCrossSpaceCompactionSelector selector =
                    new RewriteCrossSpaceCompactionSelector("", "", 0, null);
                // copy candidate source file list
                CrossSpaceCompactionCandidate candidate =
                    new CrossSpaceCompactionCandidate(
                        seqResources, unseqResources, System.currentTimeMillis() - Long.MAX_VALUE);
                candidate.addReadLock();

                CrossCompactionTaskResource crossCompactionTaskResource =
                    selector.selectOneTaskResources(candidate);
                if (!crossCompactionTaskResource.isValid()) {
                  throw new RuntimeException("compaction task resource is not valid");
                }
                if (crossCompactionTaskResource.getSeqFiles().size() != 5) {
                  throw new RuntimeException("selected seq file num is not 5");
                }
                if (crossCompactionTaskResource.getUnseqFiles().size() != 5) {
                  throw new RuntimeException("selected unseq file num is not 5");
                }
                candidate.releaseReadLock();

                CrossSpaceCompactionTask crossSpaceCompactionTask =
                    new CrossSpaceCompactionTask(
                        0,
                        tsFileManager,
                        crossCompactionTaskResource.getSeqFiles(),
                        crossCompactionTaskResource.getUnseqFiles(),
                        IoTDBDescriptor.getInstance()
                            .getConfig()
                            .getCrossCompactionPerformer()
                            .createInstance(),
                        CompactionTaskManager.currentTaskNum,
                        crossCompactionTaskResource.getTotalMemoryCost(),
                        tsFileManager.getNextCompactionTaskId());
                // set file status to COMPACTION_CANDIDATE and add into queue
                if (!crossSpaceCompactionTask.setSourceFilesToCompactionCandidate()) {
                  throw new RuntimeException("fail to set status to compaction candidate.");
                }

                // the other thread delete the file successfully when the compaction task is blocked
                // in the queue
                Thread.sleep(1000);

                if (crossSpaceCompactionTask.checkValidAndSetMerging()) {
                  throw new RuntimeException("cross space compaction task should be invalid.");
                }

                for (int i = 0; i < seqResources.size(); i++) {
                  TsFileResource resource = seqResources.get(i);
                  if (i == 1) {
                    if (resource.getStatus() != TsFileResourceStatus.DELETED) {
                      throw new RuntimeException("status should be DELETED");
                    }
                  } else if (resource.getStatus() != TsFileResourceStatus.NORMAL) {
                    throw new RuntimeException("status should be NORMAL");
                  }
                }
                for (int i = 0; i < unseqResources.size(); i++) {
                  TsFileResource resource = unseqResources.get(i);
                  if (resource.getStatus() != TsFileResourceStatus.NORMAL) {
                    throw new RuntimeException("status should be NORMAL");
                  }
                }
              } catch (Exception e) {
                fail.set(true);
                e.printStackTrace();
              }
            });

    // delete seq files
    Thread thread2 =
        new Thread(
            () -> {
              try {
                Thread.sleep(500);
                TsFileResource resource = seqResources.get(1);
                // try to delete file
                resource.writeLock();
                resource.remove();
                resource.writeUnlock();
              } catch (Exception e) {
                fail.set(true);
                e.printStackTrace();
              }
            });

    thread1.start();
    thread2.start();
    thread1.join(10000);
    thread2.join(10000);
    if (fail.get()) {
      Assert.fail();
    }
  }

  @Test
  public void testSeqFileWithFileIndexBeenDeletedDuringSelectionAndBeforeSettingCandidate()
      throws IOException, MetadataException, WriteProcessException, InterruptedException {
    createFiles(5, 2, 3, 50, 0, 10000, 50, 50, false, true);
    createFiles(5, 2, 3, 50, 0, 10000, 50, 50, false, false);
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);
    AtomicBoolean fail = new AtomicBoolean(false);
    seqResources.get(0).degradeTimeIndex();
    seqResources.get(1).degradeTimeIndex();
    seqResources.get(2).degradeTimeIndex();
    unseqResources.get(1).degradeTimeIndex();

    // select files in cross compaction
    Thread thread1 =
        new Thread(
            () -> {
              try {
                RewriteCrossSpaceCompactionSelector selector =
                    new RewriteCrossSpaceCompactionSelector("", "", 0, null);
                // copy candidate source file list
                CrossSpaceCompactionCandidate candidate =
                    new CrossSpaceCompactionCandidate(
                        seqResources, unseqResources, System.currentTimeMillis() - Long.MAX_VALUE);
                candidate.addReadLock();

                // the other thread want to hold write lock to delete files, but is stuck
                Thread.sleep(1000);
                CrossCompactionTaskResource crossCompactionTaskResource =
                    selector.selectOneTaskResources(candidate);
                if (!crossCompactionTaskResource.isValid()) {
                  throw new RuntimeException("compaction task resource is not valid");
                }
                if (crossCompactionTaskResource.getSeqFiles().size() != 5) {
                  throw new RuntimeException("selected seq file num is not 5");
                }
                if (crossCompactionTaskResource.getUnseqFiles().size() != 5) {
                  throw new RuntimeException("selected unseq file num is not 5");
                }
                candidate.releaseReadLock();

                // the other thread holds write lock and delete file successfully before setting
                // file status to COMPACTION_CANDIDATE

                CrossSpaceCompactionTask crossSpaceCompactionTask =
                    new CrossSpaceCompactionTask(
                        0,
                        tsFileManager,
                        crossCompactionTaskResource.getSeqFiles(),
                        crossCompactionTaskResource.getUnseqFiles(),
                        IoTDBDescriptor.getInstance()
                            .getConfig()
                            .getCrossCompactionPerformer()
                            .createInstance(),
                        CompactionTaskManager.currentTaskNum,
                        crossCompactionTaskResource.getTotalMemoryCost(),
                        tsFileManager.getNextCompactionTaskId());
                // set file status to COMPACTION_CANDIDATE
                if (crossSpaceCompactionTask.setSourceFilesToCompactionCandidate()) {
                  throw new RuntimeException("set status should be false");
                }
                for (int i = 0; i < seqResources.size(); i++) {
                  TsFileResource resource = seqResources.get(i);
                  if (i == 1) {
                    if (resource.getStatus() != TsFileResourceStatus.DELETED) {
                      throw new RuntimeException("status should be DELETED");
                    }
                  } else if (resource.getStatus() != TsFileResourceStatus.NORMAL) {
                    throw new RuntimeException("status should be NORMAL");
                  }
                }
                for (int i = 0; i < unseqResources.size(); i++) {
                  TsFileResource resource = unseqResources.get(i);
                  if (resource.getStatus() != TsFileResourceStatus.NORMAL) {
                    throw new RuntimeException("status should be NORMAL");
                  }
                }
              } catch (Exception e) {
                fail.set(true);
                e.printStackTrace();
              }
            });

    // delete seq files
    Thread thread2 =
        new Thread(
            () -> {
              try {
                Thread.sleep(200);
                TsFileResource resource = seqResources.get(1);
                // try to delete file
                resource.writeLock();
                resource.remove();
                resource.writeUnlock();
              } catch (Exception e) {
                fail.set(true);
                e.printStackTrace();
              }
            });

    thread1.start();
    thread2.start();
    thread1.join(10000);
    thread2.join(10000);
    if (fail.get()) {
      Assert.fail();
    }
  }

  @Test
  public void testSeqFileWithFileIndexBeenDeletedDuringSelectionAndBeforeSettingCompacting()
      throws IOException, MetadataException, WriteProcessException, InterruptedException {
    createFiles(5, 2, 3, 50, 0, 10000, 50, 50, false, true);
    createFiles(5, 2, 3, 50, 0, 10000, 50, 50, false, false);
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);
    AtomicBoolean fail = new AtomicBoolean(false);
    seqResources.get(0).degradeTimeIndex();
    seqResources.get(1).degradeTimeIndex();
    seqResources.get(2).degradeTimeIndex();
    unseqResources.get(1).degradeTimeIndex();

    // select files in cross compaction
    Thread thread1 =
        new Thread(
            () -> {
              try {
                RewriteCrossSpaceCompactionSelector selector =
                    new RewriteCrossSpaceCompactionSelector("", "", 0, null);
                // copy candidate source file list
                CrossSpaceCompactionCandidate candidate =
                    new CrossSpaceCompactionCandidate(
                        seqResources, unseqResources, System.currentTimeMillis() - Long.MAX_VALUE);
                candidate.addReadLock();

                CrossCompactionTaskResource crossCompactionTaskResource =
                    selector.selectOneTaskResources(candidate);
                if (!crossCompactionTaskResource.isValid()) {
                  throw new RuntimeException("compaction task resource is not valid");
                }
                if (crossCompactionTaskResource.getSeqFiles().size() != 5) {
                  throw new RuntimeException("selected seq file num is not 5");
                }
                if (crossCompactionTaskResource.getUnseqFiles().size() != 5) {
                  throw new RuntimeException("selected unseq file num is not 5");
                }
                candidate.releaseReadLock();

                // the other thread holds write lock and delete file successfully before setting
                // file status to COMPACTION_CANDIDATE

                CrossSpaceCompactionTask crossSpaceCompactionTask =
                    new CrossSpaceCompactionTask(
                        0,
                        tsFileManager,
                        crossCompactionTaskResource.getSeqFiles(),
                        crossCompactionTaskResource.getUnseqFiles(),
                        IoTDBDescriptor.getInstance()
                            .getConfig()
                            .getCrossCompactionPerformer()
                            .createInstance(),
                        CompactionTaskManager.currentTaskNum,
                        crossCompactionTaskResource.getTotalMemoryCost(),
                        tsFileManager.getNextCompactionTaskId());
                // set file status to COMPACTION_CANDIDATE and add into queue
                if (!crossSpaceCompactionTask.setSourceFilesToCompactionCandidate()) {
                  throw new RuntimeException("fail to set status to compaction candidate.");
                }

                // the other thread delete the file successfully when the compaction task is blocked
                // in the queue
                Thread.sleep(1000);

                if (crossSpaceCompactionTask.checkValidAndSetMerging()) {
                  throw new RuntimeException("cross space compaction task should be invalid.");
                }

                for (int i = 0; i < seqResources.size(); i++) {
                  TsFileResource resource = seqResources.get(i);
                  if (i == 1) {
                    if (resource.getStatus() != TsFileResourceStatus.DELETED) {
                      throw new RuntimeException("status should be DELETED");
                    }
                  } else if (resource.getStatus() != TsFileResourceStatus.NORMAL) {
                    throw new RuntimeException("status should be NORMAL");
                  }
                }
                for (int i = 0; i < unseqResources.size(); i++) {
                  TsFileResource resource = unseqResources.get(i);
                  if (resource.getStatus() != TsFileResourceStatus.NORMAL) {
                    throw new RuntimeException("status should be NORMAL");
                  }
                }
              } catch (Exception e) {
                fail.set(true);
                e.printStackTrace();
              }
            });

    // delete seq files
    Thread thread2 =
        new Thread(
            () -> {
              try {
                Thread.sleep(500);
                TsFileResource resource = seqResources.get(1);
                // try to delete file
                resource.writeLock();
                resource.remove();
                resource.writeUnlock();
              } catch (Exception e) {
                fail.set(true);
                e.printStackTrace();
              }
            });

    thread1.start();
    thread2.start();
    thread1.join(10000);
    thread2.join(10000);
    if (fail.get()) {
      Assert.fail();
    }
  }

  @Test
  public void testSeqFileWithFileIndexBeenDeletedDuringSelection()
      throws IOException, MetadataException, WriteProcessException, InterruptedException {
    createFiles(5, 2, 3, 50, 0, 10000, 50, 50, false, true);
    createFiles(5, 2, 3, 50, 0, 10000, 50, 50, false, false);
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);
    AtomicBoolean fail = new AtomicBoolean(false);
    seqResources.get(0).degradeTimeIndex();
    seqResources.get(1).degradeTimeIndex();
    seqResources.get(2).degradeTimeIndex();
    unseqResources.get(1).degradeTimeIndex();

    // select files in cross compaction
    Thread thread1 =
        new Thread(
            () -> {
              try {
                RewriteCrossSpaceCompactionSelector selector =
                    new RewriteCrossSpaceCompactionSelector("", "", 0, null);
                // copy candidate source file list, hold read lock
                CrossSpaceCompactionCandidate candidate =
                    new CrossSpaceCompactionCandidate(
                        seqResources, unseqResources, System.currentTimeMillis() - Long.MAX_VALUE);
                candidate.addReadLock();

                // other thread want to hold write lock to delete files, but is stuck
                Thread.sleep(1000);
                CrossCompactionTaskResource crossCompactionTaskResource =
                    selector.selectOneTaskResources(candidate);
                if (crossCompactionTaskResource.getSeqFiles().size() != 5) {
                  throw new RuntimeException("selected seq file num is not 5");
                }
                if (crossCompactionTaskResource.getUnseqFiles().size() != 5) {
                  throw new RuntimeException("selected unseq file num is not 5");
                }
                candidate.releaseReadLock();
              } catch (Exception e) {
                fail.set(true);
                e.printStackTrace();
              }
            });

    // delete seq files
    Thread thread2 =
        new Thread(
            () -> {
              try {
                Thread.sleep(200);
                TsFileResource resource = seqResources.get(1);
                // try to delete file
                resource.writeLock();
                resource.remove();
                resource.writeUnlock();
              } catch (Exception e) {
                fail.set(true);
                e.printStackTrace();
              }
            });

    thread1.start();
    thread2.start();
    thread1.join(10000);
    thread2.join(10000);
    if (fail.get()) {
      Assert.fail();
    }
  }

  @Test
  public void testUnSeqFileWithDeviceIndexBeenDeletedBeforeSelection()
      throws IOException, MetadataException, WriteProcessException, InterruptedException {
    createFiles(5, 2, 3, 50, 0, 10000, 50, 50, false, true);
    createFiles(5, 2, 3, 50, 0, 10000, 50, 50, false, false);
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);
    AtomicBoolean fail = new AtomicBoolean(false);

    Thread thread1 =
        new Thread(
            () -> {
              try {
                // the file is deleted before selection
                Thread.sleep(1000);
                RewriteCrossSpaceCompactionSelector selector =
                    new RewriteCrossSpaceCompactionSelector("", "", 0, null);
                CrossSpaceCompactionCandidate candidate =
                    new CrossSpaceCompactionCandidate(
                        seqResources, unseqResources, System.currentTimeMillis() - Long.MAX_VALUE);
                candidate.addReadLock();
                CrossCompactionTaskResource crossCompactionTaskResource =
                    selector.selectOneTaskResources(candidate);
                if (!crossCompactionTaskResource.isValid()) {
                  throw new RuntimeException("compaction task resource is not valid");
                }
                if (crossCompactionTaskResource.getSeqFiles().size() != 1) {
                  throw new RuntimeException("selected seq file num is not 1");
                }
                if (crossCompactionTaskResource.getUnseqFiles().size() != 1) {
                  throw new RuntimeException("selected unseq file num is not 1");
                }
                candidate.releaseReadLock();
              } catch (Exception e) {
                fail.set(true);
                e.printStackTrace();
                Assert.fail(e.getMessage());
              }
            });

    // delete seq files
    Thread thread2 =
        new Thread(
            () -> {
              try {
                TsFileResource resource = unseqResources.get(1);
                // try to delete file
                resource.writeLock();
                resource.remove();
                resource.writeUnlock();
              } catch (Exception e) {
                fail.set(true);
                e.printStackTrace();
                Assert.fail(e.getMessage());
              }
            });

    thread1.start();
    thread2.start();
    thread1.join(10000);
    thread2.join(10000);
    if (fail.get()) {
      Assert.fail();
    }
  }

  @Test
  public void testUnSeqFileWithDeviceIndexBeenDeletedDuringSelectionAndBeforeSettingCandidate()
      throws IOException, MetadataException, WriteProcessException, InterruptedException {
    createFiles(5, 2, 3, 50, 0, 10000, 50, 50, false, true);
    createFiles(5, 2, 3, 50, 0, 10000, 50, 50, false, false);
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);
    AtomicBoolean fail = new AtomicBoolean(false);

    // select files in cross compaction
    Thread thread1 =
        new Thread(
            () -> {
              try {
                RewriteCrossSpaceCompactionSelector selector =
                    new RewriteCrossSpaceCompactionSelector("", "", 0, null);
                // copy candidate source file list
                CrossSpaceCompactionCandidate candidate =
                    new CrossSpaceCompactionCandidate(
                        seqResources, unseqResources, System.currentTimeMillis() - Long.MAX_VALUE);
                candidate.addReadLock();

                // the other thread want to hold write lock to delete files, but is stuck
                Thread.sleep(1000);
                CrossCompactionTaskResource crossCompactionTaskResource =
                    selector.selectOneTaskResources(candidate);
                if (!crossCompactionTaskResource.isValid()) {
                  throw new RuntimeException("compaction task resource is not valid");
                }
                if (crossCompactionTaskResource.getSeqFiles().size() != 5) {
                  throw new RuntimeException("selected seq file num is not 5");
                }
                if (crossCompactionTaskResource.getUnseqFiles().size() != 5) {
                  throw new RuntimeException("selected unseq file num is not 5");
                }
                candidate.releaseReadLock();

                // the other thread holds write lock and delete file successfully before setting
                // file status to COMPACTION_CANDIDATE

                CrossSpaceCompactionTask crossSpaceCompactionTask =
                    new CrossSpaceCompactionTask(
                        0,
                        tsFileManager,
                        crossCompactionTaskResource.getSeqFiles(),
                        crossCompactionTaskResource.getUnseqFiles(),
                        IoTDBDescriptor.getInstance()
                            .getConfig()
                            .getCrossCompactionPerformer()
                            .createInstance(),
                        CompactionTaskManager.currentTaskNum,
                        crossCompactionTaskResource.getTotalMemoryCost(),
                        tsFileManager.getNextCompactionTaskId());
                // set file status to COMPACTION_CANDIDATE
                if (crossSpaceCompactionTask.setSourceFilesToCompactionCandidate()) {
                  throw new RuntimeException("set status should be false");
                }
                for (int i = 0; i < unseqResources.size(); i++) {
                  TsFileResource resource = unseqResources.get(i);
                  if (i == 1) {
                    if (resource.getStatus() != TsFileResourceStatus.DELETED) {
                      throw new RuntimeException("status should be DELETED");
                    }
                  } else if (resource.getStatus() != TsFileResourceStatus.NORMAL) {
                    throw new RuntimeException("status should be NORMAL");
                  }
                }
                for (int i = 0; i < seqResources.size(); i++) {
                  TsFileResource resource = seqResources.get(i);
                  if (resource.getStatus() != TsFileResourceStatus.NORMAL) {
                    throw new RuntimeException("status should be NORMAL");
                  }
                }

              } catch (Exception e) {
                fail.set(true);
                e.printStackTrace();
              }
            });

    // delete seq files
    Thread thread2 =
        new Thread(
            () -> {
              try {
                Thread.sleep(200);
                TsFileResource resource = unseqResources.get(1);
                // try to delete file
                resource.writeLock();
                resource.remove();
                resource.writeUnlock();
              } catch (Exception e) {
                fail.set(true);
                e.printStackTrace();
              }
            });

    thread1.start();
    thread2.start();
    thread1.join(10000);
    thread2.join(10000);
    if (fail.get()) {
      Assert.fail();
    }
  }

  @Test
  public void testUnSeqFileWithDeviceIndexBeenDeletedDuringSelectionAndBeforeSettingCompacting()
      throws IOException, MetadataException, WriteProcessException, InterruptedException {
    createFiles(5, 2, 3, 50, 0, 10000, 50, 50, false, true);
    createFiles(5, 2, 3, 50, 0, 10000, 50, 50, false, false);
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);
    AtomicBoolean fail = new AtomicBoolean(false);

    // select files in cross compaction
    Thread thread1 =
        new Thread(
            () -> {
              try {
                RewriteCrossSpaceCompactionSelector selector =
                    new RewriteCrossSpaceCompactionSelector("", "", 0, null);
                // copy candidate source file list
                CrossSpaceCompactionCandidate candidate =
                    new CrossSpaceCompactionCandidate(
                        seqResources, unseqResources, System.currentTimeMillis() - Long.MAX_VALUE);
                candidate.addReadLock();

                CrossCompactionTaskResource crossCompactionTaskResource =
                    selector.selectOneTaskResources(candidate);
                if (!crossCompactionTaskResource.isValid()) {
                  throw new RuntimeException("compaction task resource is not valid");
                }
                if (crossCompactionTaskResource.getSeqFiles().size() != 5) {
                  throw new RuntimeException("selected seq file num is not 5");
                }
                if (crossCompactionTaskResource.getUnseqFiles().size() != 5) {
                  throw new RuntimeException("selected unseq file num is not 5");
                }
                candidate.releaseReadLock();

                // the other thread holds write lock and delete file successfully before setting
                // file status to COMPACTION_CANDIDATE

                CrossSpaceCompactionTask crossSpaceCompactionTask =
                    new CrossSpaceCompactionTask(
                        0,
                        tsFileManager,
                        crossCompactionTaskResource.getSeqFiles(),
                        crossCompactionTaskResource.getUnseqFiles(),
                        IoTDBDescriptor.getInstance()
                            .getConfig()
                            .getCrossCompactionPerformer()
                            .createInstance(),
                        CompactionTaskManager.currentTaskNum,
                        crossCompactionTaskResource.getTotalMemoryCost(),
                        tsFileManager.getNextCompactionTaskId());
                // set file status to COMPACTION_CANDIDATE and add into queue
                if (!crossSpaceCompactionTask.setSourceFilesToCompactionCandidate()) {
                  throw new RuntimeException("fail to set status to compaction candidate.");
                }

                // the other thread delete the file successfully when the compaction task is blocked
                // in the queue
                Thread.sleep(1000);

                if (crossSpaceCompactionTask.checkValidAndSetMerging()) {
                  throw new RuntimeException("cross space compaction task should be invalid.");
                }

                for (int i = 0; i < unseqResources.size(); i++) {
                  TsFileResource resource = unseqResources.get(i);
                  if (i == 1) {
                    if (resource.getStatus() != TsFileResourceStatus.DELETED) {
                      throw new RuntimeException("status should be DELETED");
                    }
                  } else if (resource.getStatus() != TsFileResourceStatus.NORMAL) {
                    throw new RuntimeException("status should be NORMAL");
                  }
                }
                for (int i = 0; i < seqResources.size(); i++) {
                  TsFileResource resource = seqResources.get(i);
                  if (resource.getStatus() != TsFileResourceStatus.NORMAL) {
                    throw new RuntimeException("status should be NORMAL");
                  }
                }
              } catch (Exception e) {
                fail.set(true);
                e.printStackTrace();
              }
            });

    // delete seq files
    Thread thread2 =
        new Thread(
            () -> {
              try {
                Thread.sleep(500);
                TsFileResource resource = unseqResources.get(1);
                // try to delete file
                resource.writeLock();
                resource.remove();
                resource.writeUnlock();
              } catch (Exception e) {
                fail.set(true);
                e.printStackTrace();
              }
            });

    thread1.start();
    thread2.start();
    thread1.join(10000);
    thread2.join(10000);
    if (fail.get()) {
      Assert.fail();
    }
  }

  @Test
  public void testUnSeqFileWithFileIndexBeenDeletedDuringSelectionAndBeforeSettingCandidate()
      throws IOException, MetadataException, WriteProcessException, InterruptedException {
    createFiles(5, 2, 3, 50, 0, 10000, 50, 50, false, true);
    createFiles(5, 2, 3, 50, 0, 10000, 50, 50, false, false);
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);
    AtomicBoolean fail = new AtomicBoolean(false);
    unseqResources.get(0).degradeTimeIndex();
    unseqResources.get(1).degradeTimeIndex();
    unseqResources.get(2).degradeTimeIndex();
    seqResources.get(1).degradeTimeIndex();

    // select files in cross compaction
    Thread thread1 =
        new Thread(
            () -> {
              try {
                RewriteCrossSpaceCompactionSelector selector =
                    new RewriteCrossSpaceCompactionSelector("", "", 0, null);
                // copy candidate source file list
                CrossSpaceCompactionCandidate candidate =
                    new CrossSpaceCompactionCandidate(
                        seqResources, unseqResources, System.currentTimeMillis() - Long.MAX_VALUE);
                candidate.addReadLock();

                // the other thread want to hold write lock to delete files, but is stuck
                Thread.sleep(1000);
                CrossCompactionTaskResource crossCompactionTaskResource =
                    selector.selectOneTaskResources(candidate);
                if (!crossCompactionTaskResource.isValid()) {
                  throw new RuntimeException("compaction task resource is not valid");
                }
                if (crossCompactionTaskResource.getSeqFiles().size() != 5) {
                  throw new RuntimeException("selected seq file num is not 5");
                }
                if (crossCompactionTaskResource.getUnseqFiles().size() != 5) {
                  throw new RuntimeException("selected unseq file num is not 5");
                }
                candidate.releaseReadLock();

                // the other thread holds write lock and delete file successfully before setting
                // file status to COMPACTION_CANDIDATE

                CrossSpaceCompactionTask crossSpaceCompactionTask =
                    new CrossSpaceCompactionTask(
                        0,
                        tsFileManager,
                        crossCompactionTaskResource.getSeqFiles(),
                        crossCompactionTaskResource.getUnseqFiles(),
                        IoTDBDescriptor.getInstance()
                            .getConfig()
                            .getCrossCompactionPerformer()
                            .createInstance(),
                        CompactionTaskManager.currentTaskNum,
                        crossCompactionTaskResource.getTotalMemoryCost(),
                        tsFileManager.getNextCompactionTaskId());
                // set file status to COMPACTION_CANDIDATE
                if (crossSpaceCompactionTask.setSourceFilesToCompactionCandidate()) {
                  throw new RuntimeException("set status should be false");
                }
                for (int i = 0; i < unseqResources.size(); i++) {
                  TsFileResource resource = unseqResources.get(i);
                  if (i == 1) {
                    if (resource.getStatus() != TsFileResourceStatus.DELETED) {
                      throw new RuntimeException("status should be DELETED");
                    }
                  } else if (resource.getStatus() != TsFileResourceStatus.NORMAL) {
                    throw new RuntimeException("status should be NORMAL");
                  }
                }
                for (int i = 0; i < seqResources.size(); i++) {
                  TsFileResource resource = seqResources.get(i);
                  if (resource.getStatus() != TsFileResourceStatus.NORMAL) {
                    throw new RuntimeException("status should be NORMAL");
                  }
                }
              } catch (Exception e) {
                fail.set(true);
                e.printStackTrace();
              }
            });

    // delete seq files
    Thread thread2 =
        new Thread(
            () -> {
              try {
                Thread.sleep(200);
                TsFileResource resource = unseqResources.get(1);
                // try to delete file
                resource.writeLock();
                resource.remove();
                resource.writeUnlock();
              } catch (Exception e) {
                fail.set(true);
                e.printStackTrace();
              }
            });

    thread1.start();
    thread2.start();
    thread1.join(10000);
    thread2.join(10000);
    if (fail.get()) {
      Assert.fail();
    }
  }

  @Test
  public void testUnSeqFileWithFileIndexBeenDeletedDuringSelectionAndBeforeSettingCompacting()
      throws IOException, MetadataException, WriteProcessException, InterruptedException {
    createFiles(5, 2, 3, 50, 0, 10000, 50, 50, false, true);
    createFiles(5, 2, 3, 50, 0, 10000, 50, 50, false, false);
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);
    AtomicBoolean fail = new AtomicBoolean(false);
    unseqResources.get(0).degradeTimeIndex();
    unseqResources.get(1).degradeTimeIndex();
    unseqResources.get(2).degradeTimeIndex();
    seqResources.get(1).degradeTimeIndex();

    // select files in cross compaction
    Thread thread1 =
        new Thread(
            () -> {
              try {
                RewriteCrossSpaceCompactionSelector selector =
                    new RewriteCrossSpaceCompactionSelector("", "", 0, null);
                // copy candidate source file list
                CrossSpaceCompactionCandidate candidate =
                    new CrossSpaceCompactionCandidate(
                        seqResources, unseqResources, System.currentTimeMillis() - Long.MAX_VALUE);
                candidate.addReadLock();

                CrossCompactionTaskResource crossCompactionTaskResource =
                    selector.selectOneTaskResources(candidate);
                if (!crossCompactionTaskResource.isValid()) {
                  throw new RuntimeException("compaction task resource is not valid");
                }
                if (crossCompactionTaskResource.getSeqFiles().size() != 5) {
                  throw new RuntimeException("selected seq file num is not 5");
                }
                if (crossCompactionTaskResource.getUnseqFiles().size() != 5) {
                  throw new RuntimeException("selected unseq file num is not 5");
                }
                candidate.releaseReadLock();

                // the other thread holds write lock and delete file successfully before setting
                // file status to COMPACTION_CANDIDATE

                CrossSpaceCompactionTask crossSpaceCompactionTask =
                    new CrossSpaceCompactionTask(
                        0,
                        tsFileManager,
                        crossCompactionTaskResource.getSeqFiles(),
                        crossCompactionTaskResource.getUnseqFiles(),
                        IoTDBDescriptor.getInstance()
                            .getConfig()
                            .getCrossCompactionPerformer()
                            .createInstance(),
                        CompactionTaskManager.currentTaskNum,
                        crossCompactionTaskResource.getTotalMemoryCost(),
                        tsFileManager.getNextCompactionTaskId());
                // set file status to COMPACTION_CANDIDATE and add into queue
                if (!crossSpaceCompactionTask.setSourceFilesToCompactionCandidate()) {
                  throw new RuntimeException("fail to set status to compaction candidate.");
                }

                // the other thread delete the file successfully when the compaction task is blocked
                // in the queue
                Thread.sleep(1000);

                if (crossSpaceCompactionTask.checkValidAndSetMerging()) {
                  throw new RuntimeException("cross space compaction task should be invalid.");
                }

                for (int i = 0; i < unseqResources.size(); i++) {
                  TsFileResource resource = unseqResources.get(i);
                  if (i == 1) {
                    if (resource.getStatus() != TsFileResourceStatus.DELETED) {
                      throw new RuntimeException("status should be DELETED");
                    }
                  } else if (resource.getStatus() != TsFileResourceStatus.NORMAL) {
                    throw new RuntimeException("status should be NORMAL");
                  }
                }
                for (int i = 0; i < seqResources.size(); i++) {
                  TsFileResource resource = seqResources.get(i);
                  if (resource.getStatus() != TsFileResourceStatus.NORMAL) {
                    throw new RuntimeException("status should be NORMAL");
                  }
                }
              } catch (Exception e) {
                fail.set(true);
                e.printStackTrace();
              }
            });

    // delete seq files
    Thread thread2 =
        new Thread(
            () -> {
              try {
                Thread.sleep(500);
                TsFileResource resource = unseqResources.get(1);
                // try to delete file
                resource.writeLock();
                resource.remove();
                resource.writeUnlock();
              } catch (Exception e) {
                fail.set(true);
                e.printStackTrace();
              }
            });

    thread1.start();
    thread2.start();
    thread1.join(10000);
    thread2.join(10000);
    if (fail.get()) {
      Assert.fail();
    }
  }

  @Test
  public void testUnSeqFileWithFileIndexBeenDeletedDuringSelection()
      throws IOException, MetadataException, WriteProcessException, InterruptedException {
    createFiles(5, 2, 3, 50, 0, 10000, 50, 50, false, true);
    createFiles(5, 2, 3, 50, 0, 10000, 50, 50, false, false);
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);
    AtomicBoolean fail = new AtomicBoolean(false);
    unseqResources.get(0).degradeTimeIndex();
    unseqResources.get(1).degradeTimeIndex();
    unseqResources.get(2).degradeTimeIndex();
    seqResources.get(1).degradeTimeIndex();
    // select files in cross compaction
    Thread thread1 =
        new Thread(
            () -> {
              try {
                RewriteCrossSpaceCompactionSelector selector =
                    new RewriteCrossSpaceCompactionSelector("", "", 0, null);
                // copy candidate source file list, hold read lock
                CrossSpaceCompactionCandidate candidate =
                    new CrossSpaceCompactionCandidate(
                        seqResources, unseqResources, System.currentTimeMillis() - Long.MAX_VALUE);
                candidate.addReadLock();

                // other thread want to hold write lock to delete files, but is stuck
                Thread.sleep(1000);
                CrossCompactionTaskResource crossCompactionTaskResource =
                    selector.selectOneTaskResources(candidate);
                if (crossCompactionTaskResource.getSeqFiles().size() != 5) {
                  throw new RuntimeException("selected seq file num is not 5");
                }
                if (crossCompactionTaskResource.getUnseqFiles().size() != 5) {
                  throw new RuntimeException("selected unseq file num is not 5");
                }
                candidate.releaseReadLock();
              } catch (Exception e) {
                fail.set(true);
                e.printStackTrace();
              }
            });

    // delete seq files
    Thread thread2 =
        new Thread(
            () -> {
              try {
                Thread.sleep(200);
                TsFileResource resource = unseqResources.get(1);
                // try to delete file
                resource.writeLock();
                resource.remove();
                resource.writeUnlock();
              } catch (Exception e) {
                fail.set(true);
                e.printStackTrace();
              }
            });

    thread1.start();
    thread2.start();
    thread1.join(10000);
    thread2.join(10000);
    if (fail.get()) {
      Assert.fail();
    }
  }
}
