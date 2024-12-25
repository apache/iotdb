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
package org.apache.iotdb.db.storageengine.dataregion.compaction.cross;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.storageengine.dataregion.compaction.AbstractCompactionTest;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.exception.FileCannotTransitToCompactingException;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.AbstractCompactionTask;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.CrossSpaceCompactionTask;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.CompactionScheduleContext;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.CompactionTaskQueue;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.comparator.DefaultCompactionTaskComparatorImpl;
import org.apache.iotdb.db.storageengine.dataregion.compaction.selector.impl.RewriteCrossSpaceCompactionSelector;
import org.apache.iotdb.db.storageengine.dataregion.compaction.selector.utils.CrossCompactionTaskResource;
import org.apache.iotdb.db.storageengine.dataregion.compaction.selector.utils.CrossSpaceCompactionCandidate;
import org.apache.iotdb.db.storageengine.dataregion.read.control.FileReaderManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;
import org.apache.iotdb.db.storageengine.rescon.memory.SystemInfo;
import org.apache.iotdb.db.utils.datastructure.FixedPriorityBlockingQueue;

import org.apache.tsfile.exception.write.WriteProcessException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
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
        new RewriteCrossSpaceCompactionSelector("", "", 0, null, new CompactionScheduleContext());
    List<CrossCompactionTaskResource> selected =
        selector.selectCrossSpaceTask(seqResources, unseqResources);
    Assert.assertEquals(0, selected.size());
  }

  @Test
  public void testSelectWithOneUnclosedSeqFile()
      throws IOException, MetadataException, WriteProcessException {
    createFiles(1, 2, 3, 50, 0, 10000, 50, 50, false, true);
    createFiles(5, 2, 3, 50, 0, 10000, 50, 50, false, false);
    seqResources.get(0).setStatusForTest(TsFileResourceStatus.UNCLOSED);
    RewriteCrossSpaceCompactionSelector selector =
        new RewriteCrossSpaceCompactionSelector("", "", 0, null, new CompactionScheduleContext());
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
        new RewriteCrossSpaceCompactionSelector("", "", 0, null, new CompactionScheduleContext());
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
    seqResources.get(1).setStatusForTest(TsFileResourceStatus.UNCLOSED);
    RewriteCrossSpaceCompactionSelector selector =
        new RewriteCrossSpaceCompactionSelector("", "", 0, null, new CompactionScheduleContext());
    List<CrossCompactionTaskResource> selected =
        selector.selectCrossSpaceTask(seqResources, unseqResources);
    Assert.assertEquals(1, selected.size());
    Assert.assertEquals(1, selected.get(0).getSeqFiles().size());
    Assert.assertEquals(1, selected.get(0).getUnseqFiles().size());

    createFiles(1, 2, 3, 200, 200, 10000, 50, 50, false, true);
    seqResources.get(1).setStatusForTest(TsFileResourceStatus.NORMAL);
    seqResources.get(2).setStatusForTest(TsFileResourceStatus.UNCLOSED);
    selected = selector.selectCrossSpaceTask(seqResources, unseqResources);
    Assert.assertEquals(1, selected.size());
    Assert.assertEquals(2, selected.get(0).getSeqFiles().size());
    Assert.assertEquals(2, selected.get(0).getUnseqFiles().size());

    createFiles(1, 2, 3, 200, 1000, 10000, 50, 50, false, true);
    createFiles(1, 2, 3, 200, 2000, 10000, 50, 50, false, true);
    seqResources.get(2).setStatusForTest(TsFileResourceStatus.NORMAL);
    seqResources.get(4).setStatusForTest(TsFileResourceStatus.UNCLOSED);
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
        new RewriteCrossSpaceCompactionSelector("", "", 0, null, new CompactionScheduleContext());
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
        new RewriteCrossSpaceCompactionSelector("", "", 0, null, new CompactionScheduleContext());
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
  public void testSelectWithTooManySourceFiles()
      throws IOException,
          MetadataException,
          WriteProcessException,
          InterruptedException,
          ExecutionException {
    int oldMaxFileNumForCompaction = SystemInfo.getInstance().getTotalFileLimitForCompaction();
    SystemInfo.getInstance().setTotalFileLimitForCompactionTask(1);
    SystemInfo.getInstance().getCompactionFileNumCost().set(0);
    SystemInfo.getInstance().getCompactionMemoryCost().set(0);
    try {
      createFiles(19, 2, 3, 50, 0, 10000, 50, 50, false, true);
      createFiles(1, 2, 3, 3000, 0, 10000, 50, 50, false, false);
      RewriteCrossSpaceCompactionSelector selector =
          new RewriteCrossSpaceCompactionSelector("", "", 0, null, new CompactionScheduleContext());
      List<CrossCompactionTaskResource> selected =
          selector.selectCrossSpaceTask(seqResources, unseqResources);
      Assert.assertEquals(1, selected.size());
      Assert.assertEquals(19, selected.get(0).getSeqFiles().size());
      Assert.assertEquals(1, selected.get(0).getUnseqFiles().size());

      CrossSpaceCompactionTask crossSpaceCompactionTask =
          new CrossSpaceCompactionTask(
              0,
              tsFileManager,
              selected.get(0).getSeqFiles(),
              selected.get(0).getUnseqFiles(),
              IoTDBDescriptor.getInstance()
                  .getConfig()
                  .getCrossCompactionPerformer()
                  .createInstance(),
              1000,
              tsFileManager.getNextCompactionTaskId());
      // set file status to COMPACTION_CANDIDATE
      Assert.assertTrue(crossSpaceCompactionTask.setSourceFilesToCompactionCandidate());

      FixedPriorityBlockingQueue<AbstractCompactionTask> queue =
          new CompactionTaskQueue(50, new DefaultCompactionTaskComparatorImpl());
      queue.put(crossSpaceCompactionTask);
      Thread thread =
          new Thread(
              () -> {
                try {
                  AbstractCompactionTask task = queue.take();
                  Assert.fail();
                } catch (InterruptedException ignored) {
                }
              });
      thread.start();
      thread.join(TimeUnit.SECONDS.toMillis(2));
      Assert.assertEquals(0, SystemInfo.getInstance().getCompactionMemoryCost().get());
      Assert.assertEquals(0, SystemInfo.getInstance().getCompactionFileNumCost().get());
      for (TsFileResource resource : seqResources) {
        Assert.assertEquals(TsFileResourceStatus.NORMAL, resource.getStatus());
      }
      for (TsFileResource resource : unseqResources) {
        Assert.assertEquals(TsFileResourceStatus.NORMAL, resource.getStatus());
      }
      thread.interrupt();
      thread.join();
    } finally {
      SystemInfo.getInstance().setTotalFileLimitForCompactionTask(oldMaxFileNumForCompaction);
    }
  }

  @Test
  public void testSeqFileWithDeviceIndexBeenDeletedBeforeSelection()
      throws IOException, MetadataException, WriteProcessException, InterruptedException {
    createFiles(5, 2, 3, 50, 0, 10000, 50, 50, false, true);
    createFiles(5, 2, 3, 50, 0, 10000, 50, 50, false, false);
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);
    AtomicBoolean fail = new AtomicBoolean(false);
    CountDownLatch cd1 = new CountDownLatch(1);
    CountDownLatch cd2 = new CountDownLatch(1);

    Thread thread1 =
        new Thread(
            () -> {
              try {
                // the file is deleted before selection
                cd1.countDown();
                cd2.await();
                RewriteCrossSpaceCompactionSelector selector =
                    new RewriteCrossSpaceCompactionSelector(
                        "", "", 0, null, new CompactionScheduleContext());
                CrossSpaceCompactionCandidate candidate =
                    new CrossSpaceCompactionCandidate(
                        seqResources, unseqResources, System.currentTimeMillis() - Long.MAX_VALUE);

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
                cd1.await();
                TsFileResource resource = seqResources.get(1);
                // try to delete file
                resource.writeLock();
                resource.remove();
                resource.writeUnlock();
                cd2.countDown();
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
  public void testSeqFileWithDeviceIndexBeenDeletedDuringSelectionAndAfterCopyingList()
      throws IOException, MetadataException, WriteProcessException, InterruptedException {
    createFiles(5, 2, 3, 50, 0, 10000, 50, 50, false, true);
    createFiles(5, 2, 3, 50, 0, 10000, 50, 50, false, false);
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);
    AtomicBoolean fail = new AtomicBoolean(false);
    CountDownLatch cd1 = new CountDownLatch(1);
    CountDownLatch cd2 = new CountDownLatch(1);

    // select files in cross compaction
    Thread thread1 =
        new Thread(
            () -> {
              try {
                RewriteCrossSpaceCompactionSelector selector =
                    new RewriteCrossSpaceCompactionSelector(
                        "", "", 0, null, new CompactionScheduleContext());
                // copy candidate source file list and add read lock
                CrossSpaceCompactionCandidate candidate =
                    new CrossSpaceCompactionCandidate(
                        seqResources, unseqResources, System.currentTimeMillis() - Long.MAX_VALUE);

                // the other thread holds write lock and delete file successfully after copying list
                cd1.countDown();
                cd2.await();
                CrossCompactionTaskResource crossCompactionTaskResource =
                    selector.selectOneTaskResources(candidate);
                if (!crossCompactionTaskResource.isValid()) {
                  throw new RuntimeException("compaction task resource is not valid");
                }
                if (crossCompactionTaskResource.getSeqFiles().size() != 1) {
                  throw new RuntimeException("selected seq file should be 1");
                }
                if (crossCompactionTaskResource.getUnseqFiles().size() != 1) {
                  throw new RuntimeException("selected unseq file num should be 1");
                }

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
                        crossCompactionTaskResource.getTotalMemoryCost(),
                        tsFileManager.getNextCompactionTaskId());
                // set file status to COMPACTION_CANDIDATE
                if (!crossSpaceCompactionTask.setSourceFilesToCompactionCandidate()) {
                  throw new RuntimeException("set status should be true");
                }
                for (int i = 0; i < seqResources.size(); i++) {
                  TsFileResource resource = seqResources.get(i);
                  if (i < 1) {
                    if (resource.getStatus() != TsFileResourceStatus.COMPACTION_CANDIDATE) {
                      throw new RuntimeException("status should be COMPACTION_CANDIDATE");
                    }
                  } else if (i == 1) {
                    if (resource.getStatus() != TsFileResourceStatus.DELETED) {
                      throw new RuntimeException("status should be DELETED");
                    }
                  } else if (resource.getStatus() != TsFileResourceStatus.NORMAL) {
                    throw new RuntimeException("status should be NORMAL");
                  }
                }
                for (int i = 0; i < unseqResources.size(); i++) {
                  TsFileResource resource = unseqResources.get(i);
                  if (i < 1) {
                    if (resource.getStatus() != TsFileResourceStatus.COMPACTION_CANDIDATE) {
                      throw new RuntimeException("status should be COMPACTION_CANDIDATE");
                    }
                  } else if (resource.getStatus() != TsFileResourceStatus.NORMAL) {
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
                cd1.await();
                TsFileResource resource = seqResources.get(1);
                // try to delete file
                resource.writeLock();
                resource.remove();
                resource.writeUnlock();
                cd2.countDown();
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
  public void testSeqFileWithDeviceIndexBeenDeletedDuringSelectionAndBeforeSettingCandidate()
      throws IOException, MetadataException, WriteProcessException, InterruptedException {
    createFiles(5, 2, 3, 50, 0, 10000, 50, 50, false, true);
    createFiles(5, 2, 3, 50, 0, 10000, 50, 50, false, false);
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);
    AtomicBoolean fail = new AtomicBoolean(false);
    CountDownLatch cd1 = new CountDownLatch(1);
    CountDownLatch cd2 = new CountDownLatch(1);

    // select files in cross compaction
    Thread thread1 =
        new Thread(
            () -> {
              try {
                RewriteCrossSpaceCompactionSelector selector =
                    new RewriteCrossSpaceCompactionSelector(
                        "", "", 0, null, new CompactionScheduleContext());
                // copy candidate source file list and add read lock
                CrossSpaceCompactionCandidate candidate =
                    new CrossSpaceCompactionCandidate(
                        seqResources, unseqResources, System.currentTimeMillis() - Long.MAX_VALUE);

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

                // the other thread holds write lock and delete file successfully before setting
                // file status to COMPACTION_CANDIDATE
                cd1.countDown();
                cd2.await();

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
                cd1.await();
                TsFileResource resource = seqResources.get(1);
                // try to delete file
                resource.writeLock();
                resource.remove();
                resource.writeUnlock();
                cd2.countDown();
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
    CountDownLatch cd1 = new CountDownLatch(1);
    CountDownLatch cd2 = new CountDownLatch(1);

    // select files in cross compaction
    Thread thread1 =
        new Thread(
            () -> {
              try {
                RewriteCrossSpaceCompactionSelector selector =
                    new RewriteCrossSpaceCompactionSelector(
                        "", "", 0, null, new CompactionScheduleContext());
                // copy candidate source file list and add read lock
                CrossSpaceCompactionCandidate candidate =
                    new CrossSpaceCompactionCandidate(
                        seqResources, unseqResources, System.currentTimeMillis() - Long.MAX_VALUE);

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
                        crossCompactionTaskResource.getTotalMemoryCost(),
                        tsFileManager.getNextCompactionTaskId());
                // set file status to COMPACTION_CANDIDATE and add into queue
                if (!crossSpaceCompactionTask.setSourceFilesToCompactionCandidate()) {
                  throw new RuntimeException("fail to set status to compaction candidate.");
                }

                // the other thread delete the file successfully when the compaction task is blocked
                // in the queue
                cd1.countDown();
                cd2.await();

                boolean exceptionCaught = false;

                Assert.assertThrows(
                    "cross space compaction task should be invalid.",
                    FileCannotTransitToCompactingException.class,
                    crossSpaceCompactionTask::transitSourceFilesToMerging);

                crossSpaceCompactionTask.resetCompactionCandidateStatusForAllSourceFiles();
                crossSpaceCompactionTask.handleTaskCleanup();

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
                cd1.await();
                TsFileResource resource = seqResources.get(1);
                // try to delete file
                resource.writeLock();
                resource.remove();
                resource.writeUnlock();
                cd2.countDown();
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
  public void testSeqFileWithFileIndexBeenDeletedDuringSelectionAndAfterCopyingList()
      throws IOException, MetadataException, WriteProcessException, InterruptedException {
    createFiles(5, 2, 3, 50, 0, 10000, 50, 50, false, true);
    createFiles(5, 2, 3, 50, 0, 10000, 50, 50, false, false);
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);
    AtomicBoolean fail = new AtomicBoolean(false);
    CountDownLatch cd1 = new CountDownLatch(1);
    CountDownLatch cd2 = new CountDownLatch(1);
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
                    new RewriteCrossSpaceCompactionSelector(
                        "", "", 0, null, new CompactionScheduleContext());
                // copy candidate source file list and add read lock
                CrossSpaceCompactionCandidate candidate =
                    new CrossSpaceCompactionCandidate(
                        seqResources, unseqResources, System.currentTimeMillis() - Long.MAX_VALUE);

                // the other thread holds write lock and delete file successfully after copying list
                cd1.countDown();
                cd2.await();

                CrossCompactionTaskResource crossCompactionTaskResource =
                    selector.selectOneTaskResources(candidate);
                if (!crossCompactionTaskResource.isValid()) {
                  throw new RuntimeException("compaction task resource is not valid");
                }
                if (crossCompactionTaskResource.getSeqFiles().size() != 1) {
                  throw new RuntimeException("selected seq file num should be 1");
                }
                if (crossCompactionTaskResource.getUnseqFiles().size() != 1) {
                  throw new RuntimeException("selected unseq file num should be 1");
                }

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
                        crossCompactionTaskResource.getTotalMemoryCost(),
                        tsFileManager.getNextCompactionTaskId());
                // set file status to COMPACTION_CANDIDATE
                if (!crossSpaceCompactionTask.setSourceFilesToCompactionCandidate()) {
                  throw new RuntimeException("set status should be true");
                }
                for (int i = 0; i < seqResources.size(); i++) {
                  TsFileResource resource = seqResources.get(i);
                  if (i < 1) {
                    if (resource.getStatus() != TsFileResourceStatus.COMPACTION_CANDIDATE) {
                      throw new RuntimeException("status should be COMPACTION_CANDIDATE");
                    }
                  } else if (i == 1) {
                    if (resource.getStatus() != TsFileResourceStatus.DELETED) {
                      throw new RuntimeException("status should be DELETED");
                    }
                  } else if (resource.getStatus() != TsFileResourceStatus.NORMAL) {
                    throw new RuntimeException("status should be NORMAL");
                  }
                }
                for (int i = 0; i < unseqResources.size(); i++) {
                  TsFileResource resource = unseqResources.get(i);
                  if (i < 1) {
                    if (resource.getStatus() != TsFileResourceStatus.COMPACTION_CANDIDATE) {
                      throw new RuntimeException("status should be COMPACTION_CANDIDATE");
                    }
                  } else if (resource.getStatus() != TsFileResourceStatus.NORMAL) {
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
                cd1.await();
                TsFileResource resource = seqResources.get(1);
                // try to delete file
                resource.writeLock();
                resource.remove();
                resource.writeUnlock();
                cd2.countDown();
              } catch (Exception e) {
                fail.set(true);
                e.printStackTrace();
              }
            });

    thread1.start();
    thread2.start();
    thread1.join();
    thread2.join();
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
    CountDownLatch cd1 = new CountDownLatch(1);
    CountDownLatch cd2 = new CountDownLatch(1);
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
                    new RewriteCrossSpaceCompactionSelector(
                        "", "", 0, null, new CompactionScheduleContext());
                // copy candidate source file list and add read lock
                CrossSpaceCompactionCandidate candidate =
                    new CrossSpaceCompactionCandidate(
                        seqResources, unseqResources, System.currentTimeMillis() - Long.MAX_VALUE);

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

                // the other thread holds write lock and delete file successfully before setting
                // file status to COMPACTION_CANDIDATE
                cd1.countDown();
                cd2.await();

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
                cd1.await();
                TsFileResource resource = seqResources.get(1);
                // try to delete file
                resource.writeLock();
                resource.remove();
                resource.writeUnlock();
                cd2.countDown();
              } catch (Exception e) {
                fail.set(true);
                e.printStackTrace();
              }
            });

    thread1.start();
    thread2.start();
    thread1.join();
    thread2.join();
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
    CountDownLatch cd1 = new CountDownLatch(1);
    CountDownLatch cd2 = new CountDownLatch(1);
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
                    new RewriteCrossSpaceCompactionSelector(
                        "", "", 0, null, new CompactionScheduleContext());
                // copy candidate source file list and add read lock
                CrossSpaceCompactionCandidate candidate =
                    new CrossSpaceCompactionCandidate(
                        seqResources, unseqResources, System.currentTimeMillis() - Long.MAX_VALUE);

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
                        crossCompactionTaskResource.getTotalMemoryCost(),
                        tsFileManager.getNextCompactionTaskId());
                // set file status to COMPACTION_CANDIDATE and add into queue
                if (!crossSpaceCompactionTask.setSourceFilesToCompactionCandidate()) {
                  throw new RuntimeException("fail to set status to compaction candidate.");
                }

                // the other thread delete the file successfully when the compaction task is blocked
                // in the queue
                cd1.countDown();
                cd2.await();

                Assert.assertThrows(
                    "cross space compaction task should be invalid.",
                    FileCannotTransitToCompactingException.class,
                    crossSpaceCompactionTask::transitSourceFilesToMerging);

                crossSpaceCompactionTask.resetCompactionCandidateStatusForAllSourceFiles();
                crossSpaceCompactionTask.handleTaskCleanup();

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
                cd1.await();
                TsFileResource resource = seqResources.get(1);
                // try to delete file
                resource.writeLock();
                resource.remove();
                resource.writeUnlock();
                cd2.countDown();
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
  public void testSeqFileWithFileIndexBeenDeletedBeforeSelection()
      throws IOException, MetadataException, WriteProcessException, InterruptedException {
    createFiles(5, 2, 3, 50, 0, 10000, 50, 50, false, true);
    createFiles(5, 2, 3, 50, 0, 10000, 50, 50, false, false);
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);
    AtomicBoolean fail = new AtomicBoolean(false);
    CountDownLatch cd1 = new CountDownLatch(1);
    CountDownLatch cd2 = new CountDownLatch(1);
    seqResources.get(1).degradeTimeIndex();
    seqResources.get(2).degradeTimeIndex();
    unseqResources.get(1).degradeTimeIndex();

    Thread thread1 =
        new Thread(
            () -> {
              try {
                // the file is deleted before selection
                cd1.countDown();
                cd2.await();
                RewriteCrossSpaceCompactionSelector selector =
                    new RewriteCrossSpaceCompactionSelector(
                        "", "", 0, null, new CompactionScheduleContext());
                CrossSpaceCompactionCandidate candidate =
                    new CrossSpaceCompactionCandidate(
                        seqResources, unseqResources, System.currentTimeMillis() - Long.MAX_VALUE);

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
                cd1.await();
                TsFileResource resource = seqResources.get(1);
                // try to delete file
                resource.writeLock();
                resource.remove();
                resource.writeUnlock();
                cd2.countDown();
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
  public void testUnSeqFileWithDeviceIndexBeenDeletedBeforeSelection()
      throws IOException, MetadataException, WriteProcessException, InterruptedException {
    createFiles(5, 2, 3, 50, 0, 10000, 50, 50, false, true);
    createFiles(5, 2, 3, 50, 0, 10000, 50, 50, false, false);
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);
    AtomicBoolean fail = new AtomicBoolean(false);
    CountDownLatch cd1 = new CountDownLatch(1);
    CountDownLatch cd2 = new CountDownLatch(1);

    Thread thread1 =
        new Thread(
            () -> {
              try {
                // the file is deleted before selection
                cd1.countDown();
                cd2.await();
                RewriteCrossSpaceCompactionSelector selector =
                    new RewriteCrossSpaceCompactionSelector(
                        "", "", 0, null, new CompactionScheduleContext());
                CrossSpaceCompactionCandidate candidate =
                    new CrossSpaceCompactionCandidate(
                        seqResources, unseqResources, System.currentTimeMillis() - Long.MAX_VALUE);

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
                cd1.await();
                TsFileResource resource = unseqResources.get(1);
                // try to delete file
                resource.writeLock();
                resource.remove();
                resource.writeUnlock();
                cd2.countDown();
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
  public void testUnSeqFileWithDeviceIndexBeenDeletedDuringSelectionAndAfterCopyingList()
      throws IOException, MetadataException, WriteProcessException, InterruptedException {
    createFiles(5, 2, 3, 50, 0, 10000, 50, 50, false, true);
    createFiles(5, 2, 3, 50, 0, 10000, 50, 50, false, false);
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);
    AtomicBoolean fail = new AtomicBoolean(false);
    CountDownLatch cd1 = new CountDownLatch(1);
    CountDownLatch cd2 = new CountDownLatch(1);

    // select files in cross compaction
    Thread thread1 =
        new Thread(
            () -> {
              try {
                RewriteCrossSpaceCompactionSelector selector =
                    new RewriteCrossSpaceCompactionSelector(
                        "", "", 0, null, new CompactionScheduleContext());
                // copy candidate source file list and add read lock
                CrossSpaceCompactionCandidate candidate =
                    new CrossSpaceCompactionCandidate(
                        seqResources, unseqResources, System.currentTimeMillis() - Long.MAX_VALUE);

                // the other thread holds write lock and delete file successfully after copying list
                cd1.countDown();
                cd2.await();

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
                        crossCompactionTaskResource.getTotalMemoryCost(),
                        tsFileManager.getNextCompactionTaskId());
                // set file status to COMPACTION_CANDIDATE
                if (!crossSpaceCompactionTask.setSourceFilesToCompactionCandidate()) {
                  throw new RuntimeException("set status should be true");
                }
                for (int i = 0; i < unseqResources.size(); i++) {
                  TsFileResource resource = unseqResources.get(i);
                  if (i < 1) {
                    if (resource.getStatus() != TsFileResourceStatus.COMPACTION_CANDIDATE) {
                      throw new RuntimeException("status should be COMPACTION_CANDIDATE");
                    }
                  } else if (i == 1) {
                    if (resource.getStatus() != TsFileResourceStatus.DELETED) {
                      throw new RuntimeException("status should be DELETED");
                    }
                  } else if (resource.getStatus() != TsFileResourceStatus.NORMAL) {
                    throw new RuntimeException("status should be NORMAL");
                  }
                }
                for (int i = 0; i < seqResources.size(); i++) {
                  TsFileResource resource = seqResources.get(i);
                  if (i < 1) {
                    if (resource.getStatus() != TsFileResourceStatus.COMPACTION_CANDIDATE) {
                      throw new RuntimeException("status should be COMPACTION_CANDIDATE");
                    }
                  } else if (resource.getStatus() != TsFileResourceStatus.NORMAL) {
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
                cd1.await();
                TsFileResource resource = unseqResources.get(1);
                // try to delete file
                resource.writeLock();
                resource.remove();
                resource.writeUnlock();
                cd2.countDown();
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
  public void testUnSeqFileWithDeviceIndexBeenDeletedDuringSelectionAndBeforeSettingCandidate()
      throws IOException, MetadataException, WriteProcessException, InterruptedException {
    createFiles(5, 2, 3, 50, 0, 10000, 50, 50, false, true);
    createFiles(5, 2, 3, 50, 0, 10000, 50, 50, false, false);
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);
    AtomicBoolean fail = new AtomicBoolean(false);
    CountDownLatch cd1 = new CountDownLatch(1);
    CountDownLatch cd2 = new CountDownLatch(1);

    // select files in cross compaction
    Thread thread1 =
        new Thread(
            () -> {
              try {
                RewriteCrossSpaceCompactionSelector selector =
                    new RewriteCrossSpaceCompactionSelector(
                        "", "", 0, null, new CompactionScheduleContext());
                // copy candidate source file list and add read lock
                CrossSpaceCompactionCandidate candidate =
                    new CrossSpaceCompactionCandidate(
                        seqResources, unseqResources, System.currentTimeMillis() - Long.MAX_VALUE);

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

                // the other thread holds write lock and delete file successfully before setting
                // file status to COMPACTION_CANDIDATE
                cd1.countDown();
                cd2.await();

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
                cd1.await();
                TsFileResource resource = unseqResources.get(1);
                // try to delete file
                resource.writeLock();
                resource.remove();
                resource.writeUnlock();
                cd2.countDown();
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
    CountDownLatch cd1 = new CountDownLatch(1);
    CountDownLatch cd2 = new CountDownLatch(1);

    // select files in cross compaction
    Thread thread1 =
        new Thread(
            () -> {
              try {
                RewriteCrossSpaceCompactionSelector selector =
                    new RewriteCrossSpaceCompactionSelector(
                        "", "", 0, null, new CompactionScheduleContext());
                // copy candidate source file list and add read lock
                CrossSpaceCompactionCandidate candidate =
                    new CrossSpaceCompactionCandidate(
                        seqResources, unseqResources, System.currentTimeMillis() - Long.MAX_VALUE);

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
                        crossCompactionTaskResource.getTotalMemoryCost(),
                        tsFileManager.getNextCompactionTaskId());
                // set file status to COMPACTION_CANDIDATE and add into queue
                if (!crossSpaceCompactionTask.setSourceFilesToCompactionCandidate()) {
                  throw new RuntimeException("fail to set status to compaction candidate.");
                }

                // the other thread delete the file successfully when the compaction task is blocked
                // in the queue
                cd1.countDown();
                cd2.await();

                Assert.assertThrows(
                    "cross space compaction task should be invalid.",
                    FileCannotTransitToCompactingException.class,
                    crossSpaceCompactionTask::transitSourceFilesToMerging);

                crossSpaceCompactionTask.resetCompactionCandidateStatusForAllSourceFiles();
                crossSpaceCompactionTask.handleTaskCleanup();

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
                cd1.await();
                TsFileResource resource = unseqResources.get(1);
                // try to delete file
                resource.writeLock();
                resource.remove();
                resource.writeUnlock();
                cd2.countDown();
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
  public void testUnSeqFileWithFileIndexBeenDeletedDuringSelectionAndAfterCopyingList()
      throws IOException, MetadataException, WriteProcessException, InterruptedException {
    createFiles(5, 2, 3, 50, 0, 10000, 50, 50, false, true);
    createFiles(5, 2, 3, 50, 0, 10000, 50, 50, false, false);
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);
    AtomicBoolean fail = new AtomicBoolean(false);
    CountDownLatch cd1 = new CountDownLatch(1);
    CountDownLatch cd2 = new CountDownLatch(1);
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
                    new RewriteCrossSpaceCompactionSelector(
                        "", "", 0, null, new CompactionScheduleContext());
                // copy candidate source file list and add read lock
                CrossSpaceCompactionCandidate candidate =
                    new CrossSpaceCompactionCandidate(
                        seqResources, unseqResources, System.currentTimeMillis() - Long.MAX_VALUE);

                // the other thread holds write lock and delete file successfully after copying list
                cd1.countDown();
                cd2.await();
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
                        crossCompactionTaskResource.getTotalMemoryCost(),
                        tsFileManager.getNextCompactionTaskId());
                // set file status to COMPACTION_CANDIDATE
                if (!crossSpaceCompactionTask.setSourceFilesToCompactionCandidate()) {
                  throw new RuntimeException("set status should be true");
                }
                for (int i = 0; i < unseqResources.size(); i++) {
                  TsFileResource resource = unseqResources.get(i);
                  if (i < 1) {
                    if (resource.getStatus() != TsFileResourceStatus.COMPACTION_CANDIDATE) {
                      throw new RuntimeException("status should be COMPACTION_CANDIDATE");
                    }
                  } else if (i == 1) {
                    if (resource.getStatus() != TsFileResourceStatus.DELETED) {
                      throw new RuntimeException("status should be DELETED");
                    }
                  } else if (resource.getStatus() != TsFileResourceStatus.NORMAL) {
                    throw new RuntimeException("status should be NORMAL");
                  }
                }
                for (int i = 0; i < seqResources.size(); i++) {
                  TsFileResource resource = seqResources.get(i);
                  if (i < 1) {
                    if (resource.getStatus() != TsFileResourceStatus.COMPACTION_CANDIDATE) {
                      throw new RuntimeException("status should be COMPACTION_CANDIDATE");
                    }
                  } else if (resource.getStatus() != TsFileResourceStatus.NORMAL) {
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
                cd1.await();
                TsFileResource resource = unseqResources.get(1);
                // try to delete file
                resource.writeLock();
                resource.remove();
                resource.writeUnlock();
                cd2.countDown();
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
    CountDownLatch cd1 = new CountDownLatch(1);
    CountDownLatch cd2 = new CountDownLatch(1);
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
                    new RewriteCrossSpaceCompactionSelector(
                        "", "", 0, null, new CompactionScheduleContext());
                // copy candidate source file list and add read lock
                CrossSpaceCompactionCandidate candidate =
                    new CrossSpaceCompactionCandidate(
                        seqResources, unseqResources, System.currentTimeMillis() - Long.MAX_VALUE);

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

                // the other thread holds write lock and delete file successfully before setting
                // file status to COMPACTION_CANDIDATE
                cd1.countDown();
                cd2.await();

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
                cd1.await();
                TsFileResource resource = unseqResources.get(1);
                // try to delete file
                resource.writeLock();
                resource.remove();
                resource.writeUnlock();
                cd2.countDown();
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
    CountDownLatch cd1 = new CountDownLatch(1);
    CountDownLatch cd2 = new CountDownLatch(1);
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
                    new RewriteCrossSpaceCompactionSelector(
                        "", "", 0, null, new CompactionScheduleContext());
                // copy candidate source file list and add read lock
                CrossSpaceCompactionCandidate candidate =
                    new CrossSpaceCompactionCandidate(
                        seqResources, unseqResources, System.currentTimeMillis() - Long.MAX_VALUE);

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
                        crossCompactionTaskResource.getTotalMemoryCost(),
                        tsFileManager.getNextCompactionTaskId());
                // set file status to COMPACTION_CANDIDATE and add into queue
                if (!crossSpaceCompactionTask.setSourceFilesToCompactionCandidate()) {
                  throw new RuntimeException("fail to set status to compaction candidate.");
                }

                // the other thread delete the file successfully when the compaction task is blocked
                // in the queue
                cd1.countDown();
                cd2.await();

                Assert.assertThrows(
                    "cross space compaction task should be invalid.",
                    FileCannotTransitToCompactingException.class,
                    crossSpaceCompactionTask::transitSourceFilesToMerging);
                crossSpaceCompactionTask.resetCompactionCandidateStatusForAllSourceFiles();
                crossSpaceCompactionTask.handleTaskCleanup();

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
                cd1.await();
                TsFileResource resource = unseqResources.get(1);
                // try to delete file
                resource.writeLock();
                resource.remove();
                resource.writeUnlock();
                cd2.countDown();
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
  public void testUnSeqFileWithFileIndexBeenDeletedBeforeSelection()
      throws IOException, MetadataException, WriteProcessException, InterruptedException {
    createFiles(5, 2, 3, 50, 0, 10000, 50, 50, false, true);
    createFiles(5, 2, 3, 50, 0, 10000, 50, 50, false, false);
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);
    AtomicBoolean fail = new AtomicBoolean(false);
    CountDownLatch cd1 = new CountDownLatch(1);
    CountDownLatch cd2 = new CountDownLatch(1);
    unseqResources.get(0).degradeTimeIndex();
    unseqResources.get(1).degradeTimeIndex();
    unseqResources.get(2).degradeTimeIndex();
    seqResources.get(1).degradeTimeIndex();
    // select files in cross compaction
    Thread thread1 =
        new Thread(
            () -> {
              try {
                // the file is deleted before selection
                cd1.countDown();
                cd2.await();
                RewriteCrossSpaceCompactionSelector selector =
                    new RewriteCrossSpaceCompactionSelector(
                        "", "", 0, null, new CompactionScheduleContext());
                CrossSpaceCompactionCandidate candidate =
                    new CrossSpaceCompactionCandidate(
                        seqResources, unseqResources, System.currentTimeMillis() - Long.MAX_VALUE);

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
                cd1.await();
                TsFileResource resource = unseqResources.get(1);
                // try to delete file
                resource.writeLock();
                resource.remove();
                resource.writeUnlock();
                cd2.countDown();
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
}
