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
package org.apache.iotdb.db.storageengine.dataregion.compaction.inner;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.storageengine.dataregion.compaction.AbstractCompactionTest;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.exception.FileCannotTransitToCompactingException;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.impl.FastCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.InnerSpaceCompactionTask;
import org.apache.iotdb.db.storageengine.dataregion.compaction.selector.impl.SizeTieredCompactionSelector;
import org.apache.iotdb.db.storageengine.dataregion.modification.Deletion;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModificationFile;
import org.apache.iotdb.db.storageengine.dataregion.read.control.FileReaderManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;

import org.apache.tsfile.exception.write.WriteProcessException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

public class InnerSpaceCompactionSelectorTest extends AbstractCompactionTest {
  @Before
  public void setUp()
      throws IOException, WriteProcessException, MetadataException, InterruptedException {
    super.setUp();
    IoTDBDescriptor.getInstance().getConfig().setInnerCompactionCandidateFileNum(2);
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
  public void testFileWithDeviceIndexBeenDeletedBeforeSelection()
      throws IOException, MetadataException, WriteProcessException, InterruptedException {
    createFiles(6, 2, 3, 50, 0, 10000, 50, 50, false, true);
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);
    AtomicBoolean fail = new AtomicBoolean(false);

    CountDownLatch cd = new CountDownLatch(1);

    Thread thread1 =
        new Thread(
            () -> {
              try {
                // the file is deleted before selection
                cd.await();
                SizeTieredCompactionSelector selector =
                    new SizeTieredCompactionSelector("", "", 0, true, tsFileManager);
                List<TsFileResource> resources =
                    tsFileManager.getOrCreateSequenceListByTimePartition(0);
                List<InnerSpaceCompactionTask> innerSpaceCompactionTasks =
                    selector.selectInnerSpaceTask(resources);
                if (innerSpaceCompactionTasks.size() != 2) {
                  throw new RuntimeException("task num is not 2");
                }
                if (innerSpaceCompactionTasks.get(0).getSelectedTsFileResourceList().size() != 2
                    || innerSpaceCompactionTasks.get(1).getSelectedTsFileResourceList().size()
                        != 2) {
                  throw new RuntimeException("selected file num is not 2");
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
                TsFileResource resource = seqResources.get(1);
                // try to delete file
                resource.writeLock();
                resource.remove();
                resource.writeUnlock();
                cd.countDown();
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
  public void testFileWithDeviceIndexBeenDeletedDuringSelectionAndBeforeSettingCandidate()
      throws IOException, MetadataException, WriteProcessException, InterruptedException {
    createFiles(6, 2, 3, 50, 0, 10000, 50, 50, false, true);
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
                SizeTieredCompactionSelector selector =
                    new SizeTieredCompactionSelector("", "", 0, true, tsFileManager);
                // copy candidate source file list
                List<TsFileResource> resources =
                    tsFileManager.getOrCreateSequenceListByTimePartition(0);
                List<InnerSpaceCompactionTask> innerSpaceCompactionTasks =
                    selector.selectInnerSpaceTask(resources);

                // the other thread holds write lock and delete files successfully before setting
                // status to COMPACTION_CANDIDATE
                cd1.countDown();
                cd2.await();

                if (innerSpaceCompactionTasks.size() != 3) {
                  throw new RuntimeException("task num is not 3");
                }
                for (int idx = 0; idx < innerSpaceCompactionTasks.size(); idx++) {
                  List<TsFileResource> task =
                      innerSpaceCompactionTasks.get(idx).getSelectedTsFileResourceList();
                  if (task.size() != 2) {
                    throw new RuntimeException("selected file num is not 2");
                  }
                  InnerSpaceCompactionTask innerSpaceCompactionTask =
                      new InnerSpaceCompactionTask(
                          0,
                          tsFileManager,
                          task,
                          true,
                          new FastCompactionPerformer(false),
                          tsFileManager.getNextCompactionTaskId());
                  // set file status to COMPACTION_CANDIDATE
                  if (idx == 0) {
                    if (innerSpaceCompactionTask.setSourceFilesToCompactionCandidate()) {
                      throw new RuntimeException("set status should be false");
                    }
                    for (int i = 0; i < task.size(); i++) {
                      TsFileResource resource = task.get(i);
                      if (i == 1) {
                        if (resource.getStatus() != TsFileResourceStatus.DELETED) {
                          throw new RuntimeException("status should be DELETED");
                        }
                      } else if (resource.getStatus() != TsFileResourceStatus.NORMAL) {
                        throw new RuntimeException("status should be NORMAL");
                      }
                    }
                  } else {
                    if (!innerSpaceCompactionTask.setSourceFilesToCompactionCandidate()) {
                      throw new RuntimeException("set status should be true");
                    }
                    for (int i = 0; i < task.size(); i++) {
                      TsFileResource resource = task.get(i);
                      if (resource.getStatus() != TsFileResourceStatus.COMPACTION_CANDIDATE) {
                        throw new RuntimeException("status should be COMPACTION_CANDIDATE");
                      }
                    }
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
  public void testFileWithDeviceIndexBeenDeletedDuringSelectionAndBeforeSettingCompacting()
      throws IOException, MetadataException, WriteProcessException, InterruptedException {
    createFiles(6, 2, 3, 50, 0, 10000, 50, 50, false, true);
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
                SizeTieredCompactionSelector selector =
                    new SizeTieredCompactionSelector("", "", 0, true, tsFileManager);
                // copy candidate source file list
                List<TsFileResource> resources =
                    tsFileManager.getOrCreateSequenceListByTimePartition(0);
                List<InnerSpaceCompactionTask> taskResource =
                    selector.selectInnerSpaceTask(resources);

                if (taskResource.size() != 3) {
                  throw new RuntimeException("task num is not 3");
                }
                for (int idx = 0; idx < taskResource.size(); idx++) {
                  List<TsFileResource> task = taskResource.get(idx).getSelectedTsFileResourceList();
                  if (task.size() != 2) {
                    throw new RuntimeException("selected file num is not 2");
                  }
                  InnerSpaceCompactionTask innerSpaceCompactionTask =
                      new InnerSpaceCompactionTask(
                          0,
                          tsFileManager,
                          task,
                          true,
                          new FastCompactionPerformer(false),
                          tsFileManager.getNextCompactionTaskId());

                  if (!innerSpaceCompactionTask.setSourceFilesToCompactionCandidate()) {
                    throw new RuntimeException("set status should be true");
                  }

                  // set file status to COMPACTION_CANDIDATE
                  if (idx == 0) {
                    // the other thread holds write lock and delete files successfully before
                    // setting status to COMPACTING
                    cd1.countDown();
                    cd2.await();

                    Assert.assertThrows(
                        "inner space compaction task should be invalid.",
                        FileCannotTransitToCompactingException.class,
                        innerSpaceCompactionTask::transitSourceFilesToMerging);

                    innerSpaceCompactionTask.resetCompactionCandidateStatusForAllSourceFiles();
                    innerSpaceCompactionTask.handleTaskCleanup();

                    for (int i = 0; i < task.size(); i++) {
                      TsFileResource resource = task.get(i);
                      if (i == 1) {
                        if (resource.getStatus() != TsFileResourceStatus.DELETED) {
                          throw new RuntimeException("status should be DELETED");
                        }
                      } else if (resource.getStatus() != TsFileResourceStatus.NORMAL) {
                        throw new RuntimeException("status should be NORMAL");
                      }
                    }
                  } else {
                    try {
                      innerSpaceCompactionTask.transitSourceFilesToMerging();
                    } catch (FileCannotTransitToCompactingException e) {
                      Assert.fail("inner space compaction task should be valid.");
                    }
                    for (int i = 0; i < task.size(); i++) {
                      TsFileResource resource = task.get(i);
                      if (resource.getStatus() != TsFileResourceStatus.COMPACTING) {
                        throw new RuntimeException("status should be COMPACTING");
                      }
                    }
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
  public void testFileWithFileIndexBeenDeletedBeforeSelection()
      throws IOException, MetadataException, WriteProcessException, InterruptedException {
    createFiles(6, 2, 3, 50, 0, 10000, 50, 50, false, true);
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);
    AtomicBoolean fail = new AtomicBoolean(false);
    seqResources.get(0).degradeTimeIndex();
    seqResources.get(1).degradeTimeIndex();
    seqResources.get(3).degradeTimeIndex();

    CountDownLatch cd1 = new CountDownLatch(1);
    CountDownLatch cd2 = new CountDownLatch(1);

    Thread thread1 =
        new Thread(
            () -> {
              try {
                // the file is deleted before selection
                cd1.countDown();
                cd2.await();
                SizeTieredCompactionSelector selector =
                    new SizeTieredCompactionSelector("", "", 0, true, tsFileManager);
                List<TsFileResource> resources =
                    tsFileManager.getOrCreateSequenceListByTimePartition(0);
                List<InnerSpaceCompactionTask> innerSpaceCompactionTasks =
                    selector.selectInnerSpaceTask(resources);
                if (innerSpaceCompactionTasks.size() != 2) {
                  throw new RuntimeException("task num is not 2");
                }
                if (innerSpaceCompactionTasks.get(0).getSelectedTsFileResourceList().size() != 2
                    || innerSpaceCompactionTasks.get(1).getSelectedTsFileResourceList().size()
                        != 2) {
                  throw new RuntimeException("selected file num is not 2");
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
  public void testFileWithFileIndexBeenDeletedDuringSelectionAndBeforeSettingCandidate()
      throws IOException, MetadataException, WriteProcessException, InterruptedException {
    createFiles(6, 2, 3, 50, 0, 10000, 50, 50, false, true);
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);
    AtomicBoolean fail = new AtomicBoolean(false);
    seqResources.get(0).degradeTimeIndex();
    seqResources.get(1).degradeTimeIndex();
    seqResources.get(3).degradeTimeIndex();

    CountDownLatch cd1 = new CountDownLatch(1);
    CountDownLatch cd2 = new CountDownLatch(1);

    // select files in cross compaction
    Thread thread1 =
        new Thread(
            () -> {
              try {
                SizeTieredCompactionSelector selector =
                    new SizeTieredCompactionSelector("", "", 0, true, tsFileManager);
                // copy candidate source file list
                List<TsFileResource> resources =
                    tsFileManager.getOrCreateSequenceListByTimePartition(0);
                List<InnerSpaceCompactionTask> innerSpaceCompactionTasks =
                    selector.selectInnerSpaceTask(resources);

                // the other thread holds write lock and delete files successfully before setting
                // status to COMPACTION_CANDIDATE
                cd1.countDown();
                cd2.await();

                if (innerSpaceCompactionTasks.size() != 3) {
                  throw new RuntimeException("task num is not 3");
                }
                for (int idx = 0; idx < innerSpaceCompactionTasks.size(); idx++) {
                  List<TsFileResource> task =
                      innerSpaceCompactionTasks.get(idx).getSelectedTsFileResourceList();
                  if (task.size() != 2) {
                    throw new RuntimeException("selected file num is not 2");
                  }
                  InnerSpaceCompactionTask innerSpaceCompactionTask =
                      new InnerSpaceCompactionTask(
                          0,
                          tsFileManager,
                          task,
                          true,
                          new FastCompactionPerformer(false),
                          tsFileManager.getNextCompactionTaskId());
                  // set file status to COMPACTION_CANDIDATE
                  if (idx == 0) {
                    if (innerSpaceCompactionTask.setSourceFilesToCompactionCandidate()) {
                      throw new RuntimeException("set status should be false");
                    }
                    for (int i = 0; i < task.size(); i++) {
                      TsFileResource resource = task.get(i);
                      if (i == 1) {
                        if (resource.getStatus() != TsFileResourceStatus.DELETED) {
                          throw new RuntimeException("status should be DELETED");
                        }
                      } else if (resource.getStatus() != TsFileResourceStatus.NORMAL) {
                        throw new RuntimeException("status should be NORMAL");
                      }
                    }
                  } else {
                    if (!innerSpaceCompactionTask.setSourceFilesToCompactionCandidate()) {
                      throw new RuntimeException("set status should be true");
                    }
                    for (int i = 0; i < task.size(); i++) {
                      TsFileResource resource = task.get(i);
                      if (resource.getStatus() != TsFileResourceStatus.COMPACTION_CANDIDATE) {
                        throw new RuntimeException("status should be COMPACTION_CANDIDATE");
                      }
                    }
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
  public void testFileWithFileIndexBeenDeletedDuringSelectionAndBeforeSettingCompacting()
      throws IOException, MetadataException, WriteProcessException, InterruptedException {
    createFiles(6, 2, 3, 50, 0, 10000, 50, 50, false, true);
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);
    AtomicBoolean fail = new AtomicBoolean(false);
    seqResources.get(0).degradeTimeIndex();
    seqResources.get(1).degradeTimeIndex();
    seqResources.get(3).degradeTimeIndex();

    CountDownLatch cd1 = new CountDownLatch(1);
    CountDownLatch cd2 = new CountDownLatch(1);

    // select files in cross compaction
    Thread thread1 =
        new Thread(
            () -> {
              try {
                SizeTieredCompactionSelector selector =
                    new SizeTieredCompactionSelector("", "", 0, true, tsFileManager);
                // copy candidate source file list
                List<TsFileResource> resources =
                    tsFileManager.getOrCreateSequenceListByTimePartition(0);
                List<InnerSpaceCompactionTask> innerSpaceCompactionTasks =
                    selector.selectInnerSpaceTask(resources);

                if (innerSpaceCompactionTasks.size() != 3) {
                  throw new RuntimeException("task num is not 3");
                }
                for (int idx = 0; idx < innerSpaceCompactionTasks.size(); idx++) {
                  List<TsFileResource> task =
                      innerSpaceCompactionTasks.get(idx).getSelectedTsFileResourceList();
                  if (task.size() != 2) {
                    throw new RuntimeException("selected file num is not 2");
                  }
                  InnerSpaceCompactionTask innerSpaceCompactionTask =
                      new InnerSpaceCompactionTask(
                          0,
                          tsFileManager,
                          task,
                          true,
                          new FastCompactionPerformer(false),
                          tsFileManager.getNextCompactionTaskId());

                  if (!innerSpaceCompactionTask.setSourceFilesToCompactionCandidate()) {
                    throw new RuntimeException("set status should be true");
                  }

                  // set file status to COMPACTION_CANDIDATE
                  if (idx == 0) {
                    // the other thread holds write lock and delete files successfully before
                    // setting status to COMPACTING
                    cd1.countDown();
                    cd2.await();

                    Assert.assertThrows(
                        "inner space compaction task should be invalid.",
                        FileCannotTransitToCompactingException.class,
                        innerSpaceCompactionTask::transitSourceFilesToMerging);

                    innerSpaceCompactionTask.resetCompactionCandidateStatusForAllSourceFiles();
                    innerSpaceCompactionTask.handleTaskCleanup();

                    for (int i = 0; i < task.size(); i++) {
                      TsFileResource resource = task.get(i);
                      if (i == 1) {
                        if (resource.getStatus() != TsFileResourceStatus.DELETED) {
                          throw new RuntimeException("status should be DELETED");
                        }
                      } else if (resource.getStatus() != TsFileResourceStatus.NORMAL) {
                        throw new RuntimeException("status should be NORMAL");
                      }
                    }
                  } else {
                    try {
                      innerSpaceCompactionTask.transitSourceFilesToMerging();
                    } catch (FileCannotTransitToCompactingException e) {
                      Assert.fail("inner space compaction task should be valid.");
                    }
                    for (int i = 0; i < task.size(); i++) {
                      TsFileResource resource = task.get(i);
                      if (resource.getStatus() != TsFileResourceStatus.COMPACTING) {
                        throw new RuntimeException("status should be COMPACTING");
                      }
                    }
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
  public void testSelectWhenModsFileGreaterThan50M()
      throws IOException, MetadataException, WriteProcessException {
    createFiles(6, 2, 3, 50, 0, 10000, 50, 50, false, true);
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    // mock modification file size must greater than 50M
    MockModiFicationFile mockModiFicationFile =
        new MockModiFicationFile(
            seqResources.get(0).getTsFilePath() + ModificationFile.FILE_SUFFIX);
    mockModiFicationFile.write(new Deletion(new MeasurementPath("root.a.b"), 1, 1));
    seqResources.get(0).setModFile(mockModiFicationFile);
    SizeTieredCompactionSelector selector =
        new SizeTieredCompactionSelector("", "", 0, true, tsFileManager);
    // copy candidate source file list
    List<TsFileResource> resources = tsFileManager.getOrCreateSequenceListByTimePartition(0);
    List<InnerSpaceCompactionTask> innerSpaceCompactionTasks =
        selector.selectInnerSpaceTask(resources);
    Assert.assertEquals(3, innerSpaceCompactionTasks.size());
    mockModiFicationFile.remove();
  }

  @Test
  public void testSelectWhenModsFileGreaterThan50MWithTsFileResourceStatusIsNotNormal()
      throws IOException, MetadataException, WriteProcessException {
    createFiles(2, 2, 3, 50, 0, 10000, 50, 50, false, true);
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    // mock modification file size must greater than 50M
    MockModiFicationFile mockModiFicationFile =
        new MockModiFicationFile(
            seqResources.get(0).getTsFilePath() + ModificationFile.FILE_SUFFIX);
    mockModiFicationFile.write(new Deletion(new MeasurementPath("root.a.b"), 1, 1));
    seqResources.get(0).setModFile(mockModiFicationFile);
    seqResources.get(0).setStatusForTest(TsFileResourceStatus.COMPACTING);
    SizeTieredCompactionSelector selector =
        new SizeTieredCompactionSelector("", "", 0, true, tsFileManager);
    // copy candidate source file list
    List<TsFileResource> resources = tsFileManager.getOrCreateSequenceListByTimePartition(0);
    List<InnerSpaceCompactionTask> innerSpaceCompactionTasks =
        selector.selectInnerSpaceTask(resources);
    Assert.assertEquals(0, innerSpaceCompactionTasks.size());
    mockModiFicationFile.remove();
  }

  class MockModiFicationFile extends ModificationFile {

    /**
     * Construct a ModificationFile using a file as its storage.
     *
     * @param filePath the path of the storage file.
     */
    public MockModiFicationFile(String filePath) {
      super(filePath);
      new File(filePath);
    }

    @Override
    public long getSize() {
      return 1024 * 1024 * 50 + 1;
    }
  }
}
