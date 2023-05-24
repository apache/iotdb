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
package org.apache.iotdb.db.engine.compaction.inner;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.compaction.AbstractCompactionTest;
import org.apache.iotdb.db.engine.compaction.execute.performer.impl.FastCompactionPerformer;
import org.apache.iotdb.db.engine.compaction.execute.task.InnerSpaceCompactionTask;
import org.apache.iotdb.db.engine.compaction.schedule.CompactionTaskManager;
import org.apache.iotdb.db.engine.compaction.selector.impl.SizeTieredCompactionSelector;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class InnerSpaceCompactionSelectorTest extends AbstractCompactionTest {
  @Before
  public void setUp()
      throws IOException, WriteProcessException, MetadataException, InterruptedException {
    super.setUp();
    IoTDBDescriptor.getInstance().getConfig().setMaxInnerCompactionCandidateFileNum(2);
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
                List<List<TsFileResource>> taskResource = selector.selectInnerSpaceTask(resources);
                if (taskResource.size() != 2) {
                  throw new RuntimeException("task num is not 2");
                }
                if (taskResource.get(0).size() != 2 || taskResource.get(1).size() != 2) {
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
                List<List<TsFileResource>> taskResource = selector.selectInnerSpaceTask(resources);

                // the other thread holds write lock and delete files successfully before setting
                // status to COMPACTION_CANDIDATE
                cd1.countDown();
                cd2.await(1000, TimeUnit.MILLISECONDS);

                if (taskResource.size() != 3) {
                  throw new RuntimeException("task num is not 3");
                }
                for (int idx = 0; idx < taskResource.size(); idx++) {
                  List<TsFileResource> task = taskResource.get(idx);
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
                          CompactionTaskManager.currentTaskNum,
                          tsFileManager.getNextCompactionTaskId());
                  // set file status to COMPACTION_CANDIDATE
                  if (idx == taskResource.size() - 1) {
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
                List<List<TsFileResource>> taskResource = selector.selectInnerSpaceTask(resources);

                if (taskResource.size() != 3) {
                  throw new RuntimeException("task num is not 3");
                }
                for (int idx = 0; idx < taskResource.size(); idx++) {
                  List<TsFileResource> task = taskResource.get(idx);
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
                          CompactionTaskManager.currentTaskNum,
                          tsFileManager.getNextCompactionTaskId());

                  if (!innerSpaceCompactionTask.setSourceFilesToCompactionCandidate()) {
                    throw new RuntimeException("set status should be true");
                  }

                  // set file status to COMPACTION_CANDIDATE
                  if (idx == taskResource.size() - 1) {
                    // the other thread holds write lock and delete files successfully before
                    // setting status to COMPACTING
                    cd1.countDown();
                    cd2.await(1000, TimeUnit.MILLISECONDS);

                    if (innerSpaceCompactionTask.checkValidAndSetMerging()) {
                      throw new RuntimeException("cross space compaction task should be invalid.");
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
                    if (!innerSpaceCompactionTask.checkValidAndSetMerging()) {
                      throw new RuntimeException("cross space compaction task should be valid.");
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
                cd2.await(1000, TimeUnit.MILLISECONDS);
                SizeTieredCompactionSelector selector =
                    new SizeTieredCompactionSelector("", "", 0, true, tsFileManager);
                List<TsFileResource> resources =
                    tsFileManager.getOrCreateSequenceListByTimePartition(0);
                List<List<TsFileResource>> taskResource = selector.selectInnerSpaceTask(resources);
                if (taskResource.size() != 2) {
                  throw new RuntimeException("task num is not 2");
                }
                if (taskResource.get(0).size() != 2 || taskResource.get(1).size() != 2) {
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
                List<List<TsFileResource>> taskResource = selector.selectInnerSpaceTask(resources);

                // the other thread holds write lock and delete files successfully before setting
                // status to COMPACTION_CANDIDATE
                cd1.countDown();
                cd2.await(1000, TimeUnit.MILLISECONDS);

                if (taskResource.size() != 3) {
                  throw new RuntimeException("task num is not 3");
                }
                for (int idx = 0; idx < taskResource.size(); idx++) {
                  List<TsFileResource> task = taskResource.get(idx);
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
                          CompactionTaskManager.currentTaskNum,
                          tsFileManager.getNextCompactionTaskId());
                  // set file status to COMPACTION_CANDIDATE
                  if (idx == taskResource.size() - 1) {
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
                List<List<TsFileResource>> taskResource = selector.selectInnerSpaceTask(resources);

                if (taskResource.size() != 3) {
                  throw new RuntimeException("task num is not 3");
                }
                for (int idx = 0; idx < taskResource.size(); idx++) {
                  List<TsFileResource> task = taskResource.get(idx);
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
                          CompactionTaskManager.currentTaskNum,
                          tsFileManager.getNextCompactionTaskId());

                  if (!innerSpaceCompactionTask.setSourceFilesToCompactionCandidate()) {
                    throw new RuntimeException("set status should be true");
                  }

                  // set file status to COMPACTION_CANDIDATE
                  if (idx == taskResource.size() - 1) {
                    // the other thread holds write lock and delete files successfully before
                    // setting status to COMPACTING
                    cd1.countDown();
                    cd2.await(1000, TimeUnit.MILLISECONDS);

                    if (innerSpaceCompactionTask.checkValidAndSetMerging()) {
                      throw new RuntimeException("cross space compaction task should be invalid.");
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
                    if (!innerSpaceCompactionTask.checkValidAndSetMerging()) {
                      throw new RuntimeException("cross space compaction task should be valid.");
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
}
