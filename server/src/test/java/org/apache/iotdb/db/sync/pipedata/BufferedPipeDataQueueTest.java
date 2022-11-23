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
package org.apache.iotdb.db.sync.pipedata;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.sync.utils.SyncConstant;
import org.apache.iotdb.commons.sync.utils.SyncPathUtil;
import org.apache.iotdb.db.engine.modification.Deletion;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.sync.pipedata.queue.BufferedPipeDataQueue;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class BufferedPipeDataQueueTest {
  private static final Logger logger = LoggerFactory.getLogger(BufferedPipeDataQueueTest.class);

  File pipeLogDir =
      new File(
          SyncPathUtil.getReceiverPipeLogDir("pipe", "192.168.0.11", System.currentTimeMillis()));

  @Before
  public void setUp() throws Exception {
    if (!pipeLogDir.exists()) {
      pipeLogDir.mkdirs();
    }
  }

  @After
  public void tearDown() throws IOException, StorageEngineException {
    FileUtils.deleteDirectory(pipeLogDir);
  }

  @Test
  public void testRecoveryAndClear() {
    try {
      DataOutputStream outputStream =
          new DataOutputStream(
              new FileOutputStream(new File(pipeLogDir, SyncConstant.COMMIT_LOG_NAME), true));
      outputStream.writeLong(1);
      outputStream.close();
      // pipelog1: 0~3
      DataOutputStream pipeLogOutput1 =
          new DataOutputStream(
              new FileOutputStream(
                  new File(pipeLogDir.getPath(), SyncPathUtil.getPipeLogName(0)), false));
      for (int i = 0; i < 4; i++) {
        new TsFilePipeData("", i).serialize(pipeLogOutput1);
      }
      pipeLogOutput1.close();
      // pipelog2: 4~10
      DataOutputStream pipeLogOutput2 =
          new DataOutputStream(
              new FileOutputStream(
                  new File(pipeLogDir.getPath(), SyncPathUtil.getPipeLogName(4)), false));
      for (int i = 4; i < 11; i++) {
        new TsFilePipeData("", i).serialize(pipeLogOutput2);
      }
      pipeLogOutput2.close();
      // pipelog3: 11 without pipedata
      DataOutputStream pipeLogOutput3 =
          new DataOutputStream(
              new FileOutputStream(
                  new File(pipeLogDir.getPath(), SyncPathUtil.getPipeLogName(11)), false));
      pipeLogOutput3.close();
      // recovery
      BufferedPipeDataQueue pipeDataQueue = new BufferedPipeDataQueue(pipeLogDir.getPath());
      Assert.assertEquals(1, pipeDataQueue.getCommitSerialNumber());
      Assert.assertEquals(10, pipeDataQueue.getLastMaxSerialNumber());
      pipeDataQueue.clear();
      Assert.assertFalse(pipeLogDir.exists());
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
  }

  /** Try to take data from a new pipe. Expect to wait indefinitely if no data offer. */
  @Test
  public void testTake() {
    BufferedPipeDataQueue pipeDataQueue = new BufferedPipeDataQueue(pipeLogDir.getPath());
    List<PipeData> pipeDatas = new ArrayList<>();
    ExecutorService es1 = Executors.newSingleThreadExecutor();
    es1.execute(
        () -> {
          try {
            pipeDatas.add(pipeDataQueue.take());
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        });
    try {
      Thread.sleep(3000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    es1.shutdownNow();

    Assert.assertEquals(0, pipeDatas.size());
  }

  /** Try to take data from a new pipe. Expect to wake after offer. */
  @Test
  public void testTakeAndOffer() {
    BufferedPipeDataQueue pipeDataQueue = new BufferedPipeDataQueue(pipeLogDir.getPath());
    try {
      List<PipeData> pipeDatas = new ArrayList<>();
      ExecutorService es1 = Executors.newSingleThreadExecutor();
      es1.execute(
          () -> {
            try {
              pipeDatas.add(pipeDataQueue.take());
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            }
          });
      pipeDataQueue.offer(new TsFilePipeData("", 0));
      try {
        Thread.sleep(3000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      es1.shutdownNow();
      try {
        Thread.sleep(500);
      } catch (InterruptedException e) {
        e.printStackTrace();
        Assert.fail();
      }
      Assert.assertEquals(1, pipeDatas.size());
    } finally {
      pipeDataQueue.clear();
    }
  }

  /** Try to offer data to a new pipe. */
  @Test
  public void testOfferNewPipe() {
    BufferedPipeDataQueue pipeDataQueue = new BufferedPipeDataQueue(pipeLogDir.getPath());
    try {
      PipeData pipeData = new TsFilePipeData("fakePath", 1);
      pipeDataQueue.offer(pipeData);
      List<PipeData> pipeDatas = new ArrayList<>();
      ExecutorService es1 = Executors.newSingleThreadExecutor();
      es1.execute(
          () -> {
            try {
              pipeDatas.add(pipeDataQueue.take());
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            }
          });
      try {
        Thread.sleep(3000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      es1.shutdownNow();
      try {
        Thread.sleep(500);
      } catch (InterruptedException e) {
        e.printStackTrace();
        Assert.fail();
      }
      Assert.assertEquals(1, pipeDatas.size());
      Assert.assertEquals(pipeData, pipeDatas.get(0));
    } finally {
      pipeDataQueue.clear();
    }
  }

  /**
   * Step1: recover pipeDataQueue (with an empty latest pipelog) Step2: offer new pipeData Step3:
   * check result
   */
  @Test
  public void testOfferAfterRecoveryWithEmptyPipeLog() {
    try {
      DataOutputStream outputStream =
          new DataOutputStream(
              new FileOutputStream(new File(pipeLogDir, SyncConstant.COMMIT_LOG_NAME), true));
      outputStream.writeLong(1);
      outputStream.close();
      List<PipeData> pipeDataList = new ArrayList<>();
      // pipelog1: 0~3
      DataOutputStream pipeLogOutput1 =
          new DataOutputStream(
              new FileOutputStream(
                  new File(pipeLogDir.getPath(), SyncPathUtil.getPipeLogName(0)), false));
      for (int i = 0; i < 4; i++) {
        PipeData pipeData = new TsFilePipeData("fake" + i, i);
        pipeDataList.add(pipeData);
        pipeData.serialize(pipeLogOutput1);
      }
      pipeLogOutput1.close();
      // pipelog2: 4~10
      DataOutputStream pipeLogOutput2 =
          new DataOutputStream(
              new FileOutputStream(
                  new File(pipeLogDir.getPath(), SyncPathUtil.getPipeLogName(4)), false));
      for (int i = 4; i < 8; i++) {
        PipeData pipeData =
            new DeletionPipeData(new Deletion(new PartialPath("fake" + i), 0, 99), i);
        pipeDataList.add(pipeData);
        pipeData.serialize(pipeLogOutput2);
      }
      for (int i = 8; i < 11; i++) {
        PipeData pipeData =
            new DeletionPipeData(new Deletion(new PartialPath("fake" + i), 0, 99), i);
        pipeDataList.add(pipeData);
        pipeData.serialize(pipeLogOutput2);
      }
      pipeLogOutput2.close();
      // pipelog3: 11 without pipedata
      DataOutputStream pipeLogOutput3 =
          new DataOutputStream(
              new FileOutputStream(
                  new File(pipeLogDir.getPath(), SyncPathUtil.getPipeLogName(11)), false));
      pipeLogOutput3.close();
      // recovery
      BufferedPipeDataQueue pipeDataQueue = new BufferedPipeDataQueue(pipeLogDir.getPath());
      try {

        Assert.assertEquals(1, pipeDataQueue.getCommitSerialNumber());
        Assert.assertEquals(10, pipeDataQueue.getLastMaxSerialNumber());
        PipeData offerPipeData = new TsFilePipeData("fake11", 11);
        pipeDataList.add(offerPipeData);
        pipeDataQueue.offer(offerPipeData);

        // take and check
        List<PipeData> pipeDataTakeList = new ArrayList<>();
        ExecutorService es1 = Executors.newSingleThreadExecutor();
        es1.execute(
            () -> {
              while (true) {
                try {
                  pipeDataTakeList.add(pipeDataQueue.take());
                  pipeDataQueue.commit();
                } catch (InterruptedException e) {
                  break;
                }
              }
            });
        try {
          Thread.sleep(3000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
        es1.shutdownNow();
        try {
          Thread.sleep(500);
        } catch (InterruptedException e) {
          e.printStackTrace();
          Assert.fail();
        }
        Assert.assertEquals(10, pipeDataTakeList.size());
        for (int i = 0; i < 10; i++) {
          Assert.assertEquals(pipeDataList.get(i + 2), pipeDataTakeList.get(i));
        }
      } finally {
        pipeDataQueue.clear();
      }
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail();
    }
  }

  /** Step1: recover pipeDataQueue (without empty latest pipelog) Step2: check result */
  @Test
  public void testRecoveryWithEmptyPipeLog() {
    try {
      DataOutputStream outputStream =
          new DataOutputStream(
              new FileOutputStream(new File(pipeLogDir, SyncConstant.COMMIT_LOG_NAME), true));
      outputStream.writeLong(1);
      outputStream.close();
      List<PipeData> pipeDataList = new ArrayList<>();
      // pipelog1: 0~3
      DataOutputStream pipeLogOutput1 =
          new DataOutputStream(
              new FileOutputStream(
                  new File(pipeLogDir.getPath(), SyncPathUtil.getPipeLogName(0)), false));
      for (int i = 0; i < 4; i++) {
        PipeData pipeData = new TsFilePipeData("fake" + i, i);
        pipeDataList.add(pipeData);
        pipeData.serialize(pipeLogOutput1);
      }
      pipeLogOutput1.close();
      // pipelog2: 4~10
      DataOutputStream pipeLogOutput2 =
          new DataOutputStream(
              new FileOutputStream(
                  new File(pipeLogDir.getPath(), SyncPathUtil.getPipeLogName(4)), false));
      for (int i = 4; i < 8; i++) {
        PipeData pipeData =
            new DeletionPipeData(new Deletion(new PartialPath("fake" + i), 0, 99), i);
        pipeDataList.add(pipeData);
        pipeData.serialize(pipeLogOutput2);
      }
      for (int i = 8; i < 11; i++) {
        PipeData pipeData =
            new DeletionPipeData(new Deletion(new PartialPath("fake" + i), 0, 99), i);
        pipeDataList.add(pipeData);
        pipeData.serialize(pipeLogOutput2);
      }
      pipeLogOutput2.close();
      // pipelog3: 11 without pipedata
      DataOutputStream pipeLogOutput3 =
          new DataOutputStream(
              new FileOutputStream(
                  new File(pipeLogDir.getPath(), SyncPathUtil.getPipeLogName(11)), false));
      pipeLogOutput3.close();
      // recovery
      BufferedPipeDataQueue pipeDataQueue = new BufferedPipeDataQueue(pipeLogDir.getPath());
      try {
        Assert.assertEquals(1, pipeDataQueue.getCommitSerialNumber());
        Assert.assertEquals(10, pipeDataQueue.getLastMaxSerialNumber());

        // take and check
        List<PipeData> pipeDataTakeList = new ArrayList<>();
        ExecutorService es1 = Executors.newSingleThreadExecutor();
        es1.execute(
            () -> {
              while (true) {
                try {
                  pipeDataTakeList.add(pipeDataQueue.take());
                  pipeDataQueue.commit();
                } catch (InterruptedException e) {
                  break;
                }
              }
            });
        try {
          Thread.sleep(3000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
        es1.shutdownNow();
        try {
          Thread.sleep(500);
        } catch (InterruptedException e) {
          e.printStackTrace();
          Assert.fail();
        }
        Assert.assertEquals(9, pipeDataTakeList.size());
        for (int i = 0; i < 9; i++) {
          Assert.assertEquals(pipeDataList.get(i + 2), pipeDataTakeList.get(i));
        }
      } finally {
        pipeDataQueue.clear();
      }
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail();
    }
  }

  /** Step1: recover pipeDataQueue (without empty latest pipelog) Step2: check result */
  @Test
  public void testRecoveryWithoutEmptyPipeLog() {
    try {
      DataOutputStream outputStream =
          new DataOutputStream(
              new FileOutputStream(new File(pipeLogDir, SyncConstant.COMMIT_LOG_NAME), true));
      outputStream.writeLong(1);
      outputStream.close();
      List<PipeData> pipeDataList = new ArrayList<>();
      // pipelog1: 0~3
      DataOutputStream pipeLogOutput1 =
          new DataOutputStream(
              new FileOutputStream(
                  new File(pipeLogDir.getPath(), SyncPathUtil.getPipeLogName(0)), false));
      for (int i = 0; i < 4; i++) {
        PipeData pipeData = new TsFilePipeData("fake" + i, i);
        pipeDataList.add(pipeData);
        pipeData.serialize(pipeLogOutput1);
      }
      pipeLogOutput1.close();
      // pipelog2: 4~10
      DataOutputStream pipeLogOutput2 =
          new DataOutputStream(
              new FileOutputStream(
                  new File(pipeLogDir.getPath(), SyncPathUtil.getPipeLogName(4)), false));
      for (int i = 4; i < 8; i++) {
        PipeData pipeData =
            new DeletionPipeData(new Deletion(new PartialPath("fake" + i), 0, 99), i);
        pipeDataList.add(pipeData);
        pipeData.serialize(pipeLogOutput2);
      }
      for (int i = 8; i < 11; i++) {
        PipeData pipeData =
            new DeletionPipeData(new Deletion(new PartialPath("fake" + i), 0, 99), i);
        pipeDataList.add(pipeData);
        pipeData.serialize(pipeLogOutput2);
      }
      pipeLogOutput2.close();
      ;
      // recovery
      BufferedPipeDataQueue pipeDataQueue = new BufferedPipeDataQueue(pipeLogDir.getPath());
      try {
        Assert.assertEquals(1, pipeDataQueue.getCommitSerialNumber());
        Assert.assertEquals(10, pipeDataQueue.getLastMaxSerialNumber());

        // take and check
        List<PipeData> pipeDataTakeList = new ArrayList<>();
        ExecutorService es1 = Executors.newSingleThreadExecutor();
        es1.execute(
            () -> {
              while (true) {
                try {
                  pipeDataTakeList.add(pipeDataQueue.take());
                  pipeDataQueue.commit();
                } catch (InterruptedException e) {
                  break;
                }
              }
            });
        try {
          Thread.sleep(3000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
        es1.shutdownNow();
        try {
          Thread.sleep(500);
        } catch (InterruptedException e) {
          e.printStackTrace();
          Assert.fail();
        }
        Assert.assertEquals(9, pipeDataTakeList.size());
        for (int i = 0; i < 9; i++) {
          Assert.assertEquals(pipeDataList.get(i + 2), pipeDataTakeList.get(i));
        }
      } finally {
        pipeDataQueue.clear();
      }
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail();
    }
  }

  @Test
  public void testOfferWhileTaking() {
    try {
      DataOutputStream outputStream =
          new DataOutputStream(
              new FileOutputStream(new File(pipeLogDir, SyncConstant.COMMIT_LOG_NAME), true));
      outputStream.writeLong(1);
      outputStream.close();
      List<PipeData> pipeDataList = new ArrayList<>();
      // pipelog1: 0~3
      DataOutputStream pipeLogOutput1 =
          new DataOutputStream(
              new FileOutputStream(
                  new File(pipeLogDir.getPath(), SyncPathUtil.getPipeLogName(0)), false));
      for (int i = 0; i < 4; i++) {
        PipeData pipeData = new TsFilePipeData("fake" + i, i);
        pipeDataList.add(pipeData);
        pipeData.serialize(pipeLogOutput1);
      }
      pipeLogOutput1.close();
      // pipelog2: 4~10
      DataOutputStream pipeLogOutput2 =
          new DataOutputStream(
              new FileOutputStream(
                  new File(pipeLogDir.getPath(), SyncPathUtil.getPipeLogName(4)), false));
      for (int i = 4; i < 8; i++) {
        PipeData pipeData =
            new DeletionPipeData(new Deletion(new PartialPath("fake" + i), 0, 99), i);
        pipeDataList.add(pipeData);
        pipeData.serialize(pipeLogOutput2);
      }
      for (int i = 8; i < 11; i++) {
        PipeData pipeData =
            new DeletionPipeData(new Deletion(new PartialPath("fake" + i), 0, 99), i);
        pipeDataList.add(pipeData);
        pipeData.serialize(pipeLogOutput2);
      }
      pipeLogOutput2.close();
      ;
      // recovery
      BufferedPipeDataQueue pipeDataQueue = new BufferedPipeDataQueue(pipeLogDir.getPath());
      try {
        Assert.assertEquals(1, pipeDataQueue.getCommitSerialNumber());
        Assert.assertEquals(10, pipeDataQueue.getLastMaxSerialNumber());

        // take
        List<PipeData> pipeDataTakeList = new ArrayList<>();
        ExecutorService es1 = Executors.newSingleThreadExecutor();
        es1.execute(
            () -> {
              while (true) {
                try {
                  pipeDataTakeList.add(pipeDataQueue.take());
                  pipeDataQueue.commit();
                } catch (InterruptedException e) {
                  break;
                } catch (Exception e) {
                  e.printStackTrace();
                  break;
                }
              }
            });
        // offer
        for (int i = 11; i < 20; i++) {
          pipeDataQueue.offer(
              new DeletionPipeData(new Deletion(new PartialPath("fake" + i), 0, 0), i));
        }
        try {
          Thread.sleep(3000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
        es1.shutdownNow();
        try {
          Thread.sleep(500);
        } catch (InterruptedException e) {
          e.printStackTrace();
          Assert.fail();
        }
        Assert.assertEquals(18, pipeDataTakeList.size());
        for (int i = 0; i < 9; i++) {
          Assert.assertEquals(pipeDataList.get(i + 2), pipeDataTakeList.get(i));
        }
      } finally {
        pipeDataQueue.clear();
      }
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail();
    }
  }

  @Test
  public void testOfferWhileTakingWithDiscontinuousSerialNumber() {
    try {
      DataOutputStream outputStream =
          new DataOutputStream(
              new FileOutputStream(new File(pipeLogDir, SyncConstant.COMMIT_LOG_NAME), true));
      outputStream.writeLong(1);
      outputStream.close();
      List<PipeData> pipeDataList = new ArrayList<>();
      // pipelog1: 3
      DataOutputStream pipeLogOutput1 =
          new DataOutputStream(
              new FileOutputStream(
                  new File(pipeLogDir.getPath(), SyncPathUtil.getPipeLogName(0)), false));
      PipeData tsFile3PipeData = new TsFilePipeData("fake3", 3);
      pipeDataList.add(tsFile3PipeData);
      tsFile3PipeData.serialize(pipeLogOutput1);
      pipeLogOutput1.close();
      // pipelog2: 4,5,6,7,10
      DataOutputStream pipeLogOutput2 =
          new DataOutputStream(
              new FileOutputStream(
                  new File(pipeLogDir.getPath(), SyncPathUtil.getPipeLogName(4)), false));
      for (int i = 4; i < 8; i++) {
        PipeData pipeData =
            new DeletionPipeData(new Deletion(new PartialPath("fake" + i), 0, 99), i);
        pipeDataList.add(pipeData);
        pipeData.serialize(pipeLogOutput2);
      }
      PipeData schema10PipeData =
          new DeletionPipeData(new Deletion(new PartialPath("fake" + 10), 0, 99), 10);
      pipeDataList.add(schema10PipeData);
      schema10PipeData.serialize(pipeLogOutput2);
      pipeLogOutput2.close();
      ;
      // recovery
      BufferedPipeDataQueue pipeDataQueue = new BufferedPipeDataQueue(pipeLogDir.getPath());
      try {
        Assert.assertEquals(1, pipeDataQueue.getCommitSerialNumber());
        Assert.assertEquals(10, pipeDataQueue.getLastMaxSerialNumber());

        // take
        List<PipeData> pipeDataTakeList = new ArrayList<>();
        ExecutorService es1 = Executors.newSingleThreadExecutor();
        es1.execute(
            () -> {
              while (true) {
                try {
                  PipeData pipeData = pipeDataQueue.take();
                  logger.info(String.format("PipeData: %s", pipeData));
                  pipeDataTakeList.add(pipeData);
                  pipeDataQueue.commit();
                } catch (InterruptedException e) {
                  break;
                } catch (Exception e) {
                  e.printStackTrace();
                  break;
                }
              }
            });
        // offer
        for (int i = 16; i < 20; i++) {
          if (!pipeDataQueue.offer(
              new DeletionPipeData(new Deletion(new PartialPath("fake" + i), 0, 0), i))) {
            logger.info(String.format("Can not offer serialize number %d", i));
          }
        }
        try {
          Thread.sleep(3000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
        es1.shutdownNow();
        try {
          Thread.sleep(500);
        } catch (InterruptedException e) {
          e.printStackTrace();
          Assert.fail();
        }
        logger.info(String.format("PipeDataTakeList: %s", pipeDataTakeList));
        Assert.assertEquals(10, pipeDataTakeList.size());
        for (int i = 0; i < 6; i++) {
          Assert.assertEquals(pipeDataList.get(i), pipeDataTakeList.get(i));
        }
      } finally {
        pipeDataQueue.clear();
      }
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail();
    }
  }
}
