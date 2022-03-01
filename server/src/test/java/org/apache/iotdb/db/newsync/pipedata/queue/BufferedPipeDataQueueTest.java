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
package org.apache.iotdb.db.newsync.pipedata.queue;

import org.apache.iotdb.db.engine.modification.Deletion;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.newsync.conf.SyncConstant;
import org.apache.iotdb.db.newsync.conf.SyncPathUtil;
import org.apache.iotdb.db.newsync.pipedata.DeletionPipeData;
import org.apache.iotdb.db.newsync.pipedata.PipeData;
import org.apache.iotdb.db.newsync.pipedata.SchemaPipeData;
import org.apache.iotdb.db.newsync.pipedata.TsFilePipeData;
import org.apache.iotdb.db.qp.physical.sys.SetStorageGroupPlan;
import org.apache.iotdb.db.utils.EnvironmentUtils;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class BufferedPipeDataQueueTest {
  File pipeLogDir =
      new File(
          SyncPathUtil.getReceiverPipeLogDir("pipe", "192.168.0.11", System.currentTimeMillis()));

  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.envSetUp();
    if (!pipeLogDir.exists()) {
      pipeLogDir.mkdirs();
    }
  }

  @After
  public void tearDown() throws IOException, StorageEngineException {
    FileUtils.deleteDirectory(pipeLogDir);
    EnvironmentUtils.cleanEnv();
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
                  new File(pipeLogDir.getPath(), SyncConstant.getPipeLogName(0)), false));
      for (int i = 0; i < 4; i++) {
        new TsFilePipeData(null, i).serialize(pipeLogOutput1);
      }
      pipeLogOutput1.close();
      // pipelog2: 4~10
      DataOutputStream pipeLogOutput2 =
          new DataOutputStream(
              new FileOutputStream(
                  new File(pipeLogDir.getPath(), SyncConstant.getPipeLogName(4)), false));
      for (int i = 4; i < 11; i++) {
        new TsFilePipeData(null, i).serialize(pipeLogOutput2);
      }
      pipeLogOutput2.close();
      // pipelog3: 11 without pipedata
      DataOutputStream pipeLogOutput3 =
          new DataOutputStream(
              new FileOutputStream(
                  new File(pipeLogDir.getPath(), SyncConstant.getPipeLogName(11)), false));
      pipeLogOutput3.close();
      // recovery
      BufferedPipeDataQueue pipeDataQueue = new BufferedPipeDataQueue(pipeLogDir.getPath());
      Assert.assertEquals(1, pipeDataQueue.getCommitSerialNumber());
      Assert.assertEquals(10, pipeDataQueue.getLastMaxSerialNumber());
      pipeDataQueue.clear();
      Assert.assertFalse(pipeLogDir.exists());
    } catch (Exception e) {
      Assert.fail();
    }
  }

  /** Try to take data from a new pipe. Expect to wait indefinitely if no data offer. */
  // TODO: 抛出NPE
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
    pipeDataQueue.offer(new TsFilePipeData(null, 0));
    try {
      Thread.sleep(3000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    es1.shutdownNow();
    Assert.assertEquals(1, pipeDatas.size());
  }

  /** Try to offer data to a new pipe. */
  @Test
  public void testOfferNewPipe() {
    BufferedPipeDataQueue pipeDataQueue = new BufferedPipeDataQueue(pipeLogDir.getPath());
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
    Assert.assertEquals(1, pipeDatas.size());
    Assert.assertEquals(pipeData, pipeDatas.get(0));
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
                  new File(pipeLogDir.getPath(), SyncConstant.getPipeLogName(0)), false));
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
                  new File(pipeLogDir.getPath(), SyncConstant.getPipeLogName(4)), false));
      for (int i = 4; i < 8; i++) {
        PipeData pipeData =
            new DeletionPipeData(new Deletion(new PartialPath("fake" + i), 0, 99), i);
        pipeDataList.add(pipeData);
        pipeData.serialize(pipeLogOutput2);
      }
      for (int i = 8; i < 11; i++) {
        PipeData pipeData =
            new SchemaPipeData(new SetStorageGroupPlan(new PartialPath("fake" + i)), i);
        pipeDataList.add(pipeData);
        pipeData.serialize(pipeLogOutput2);
      }
      pipeLogOutput2.close();
      // pipelog3: 11 without pipedata
      DataOutputStream pipeLogOutput3 =
          new DataOutputStream(
              new FileOutputStream(
                  new File(pipeLogDir.getPath(), SyncConstant.getPipeLogName(11)), false));
      pipeLogOutput3.close();
      // recovery
      BufferedPipeDataQueue pipeDataQueue = new BufferedPipeDataQueue(pipeLogDir.getPath());
      Assert.assertEquals(1, pipeDataQueue.getCommitSerialNumber());
      Assert.assertEquals(10, pipeDataQueue.getLastMaxSerialNumber());
      PipeData offerPipeData = new TsFilePipeData(null, 11);
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
                Thread.currentThread().interrupt();
              }
            }
          });
      try {
        Thread.sleep(3000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      es1.shutdownNow();
      Assert.assertEquals(10, pipeDataTakeList.size());
      for (int i = 0; i < 10; i++) {
        Assert.assertEquals(pipeDataList.get(i + 2), pipeDataTakeList.get(i));
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
                  new File(pipeLogDir.getPath(), SyncConstant.getPipeLogName(0)), false));
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
                  new File(pipeLogDir.getPath(), SyncConstant.getPipeLogName(4)), false));
      for (int i = 4; i < 8; i++) {
        PipeData pipeData =
            new DeletionPipeData(new Deletion(new PartialPath("fake" + i), 0, 99), i);
        pipeDataList.add(pipeData);
        pipeData.serialize(pipeLogOutput2);
      }
      for (int i = 8; i < 11; i++) {
        PipeData pipeData =
            new SchemaPipeData(new SetStorageGroupPlan(new PartialPath("fake" + i)), i);
        pipeDataList.add(pipeData);
        pipeData.serialize(pipeLogOutput2);
      }
      pipeLogOutput2.close();
      // pipelog3: 11 without pipedata
      DataOutputStream pipeLogOutput3 =
          new DataOutputStream(
              new FileOutputStream(
                  new File(pipeLogDir.getPath(), SyncConstant.getPipeLogName(11)), false));
      pipeLogOutput3.close();
      // recovery
      BufferedPipeDataQueue pipeDataQueue = new BufferedPipeDataQueue(pipeLogDir.getPath());
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
                Thread.currentThread().interrupt();
              }
            }
          });
      try {
        Thread.sleep(3000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      es1.shutdownNow();
      Assert.assertEquals(9, pipeDataTakeList.size());
      for (int i = 0; i < 9; i++) {
        Assert.assertEquals(pipeDataList.get(i + 2), pipeDataTakeList.get(i));
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
                  new File(pipeLogDir.getPath(), SyncConstant.getPipeLogName(0)), false));
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
                  new File(pipeLogDir.getPath(), SyncConstant.getPipeLogName(4)), false));
      for (int i = 4; i < 8; i++) {
        PipeData pipeData =
            new DeletionPipeData(new Deletion(new PartialPath("fake" + i), 0, 99), i);
        pipeDataList.add(pipeData);
        pipeData.serialize(pipeLogOutput2);
      }
      for (int i = 8; i < 11; i++) {
        PipeData pipeData =
            new SchemaPipeData(new SetStorageGroupPlan(new PartialPath("fake" + i)), i);
        pipeDataList.add(pipeData);
        pipeData.serialize(pipeLogOutput2);
      }
      pipeLogOutput2.close();
      ;
      // recovery
      BufferedPipeDataQueue pipeDataQueue = new BufferedPipeDataQueue(pipeLogDir.getPath());
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
                Thread.currentThread().interrupt();
              }
            }
          });
      try {
        Thread.sleep(3000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      es1.shutdownNow();
      Assert.assertEquals(9, pipeDataTakeList.size());
      for (int i = 0; i < 9; i++) {
        Assert.assertEquals(pipeDataList.get(i + 2), pipeDataTakeList.get(i));
      }
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail();
    }
  }
}
