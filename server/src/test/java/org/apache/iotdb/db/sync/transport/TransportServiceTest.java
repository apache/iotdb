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
 *
 */
package org.apache.iotdb.db.sync.transport;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.modification.Deletion;
import org.apache.iotdb.db.engine.modification.ModificationFile;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.qp.physical.sys.CreateTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.SetStorageGroupPlan;
import org.apache.iotdb.db.sync.conf.SyncPathUtil;
import org.apache.iotdb.db.sync.pipedata.DeletionPipeData;
import org.apache.iotdb.db.sync.pipedata.PipeData;
import org.apache.iotdb.db.sync.pipedata.SchemaPipeData;
import org.apache.iotdb.db.sync.pipedata.TsFilePipeData;
import org.apache.iotdb.db.sync.pipedata.queue.PipeDataQueue;
import org.apache.iotdb.db.sync.pipedata.queue.PipeDataQueueFactory;
import org.apache.iotdb.db.sync.sender.pipe.Pipe;
import org.apache.iotdb.db.sync.sender.pipe.TsFilePipe;
import org.apache.iotdb.db.sync.transport.client.TransportClient;
import org.apache.iotdb.db.sync.transport.server.TransportServerManager;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.net.InetAddress;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TransportServiceTest {
  /** create tsfile and move to tmpDir for sync test */
  File tmpDir = new File("target/synctest");

  String pipeName1 = "pipe1";
  String remoteIp1;
  long createdTime1 = System.currentTimeMillis();
  File fileDir;
  PipeDataQueue pipeDataQueue;

  @Before
  public void setUp() throws Exception {
    remoteIp1 = InetAddress.getLocalHost().getHostAddress();
    fileDir = new File(SyncPathUtil.getReceiverFileDataDir(pipeName1, remoteIp1, createdTime1));
    pipeDataQueue =
        PipeDataQueueFactory.getBufferedPipeDataQueue(
            SyncPathUtil.getReceiverPipeLogDir(pipeName1, remoteIp1, createdTime1));
    EnvironmentUtils.envSetUp();
    if (!tmpDir.exists()) {
      tmpDir.mkdirs();
    }
  }

  @After
  public void tearDown() throws Exception {
    pipeDataQueue.clear();
    FileUtils.deleteDirectory(tmpDir);
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void test() throws Exception {
    // 1. prepare fake file
    File tsfile = new File(tmpDir, "test.tsfile");
    File resourceFile = new File(tsfile.getAbsoluteFile() + TsFileResource.RESOURCE_SUFFIX);
    File modsFile = new File(tsfile.getAbsoluteFile() + ModificationFile.FILE_SUFFIX);
    FileWriter out = new FileWriter(tsfile);
    out.write("tsfile");
    out.flush();
    out.close();
    out = new FileWriter(resourceFile);
    out.write("resource");
    out.flush();
    out.close();
    out = new FileWriter(modsFile);
    out.write("mods");
    out.flush();
    out.close();

    // 2. prepare pipelog and pipeDataQueue
    int serialNum = 0;
    List<PipeData> pipeDataList = new ArrayList<>();
    pipeDataList.add(
        new SchemaPipeData(new SetStorageGroupPlan(new PartialPath("root.vehicle")), serialNum++));
    pipeDataList.add(
        new SchemaPipeData(
            new CreateTimeSeriesPlan(
                new PartialPath("root.vehicle.d0.s0"),
                new MeasurementSchema("s0", TSDataType.INT32, TSEncoding.RLE)),
            serialNum++));
    TsFilePipeData tsFilePipeData = new TsFilePipeData(tsfile.getPath(), serialNum++);
    pipeDataList.add(tsFilePipeData);
    Deletion deletion = new Deletion(new PartialPath("root.vehicle.**"), 0, 33, 38);
    pipeDataList.add(new DeletionPipeData(deletion, serialNum++));

    // 3. start server
    TransportServerManager.getInstance().startService();

    // 4. start client
    Pipe pipe = new TsFilePipe(createdTime1, pipeName1, null, 0, false);
    TransportClient client =
        new TransportClient(
            pipe, "127.0.0.1", IoTDBDescriptor.getInstance().getConfig().getPipeServerPort());
    client.handshake();
    for (PipeData pipeData : pipeDataList) {
      client.senderTransport(pipeData);
    }

    // 5. check file
    Thread.sleep(1000);
    client.close();
    TransportServerManager.getInstance().stopService();
    File[] targetFiles = fileDir.listFiles((dir1, name) -> name.equals(tsfile.getName()));
    Assert.assertNotNull(targetFiles);
    Assert.assertEquals(1, targetFiles.length);
    compareFile(targetFiles[0], tsfile);
    File[] resourceFiles = fileDir.listFiles((dir1, name) -> name.equals(resourceFile.getName()));
    Assert.assertNotNull(resourceFiles);
    Assert.assertEquals(1, resourceFiles.length);
    compareFile(resourceFiles[0], resourceFile);
    File[] modsFiles = fileDir.listFiles((dir1, name) -> name.equals(modsFile.getName()));
    Assert.assertNotNull(modsFiles);
    Assert.assertEquals(1, modsFiles.length);
    compareFile(modsFiles[0], modsFile);

    // 6. check pipedata
    tsFilePipeData.setParentDirPath(fileDir.getAbsolutePath());
    ExecutorService es1 = Executors.newSingleThreadExecutor();
    List<PipeData> resPipeData = new ArrayList<>();
    es1.execute(
        () -> {
          for (int i = 0; i < pipeDataList.size(); i++) {
            try {
              resPipeData.add(pipeDataQueue.take());
              pipeDataQueue.commit();
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            }
          }
        });
    try {
      Thread.sleep(500);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    es1.shutdownNow();
    Assert.assertEquals(pipeDataList.size(), resPipeData.size());
    for (int i = 0; i < resPipeData.size(); i++) {
      Assert.assertEquals(pipeDataList.get(i), resPipeData.get(i));
    }
    pipeDataQueue.clear();
  }

  private void compareFile(File firFile, File secFile) {
    try {
      MessageDigest messageDigest1 = MessageDigest.getInstance("SHA-256");
      MessageDigest messageDigest2 = MessageDigest.getInstance("SHA-256");
      BufferedInputStream fir = new BufferedInputStream(new FileInputStream(firFile));
      BufferedInputStream sec = new BufferedInputStream(new FileInputStream(secFile));
      // To compare the length and hash of the files.
      Assert.assertEquals(fir.available(), sec.available());
      byte[] firstBytes = new byte[1024];
      byte[] secondBytes = new byte[1024];
      int length = -1;
      while ((length = fir.read(firstBytes)) != -1) {
        Assert.assertEquals(length, sec.read(secondBytes));
        messageDigest1.update(firstBytes, 0, length);
        messageDigest2.update(secondBytes, 0, length);
      }
      fir.close();
      sec.close();
      Assert.assertArrayEquals(messageDigest1.digest(), messageDigest2.digest());
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
  }
}
