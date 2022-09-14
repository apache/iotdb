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

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.sync.SyncConstant;
import org.apache.iotdb.commons.sync.SyncPathUtil;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.modification.Deletion;
import org.apache.iotdb.db.qp.physical.sys.CreateTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.SetStorageGroupPlan;
import org.apache.iotdb.db.sync.pipedata.DeletionPipeData;
import org.apache.iotdb.db.sync.pipedata.PipeData;
import org.apache.iotdb.db.sync.pipedata.SchemaPipeData;
import org.apache.iotdb.db.sync.pipedata.TsFilePipeData;
import org.apache.iotdb.db.sync.sender.pipe.Pipe;
import org.apache.iotdb.db.sync.sender.pipe.TsFilePipe;
import org.apache.iotdb.db.sync.transport.client.IoTDBSyncClient;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.rpc.RpcTransportFactory;
import org.apache.iotdb.rpc.TConfigurationConst;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.IClientRPCService;
import org.apache.iotdb.service.rpc.thrift.TSyncIdentityInfo;
import org.apache.iotdb.service.rpc.thrift.TSyncTransportMetaInfo;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.SessionDataSet;
import org.apache.iotdb.session.util.Version;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.apache.commons.io.FileUtils;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class SyncTransportTest {

  private static IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  /** create tsfile and move to tmpDir for sync test */
  File tmpDir = new File("target/synctest");

  String pipeName1 = "pipe1";
  String remoteIp1;
  long createdTime1 = System.currentTimeMillis();
  File fileDir;

  File tsfile;
  File resourceFile;
  File modsFile;

  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.envSetUp();
    remoteIp1 = "127.0.0.1";
    fileDir = new File(SyncPathUtil.getReceiverFileDataDir(pipeName1, remoteIp1, createdTime1));
    prepareData();
    EnvironmentUtils.shutdownDaemon();
    File srcDir =
        new File(
            IoTDBDescriptor.getInstance().getConfig().getDataDirs()[0]
                + File.separator
                + "sequence"
                + File.separator
                + "root.vehicle"
                + File.separator
                + "0"
                + File.separator
                + "0");
    if (tmpDir.exists()) {
      FileUtils.deleteDirectory(tmpDir);
    }
    FileUtils.moveDirectory(srcDir, tmpDir);
    tsfile = null;
    resourceFile = null;
    modsFile = null;
    File[] fileList = tmpDir.listFiles();
    for (File f : fileList) {
      if (f.getName().endsWith(".tsfile")) {
        tsfile = f;
      } else if (f.getName().endsWith(".mods")) {
        modsFile = f;
      } else if (f.getName().endsWith(".resource")) {
        resourceFile = f;
      }
    }
    EnvironmentUtils.cleanEnv();
    EnvironmentUtils.envSetUp();
  }

  @After
  public void tearDown() throws Exception {
    FileUtils.deleteDirectory(tmpDir);
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void testTransportFile() throws Exception {
    TSyncIdentityInfo identityInfo =
        new TSyncIdentityInfo("127.0.0.1", pipeName1, createdTime1, config.getIoTDBVersion(), "");
    try (TTransport transport =
        RpcTransportFactory.INSTANCE.getTransport(
            new TSocket(
                TConfigurationConst.defaultTConfiguration,
                "127.0.0.1",
                6667,
                SyncConstant.SOCKET_TIMEOUT_MILLISECONDS,
                SyncConstant.CONNECT_TIMEOUT_MILLISECONDS))) {
      TProtocol protocol;
      if (config.isRpcThriftCompressionEnable()) {
        protocol = new TCompactProtocol(transport);
      } else {
        protocol = new TBinaryProtocol(transport);
      }
      IClientRPCService.Client serviceClient = new IClientRPCService.Client(protocol);
      // Underlay socket open.
      if (!transport.isOpen()) {
        transport.open();
      }
      byte[] buffer = new byte[10];
      try (RandomAccessFile randomAccessFile = new RandomAccessFile(tsfile, "rw")) {
        // no handshake, response TException
        try {
          serviceClient.sendFile(
              new TSyncTransportMetaInfo(tsfile.getName(), 0), ByteBuffer.wrap(buffer));
          Assert.fail();
        } catch (TException e) {
          // do nothing
        }
        serviceClient.handshake(identityInfo);
        // response REBASE:0
        randomAccessFile.read(buffer, 0, 10);
        TSStatus tsStatus1 =
            serviceClient.sendFile(
                new TSyncTransportMetaInfo(tsfile.getName(), 1), ByteBuffer.wrap(buffer));
        Assert.assertEquals(tsStatus1.getCode(), TSStatusCode.SYNC_FILE_REBASE.getStatusCode());
        Assert.assertEquals(tsStatus1.getMessage(), "0");
        // response SUCCESS
        TSStatus tsStatus2 =
            serviceClient.sendFile(
                new TSyncTransportMetaInfo(tsfile.getName(), 0), ByteBuffer.wrap(buffer));
        Assert.assertEquals(tsStatus2.getCode(), TSStatusCode.SUCCESS_STATUS.getStatusCode());
        // response response REBASE:10
        TSStatus tsStatus3 =
            serviceClient.sendFile(
                new TSyncTransportMetaInfo(tsfile.getName(), 0), ByteBuffer.wrap(buffer));
        Assert.assertEquals(tsStatus3.getCode(), TSStatusCode.SYNC_FILE_REBASE.getStatusCode());
        Assert.assertEquals(tsStatus3.getMessage(), "10");
        TSStatus tsStatus4 =
            serviceClient.sendFile(
                new TSyncTransportMetaInfo(tsfile.getName(), 100), ByteBuffer.wrap(buffer));
        Assert.assertEquals(tsStatus4.getCode(), TSStatusCode.SYNC_FILE_REBASE.getStatusCode());
        Assert.assertEquals(tsStatus4.getMessage(), "10");
        // response SUCCESS
        byte[] remainBuffer = new byte[(int) (randomAccessFile.length() - 10)];
        randomAccessFile.read(remainBuffer, 0, (int) (randomAccessFile.length() - 10));
        TSStatus tsStatus5 =
            serviceClient.sendFile(
                new TSyncTransportMetaInfo(tsfile.getName(), 10), ByteBuffer.wrap(remainBuffer));
        Assert.assertEquals(tsStatus5.getCode(), TSStatusCode.SUCCESS_STATUS.getStatusCode());
      }
    }
    // check completeness of file
    File receiveFile =
        new File(
            SyncPathUtil.getFileDataDirPath(identityInfo),
            tsfile.getName() + SyncConstant.PATCH_SUFFIX);
    Assert.assertTrue(receiveFile.exists());

    try (RandomAccessFile originFileRAF = new RandomAccessFile(tsfile, "r");
        RandomAccessFile receiveFileRAF = new RandomAccessFile(receiveFile, "r")) {
      Assert.assertEquals(originFileRAF.length(), receiveFileRAF.length());
      byte[] buffer1 = new byte[(int) originFileRAF.length()];
      byte[] buffer2 = new byte[(int) receiveFile.length()];
      originFileRAF.read(buffer1);
      receiveFileRAF.read(buffer2);
      Assert.assertArrayEquals(buffer1, buffer2);
    }
  }

  @Test
  public void testTransportPipeData() throws Exception {
    try (TTransport transport =
        RpcTransportFactory.INSTANCE.getTransport(
            new TSocket(
                TConfigurationConst.defaultTConfiguration,
                "127.0.0.1",
                6667,
                SyncConstant.SOCKET_TIMEOUT_MILLISECONDS,
                SyncConstant.CONNECT_TIMEOUT_MILLISECONDS))) {
      TProtocol protocol;
      if (config.isRpcThriftCompressionEnable()) {
        protocol = new TCompactProtocol(transport);
      } else {
        protocol = new TBinaryProtocol(transport);
      }
      IClientRPCService.Client serviceClient = new IClientRPCService.Client(protocol);
      // Underlay socket open.
      if (!transport.isOpen()) {
        transport.open();
      }
      PipeData pipeData =
          new SchemaPipeData(new SetStorageGroupPlan(new PartialPath("root.sg1")), 0);
      byte[] buffer = pipeData.serialize();
      ByteBuffer buffToSend = ByteBuffer.wrap(buffer);
      try {
        TSStatus tsStatus = serviceClient.sendPipeData(buffToSend);
        Assert.fail();
      } catch (TException e) {
        // do nothing
      }
      serviceClient.handshake(
          new TSyncIdentityInfo(
              "127.0.0.1", pipeName1, createdTime1, config.getIoTDBVersion(), "root.sg1"));
      TSStatus tsStatus = serviceClient.sendPipeData(buffToSend);
      Assert.assertEquals(tsStatus.getCode(), TSStatusCode.SUCCESS_STATUS.getStatusCode());
    }
  }

  @Test
  public void testSyncClient() throws Exception {
    // 1. prepare fake file
    Assert.assertNotNull(tsfile);
    Assert.assertNotNull(modsFile);
    Assert.assertNotNull(resourceFile);

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

    // 3. start client
    Pipe pipe = new TsFilePipe(createdTime1, pipeName1, null, 0, false);
    IoTDBSyncClient client =
        new IoTDBSyncClient(
            pipe,
            "127.0.0.1",
            IoTDBDescriptor.getInstance().getConfig().getRpcPort(),
            "127.0.0.1",
            "root.vehicle");
    client.handshake();
    for (PipeData pipeData : pipeDataList) {
      client.send(pipeData);
    }

    // 4. check result
    checkResult(
        "select ** from root.vehicle",
        new String[] {"Time", "root.vehicle.d0.s0"},
        new String[] {"2,2"});
  }

  private void prepareData() throws Exception {
    Session session =
        new Session.Builder()
            .host("127.0.0.1")
            .port(6667)
            .username("root")
            .password("root")
            .version(Version.V_0_13)
            .build();
    try {
      session.open(false);

      // set session fetchSize
      session.setFetchSize(10000);
      session.setStorageGroup("root.vehicle");

      List<String> measurements = Collections.singletonList("s0");
      List<TSDataType> types = Collections.singletonList(TSDataType.INT32);
      session.insertRecord("root.vehicle.d0", 1, measurements, types, Collections.singletonList(1));
      session.insertRecord("root.vehicle.d0", 2, measurements, types, Collections.singletonList(2));
      session.insertRecord(
          "root.vehicle.d0", 35, measurements, types, Collections.singletonList(35));
      session.executeNonQueryStatement("flush");
      session.executeNonQueryStatement("delete from root.vehicle.d0.s0 where time<2");
    } finally {
      session.close();
    }
  }

  private void checkResult(String sql, String[] columnNames, String[] retArray) throws Exception {
    Session session =
        new Session.Builder()
            .host("127.0.0.1")
            .port(6667)
            .username("root")
            .password("root")
            .version(Version.V_0_13)
            .build();
    try {
      session.open(false);
      // set session fetchSize
      session.setFetchSize(10000);
      try (SessionDataSet dataSet = session.executeQueryStatement(sql)) {
        Assert.assertArrayEquals(columnNames, dataSet.getColumnNames().toArray(new String[0]));
        List<String> actualRetArray = new ArrayList<>();
        while (dataSet.hasNext()) {
          RowRecord rowRecord = dataSet.next();
          StringBuilder rowString = new StringBuilder(rowRecord.getTimestamp() + ",");
          rowRecord.getFields().forEach(i -> rowString.append(i.getStringValue()));
          actualRetArray.add(rowString.toString());
        }
        Assert.assertArrayEquals(retArray, actualRetArray.toArray(new String[0]));
      }
    } finally {
      session.close();
    }
  }
}
