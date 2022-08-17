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

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.sync.SyncPathUtil;
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
import org.apache.iotdb.db.sync.transport.client.IoTDBSinkTransportClient;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.SessionDataSet;
import org.apache.iotdb.session.util.Version;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class SyncTransportTest {
  /** create tsfile and move to tmpDir for sync test */
  File tmpDir = new File("target/synctest");

  String pipeName1 = "pipe1";
  String remoteIp1;
  long createdTime1 = System.currentTimeMillis();
  File fileDir;

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
    EnvironmentUtils.cleanEnv();
    EnvironmentUtils.envSetUp();
  }

  @After
  public void tearDown() throws Exception {
    FileUtils.deleteDirectory(tmpDir);
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void test() throws Exception {
    // 1. prepare fake file
    File[] fileList = tmpDir.listFiles();
    File tsfile = null;
    File resourceFile = null;
    File modsFile = null;
    for (File f : fileList) {
      if (f.getName().endsWith(".tsfile")) {
        tsfile = f;
      } else if (f.getName().endsWith(".mods")) {
        modsFile = f;
      } else if (f.getName().endsWith(".resource")) {
        resourceFile = f;
      }
    }
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
    IoTDBSinkTransportClient client =
        new IoTDBSinkTransportClient(
            pipe, "127.0.0.1", IoTDBDescriptor.getInstance().getConfig().getRpcPort(), "127.0.0.1");
    client.handshake();
    for (PipeData pipeData : pipeDataList) {
      client.senderTransport(pipeData);
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
