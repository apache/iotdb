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

package org.apache.iotdb.pipe.it.dual.tablemodel.manual.basic;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.confignode.rpc.thrift.TCreatePipeReq;
import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT2DualTableManualBasic;
import org.apache.iotdb.pipe.it.dual.tablemodel.TableModelUtils;
import org.apache.iotdb.pipe.it.dual.tablemodel.manual.AbstractPipeTableModelDualManualIT;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.write.record.Tablet;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2DualTableManualBasic.class})
public class IoTDBPipeDataSinkIT extends AbstractPipeTableModelDualManualIT {

  @Override
  @Before
  public void setUp() {
    super.setUp();
  }

  @Test
  public void testThriftConnectorWithRealtimeFirstDisabled() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();
    final Consumer<String> handleFailure =
        o -> {
          TestUtils.executeNonQueryWithRetry(senderEnv, "flush");
          TestUtils.executeNonQueryWithRetry(receiverEnv, "flush");
        };

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {

      TableModelUtils.createDataBaseAndTable(senderEnv, "test", "test");
      TableModelUtils.insertData("test", "test", 0, 50, senderEnv, true);

      if (!TestUtils.tryExecuteNonQueriesWithRetry(
          senderEnv,
          Arrays.asList("insert into root.vehicle.d0(time, s1) values (0, 1)", "flush"))) {
        return;
      }

      final Map<String, String> extractorAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> connectorAttributes = new HashMap<>();

      extractorAttributes.put("extractor.realtime.mode", "log");
      extractorAttributes.put("capture.table", "true");
      extractorAttributes.put("capture.tree", "true");
      extractorAttributes.put("user", "root");

      connectorAttributes.put("connector", "iotdb-thrift-connector");
      connectorAttributes.put("connector.batch.enable", "true");
      connectorAttributes.put("connector.ip", receiverIp);
      connectorAttributes.put("connector.port", Integer.toString(receiverPort));
      connectorAttributes.put("connector.realtime-first", "false");

      final TSStatus status =
          client.createPipe(
              new TCreatePipeReq("testPipe", connectorAttributes)
                  .setExtractorAttributes(extractorAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("testPipe").getCode());

      TableModelUtils.insertData("test", "test", 50, 100, senderEnv, true);

      if (!TestUtils.tryExecuteNonQueriesWithRetry(
          senderEnv,
          Arrays.asList("insert into root.vehicle.d0(time, s1) values (1, 1)", "flush"))) {
        return;
      }

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "select * from root.**",
          "Time,root.vehicle.d0.s1,",
          Collections.unmodifiableSet(new HashSet<>(Arrays.asList("0,1.0,", "1,1.0,"))),
          handleFailure);

      TableModelUtils.assertCountData("test", "test", 100, receiverEnv, handleFailure);
    }
  }

  @Test
  public void testSinkTabletFormat() throws Exception {
    testSinkFormat("tablet");
  }

  @Test
  public void testSinkTsFileFormat() throws Exception {
    testSinkFormat("tsfile");
  }

  @Test
  public void testSinkHybridFormat() throws Exception {
    testSinkFormat("hybrid");
  }

  private void testSinkFormat(final String format) throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();
    final Consumer<String> handleFailure =
        o -> {
          TestUtils.executeNonQueryWithRetry(senderEnv, "flush");
          TestUtils.executeNonQueryWithRetry(receiverEnv, "flush");
        };

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {

      TableModelUtils.createDataBaseAndTable(senderEnv, "test", "test");
      TableModelUtils.insertData("test", "test", 0, 50, senderEnv, true);

      if (!TestUtils.tryExecuteNonQueriesWithRetry(
          senderEnv,
          Arrays.asList("insert into root.vehicle.d0(time, s1) values (1, 1)", "flush"))) {
        return;
      }

      final Map<String, String> extractorAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> connectorAttributes = new HashMap<>();

      extractorAttributes.put("capture.table", "true");
      extractorAttributes.put("capture.tree", "true");
      extractorAttributes.put("user", "root");

      connectorAttributes.put("connector", "iotdb-thrift-connector");
      connectorAttributes.put("connector.batch.enable", "true");
      connectorAttributes.put("connector.ip", receiverIp);
      connectorAttributes.put("connector.port", Integer.toString(receiverPort));
      connectorAttributes.put("connector.format", format);
      connectorAttributes.put("connector.realtime-first", "false");

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(),
          client
              .createPipe(
                  new TCreatePipeReq("testPipe", connectorAttributes)
                      .setExtractorAttributes(extractorAttributes)
                      .setProcessorAttributes(processorAttributes))
              .getCode());

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("testPipe").getCode());

      TableModelUtils.insertData("test", "test", 50, 150, senderEnv, true);

      if (!TestUtils.tryExecuteNonQueriesWithRetry(
          senderEnv,
          Arrays.asList("insert into root.vehicle.d0(time, s1) values (2, 1)", "flush"))) {
        return;
      }

      TableModelUtils.assertCountData("test", "test", 150, receiverEnv, handleFailure);

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "select * from root.**",
          "Time,root.vehicle.d0.s1,",
          Collections.unmodifiableSet(new HashSet<>(Arrays.asList("1,1.0,", "2,1.0,"))),
          handleFailure);

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.dropPipe("testPipe").getCode());

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(),
          client
              .createPipe(
                  new TCreatePipeReq("testPipe", connectorAttributes)
                      .setExtractorAttributes(extractorAttributes)
                      .setProcessorAttributes(processorAttributes))
              .getCode());

      // Do not fail if the failure has nothing to do with pipe
      // Because the failures will randomly generate due to resource limitation
      if (!TestUtils.tryExecuteNonQueriesWithRetry(
          senderEnv,
          Arrays.asList(
              "insert into root.vehicle.d0(time, s1) values (4, 1)",
              "insert into root.vehicle.d0(time, s1) values (3, 1), (0, 1)",
              "flush"))) {
        return;
      }

      TableModelUtils.insertData("test", "test", 150, 200, senderEnv, true);
      TableModelUtils.insertTablet("test", "test", 200, 250, senderEnv, true);
      TableModelUtils.insertTablet("test", "test", 250, 300, senderEnv, true);
      TableModelUtils.insertTablet("test", "test", 300, 350, senderEnv, true);

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "select * from root.**",
          "Time,root.vehicle.d0.s1,",
          Collections.unmodifiableSet(
              new HashSet<>(Arrays.asList("0,1.0,", "1,1.0,", "2,1.0,", "3,1.0,", "4,1.0,"))),
          handleFailure);

      TableModelUtils.assertCountData("test", "test", 350, receiverEnv, handleFailure);
    }
  }

  @Test
  public void testWriteBackSink() throws Exception {
    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      final Map<String, String> extractorAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> connectorAttributes = new HashMap<>();

      extractorAttributes.put("capture.table", "true");
      extractorAttributes.put("capture.tree", "true");
      extractorAttributes.put("forwarding-pipe-requests", "false");
      extractorAttributes.put("extractor.database-name", "test.*");
      extractorAttributes.put("extractor.table-name", "test.*");
      extractorAttributes.put("user", "root");

      processorAttributes.put("processor", "rename-database-processor");
      processorAttributes.put("processor.new-db-name", "Test1");

      connectorAttributes.put("connector", "write-back-sink");
      connectorAttributes.put("user", "root");

      final TSStatus status =
          client.createPipe(
              new TCreatePipeReq("testPipe", connectorAttributes)
                  .setExtractorAttributes(extractorAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("testPipe").getCode());

      if (!TestUtils.tryExecuteNonQueriesWithRetry(
          senderEnv,
          Arrays.asList("insert into root.vehicle.d0(time, s1) values (1, 1)", "flush"))) {
        return;
      }

      TableModelUtils.createDataBaseAndTable(senderEnv, "test", "test");
      TableModelUtils.insertDataNotThrowError("test", "test", 0, 20, senderEnv);

      TableModelUtils.insertTablet("test", "test", 20, 200, senderEnv, true);

      TableModelUtils.insertTablet("test", "test", 200, 400, senderEnv, true);

      TableModelUtils.assertCountData("test1", "test", 400, senderEnv);
    }
  }

  @Test
  public void testSinkTsFileFormat2() throws Exception {
    doTest(this::insertTablet1);
  }

  @Test
  public void testSinkTsFileFormat3() throws Exception {
    doTest(this::insertTablet2);
  }

  @Test
  public void testSinkTsFileFormat4() throws Exception {
    doTest(this::insertTablet3);
  }

  @Test
  public void testSinkTsFileFormat5() throws Exception {
    doTest(this::insertTablet4);
  }

  @Test
  public void testSinkTsFileFormat6() throws Exception {
    doTest(this::insertTablet5);
  }

  @Test
  public void testSinkTsFileFormat7() throws Exception {
    doTest(this::insertTablet6);
  }

  @Test
  public void testSinkTsFileFormat8() throws Exception {
    doTest(this::insertTablet7);
  }

  @Test
  public void testSinkTsFileFormat9() throws Exception {
    doTest(this::insertTablet8);
  }

  @Test
  public void testSinkTsFileFormat10() throws Exception {
    doTest(this::insertTablet9);
  }

  private void doTest(BiConsumer<Map<String, List<Tablet>>, Map<String, List<Tablet>>> consumer)
      throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();
    final Consumer<String> handleFailure =
        o -> {
          TestUtils.executeNonQueryWithRetry(senderEnv, "flush");
          TestUtils.executeNonQueryWithRetry(receiverEnv, "flush");
        };

    Map<String, List<Tablet>> testResult = new HashMap<>();
    Map<String, List<Tablet>> test1Result = new HashMap<>();

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {

      for (int i = 0; i < 5; i++) {
        TableModelUtils.createDataBaseAndTable(senderEnv, "test" + i, "test0");
        TableModelUtils.createDataBaseAndTable(senderEnv, "test" + i, "test1");
      }

      if (!TestUtils.tryExecuteNonQueriesWithRetry(
          senderEnv,
          Arrays.asList("insert into root.vehicle.d0(time, s1) values (1, 1)", "flush"))) {
        return;
      }

      final Map<String, String> extractorAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> connectorAttributes = new HashMap<>();

      extractorAttributes.put("capture.table", "true");
      extractorAttributes.put("capture.tree", "true");
      extractorAttributes.put("extractor.database-name", "test.*");
      extractorAttributes.put("extractor.table-name", "test.*");
      extractorAttributes.put("user", "root");

      connectorAttributes.put("connector", "iotdb-thrift-connector");
      connectorAttributes.put("connector.ip", receiverIp);
      connectorAttributes.put("connector.port", Integer.toString(receiverPort));
      connectorAttributes.put("connector.format", "tsfile");
      connectorAttributes.put("connector.realtime-first", "true");

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(),
          client
              .createPipe(
                  new TCreatePipeReq("testPipe", connectorAttributes)
                      .setExtractorAttributes(extractorAttributes)
                      .setProcessorAttributes(processorAttributes))
              .getCode());

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("testPipe").getCode());

      if (!TestUtils.tryExecuteNonQueriesWithRetry(
          senderEnv,
          Arrays.asList("insert into root.vehicle.d0(time, s1) values (2, 1)", "flush"))) {
        return;
      }

      consumer.accept(testResult, test1Result);

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "select * from root.**",
          "Time,root.vehicle.d0.s1,",
          Collections.unmodifiableSet(new HashSet<>(Arrays.asList("1,1.0,", "2,1.0,"))),
          handleFailure);

      if (!TestUtils.tryExecuteNonQueriesWithRetry(
          senderEnv,
          Arrays.asList(
              "insert into root.vehicle.d0(time, s1) values (4, 1)",
              "insert into root.vehicle.d0(time, s1) values (3, 1), (0, 1)",
              "flush"))) {
        return;
      }

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "select * from root.**",
          "Time,root.vehicle.d0.s1,",
          Collections.unmodifiableSet(
              new HashSet<>(Arrays.asList("0,1.0,", "1,1.0,", "2,1.0,", "3,1.0,", "4,1.0,"))),
          handleFailure);
    }

    for (Map.Entry<String, List<Tablet>> entry : testResult.entrySet()) {
      final Set<String> set = new HashSet<>();
      entry
          .getValue()
          .forEach(
              tablet -> {
                set.addAll(TableModelUtils.generateExpectedResults(tablet));
              });
      TableModelUtils.assertCountData(
          "test0", entry.getKey(), set.size(), receiverEnv, handleFailure);
      TableModelUtils.assertData("test0", entry.getKey(), set, receiverEnv, handleFailure);
    }

    for (Map.Entry<String, List<Tablet>> entry : test1Result.entrySet()) {
      final Set<String> set = new HashSet<>();
      entry
          .getValue()
          .forEach(
              tablet -> {
                set.addAll(TableModelUtils.generateExpectedResults(tablet));
              });
      TableModelUtils.assertCountData(
          "test1", entry.getKey(), set.size(), receiverEnv, handleFailure);
      TableModelUtils.assertData("test1", entry.getKey(), set, receiverEnv, handleFailure);
    }
  }

  private void insertTablet1(
      final Map<String, List<Tablet>> testResult, final Map<String, List<Tablet>> test1Result) {

    int deviceIDStartIndex = 0;
    int deviceIDEndIndex = 10;

    for (int j = 0; j < 25; j++) {
      final String dataBaseName = "test" + j % 2;
      for (int i = 0; i < 5; i++) {
        final String tableName = "test" + i;
        Tablet tablet =
            TableModelUtils.generateTablet(
                tableName, deviceIDStartIndex, deviceIDEndIndex, 0, 10, false, false);
        TableModelUtils.insertTablet(dataBaseName, tablet, senderEnv);
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
        Map<String, List<Tablet>> map = j % 2 == 0 ? testResult : test1Result;
        map.computeIfAbsent(tableName, k -> new ArrayList<>()).add(tablet);
      }
      deviceIDStartIndex += 2;
      deviceIDEndIndex += 2;
    }
  }

  private void insertTablet2(
      final Map<String, List<Tablet>> testResult, final Map<String, List<Tablet>> test1Result) {
    int deviceIDStartIndex = 0;
    int deviceIDEndIndex = 10;

    for (int j = 0; j < 25; j++) {
      final String dataBaseName = "test" + j % 2;
      for (int i = 0; i < 5; i++) {
        final String tableName = "test" + i;
        Tablet tablet =
            TableModelUtils.generateTablet(
                tableName, deviceIDStartIndex, deviceIDEndIndex, 10, false, false);
        TableModelUtils.insertTablet(dataBaseName, tablet, senderEnv);
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
        Map<String, List<Tablet>> map = j % 2 == 0 ? testResult : test1Result;
        map.computeIfAbsent(tableName, k -> new ArrayList<>()).add(tablet);
      }
      deviceIDStartIndex += 2;
      deviceIDEndIndex += 2;
    }
  }

  private void insertTablet3(
      final Map<String, List<Tablet>> testResult, final Map<String, List<Tablet>> test1Result) {
    final Random random = new Random();
    int deviceIDStartIndex = 0;
    int deviceIDEndIndex = 100;

    for (int j = 0; j < 25; j++) {
      for (int i = 0; i < 5; i++) {
        final String tableName = "test" + i;
        final String dataBaseName = "test" + j % 2;
        deviceIDStartIndex = random.nextInt(1 << 16) - 10;
        deviceIDEndIndex = deviceIDStartIndex + 10;
        Tablet tablet =
            TableModelUtils.generateTablet(
                tableName,
                deviceIDStartIndex,
                deviceIDEndIndex,
                deviceIDStartIndex,
                deviceIDEndIndex,
                false,
                true);
        TableModelUtils.insertTablet(dataBaseName, tablet, senderEnv);
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
        Map<String, List<Tablet>> map = j % 2 == 0 ? testResult : test1Result;
        map.computeIfAbsent(tableName, k -> new ArrayList<>()).add(tablet);
      }
    }
  }

  private void insertTablet4(
      final Map<String, List<Tablet>> testResult, final Map<String, List<Tablet>> test1Result) {
    final Random random = new Random();
    int deviceIDStartIndex = 0;
    int deviceIDEndIndex = 100;

    for (int j = 0; j < 25; j++) {
      final String dataBaseName = "test" + j % 2;
      for (int i = 0; i < 5; i++) {
        final String tableName = "test" + i;
        deviceIDStartIndex = random.nextInt(1 << 16) - 10;
        deviceIDEndIndex = deviceIDStartIndex + 10;
        Tablet tablet =
            TableModelUtils.generateTablet(
                tableName,
                deviceIDStartIndex,
                deviceIDEndIndex,
                deviceIDStartIndex,
                deviceIDEndIndex,
                false,
                false);
        TableModelUtils.insertTablet(dataBaseName, tablet, senderEnv);
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
        Map<String, List<Tablet>> map = j % 2 == 0 ? testResult : test1Result;
        map.computeIfAbsent(tableName, k -> new ArrayList<>()).add(tablet);
      }
    }
  }

  private void insertTablet5(
      final Map<String, List<Tablet>> testResult, final Map<String, List<Tablet>> test1Result) {
    final Random random = new Random();
    int deviceIDStartIndex = 0;
    int deviceIDEndIndex = 100;
    for (int j = 0; j < 25; j++) {
      for (int i = 0; i < 5; i++) {
        final String tableName = "test" + i;
        final String dataBaseName = "test" + j % 2;
        deviceIDStartIndex = random.nextInt(1 << 16) - 10;
        deviceIDEndIndex = deviceIDStartIndex + 10;
        Tablet tablet =
            TableModelUtils.generateTablet(
                tableName, 0, 10, deviceIDStartIndex, deviceIDEndIndex, false, true);
        TableModelUtils.insertTablet(dataBaseName, tablet, senderEnv);
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
        Map<String, List<Tablet>> map = j % 2 == 0 ? testResult : test1Result;
        map.computeIfAbsent(tableName, k -> new ArrayList<>()).add(tablet);
      }
    }
  }

  private void insertTablet6(
      final Map<String, List<Tablet>> testResult, final Map<String, List<Tablet>> test1Result) {
    final Random random = new Random();
    int deviceIDStartIndex = 0;
    int deviceIDEndIndex = 100;

    for (int j = 0; j < 25; j++) {
      final String dataBaseName = "test" + j % 2;
      for (int i = 0; i < 5; i++) {
        final String tableName = "test" + i;
        deviceIDStartIndex = random.nextInt(1 << 16) - 10;
        deviceIDEndIndex = deviceIDStartIndex + 10;
        Tablet tablet =
            TableModelUtils.generateTablet(
                tableName, deviceIDStartIndex, deviceIDEndIndex, 100, 110, false, true);
        TableModelUtils.insertTablet(dataBaseName, tablet, senderEnv);
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
        Map<String, List<Tablet>> map = j % 2 == 0 ? testResult : test1Result;
        map.computeIfAbsent(tableName, k -> new ArrayList<>()).add(tablet);
      }
    }
  }

  private void insertTablet7(
      final Map<String, List<Tablet>> testResult, final Map<String, List<Tablet>> test1Result) {

    final Random random = new Random();
    int deviceIDStartIndex = 0;
    int deviceIDEndIndex = 100;
    deviceIDStartIndex = random.nextInt(1 << 16) - 10;
    deviceIDEndIndex = deviceIDStartIndex + 10;
    for (int j = 0; j < 25; j++) {
      final String dataBaseName = "test" + j % 2;
      for (int i = 0; i < 5; i++) {
        final String tableName = "test" + i;
        Tablet tablet =
            TableModelUtils.generateTablet(
                tableName, deviceIDStartIndex, deviceIDEndIndex, 100, 110, false, true);
        TableModelUtils.insertTablet(dataBaseName, tablet, senderEnv);
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
        Map<String, List<Tablet>> map = j % 2 == 0 ? testResult : test1Result;
        map.computeIfAbsent(tableName, k -> new ArrayList<>()).add(tablet);
      }
      deviceIDStartIndex += 2;
      deviceIDEndIndex += 2;
    }
  }

  private void insertTablet8(
      final Map<String, List<Tablet>> testResult, final Map<String, List<Tablet>> test1Result) {
    final Random random = new Random();
    int deviceIDStartIndex = random.nextInt(1 << 16);
    int deviceIDEndIndex = deviceIDStartIndex + 10;
    for (int j = 0; j < 25; j++) {
      final String dataBaseName = "test" + j % 2;
      for (int i = 0; i < 5; i++) {
        final String tableName = "test" + i;
        Tablet tablet =
            TableModelUtils.generateTablet(
                tableName, 100, 110, deviceIDStartIndex, deviceIDEndIndex, false, true);
        TableModelUtils.insertTablet(dataBaseName, tablet, senderEnv);
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
        Map<String, List<Tablet>> map = j % 2 == 0 ? testResult : test1Result;
        map.computeIfAbsent(tableName, k -> new ArrayList<>()).add(tablet);
      }
      deviceIDStartIndex += 2;
      deviceIDEndIndex += 2;
    }
  }

  private void insertTablet9(
      final Map<String, List<Tablet>> testResult, final Map<String, List<Tablet>> test1Result) {
    final Random random = new Random();
    for (int j = 0; j < 25; j++) {
      final String dataBaseName = "test" + j % 2;
      for (int i = 0; i < 5; i++) {
        final String tableName = "test" + i;
        Tablet tablet =
            TableModelUtils.generateTabletDeviceIDAllIsNull(tableName, 100, 110, 10, false);
        TableModelUtils.insertTablet(dataBaseName, tablet, senderEnv);
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
        Map<String, List<Tablet>> map = j % 2 == 0 ? testResult : test1Result;
        map.computeIfAbsent(tableName, k -> new ArrayList<>()).add(tablet);
      }
    }
  }

  @Test
  public void testLoadTsFileWithoutVerify() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {

      // Do not fail if the failure has nothing to do with pipe
      // Because the failures will randomly generate due to resource limitation
      if (!TestUtils.tryExecuteNonQueriesWithRetry(
          senderEnv,
          Arrays.asList("insert into root.vehicle.d0(time, s1) values (1, 1)", "flush"))) {
        return;
      }

      for (int i = 0; i < 5; i++) {
        TableModelUtils.createDataBaseAndTable(senderEnv, "test" + i, "test0");
        TableModelUtils.createDataBaseAndTable(senderEnv, "test" + i, "test1");
      }

      final Map<String, String> extractorAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> connectorAttributes = new HashMap<>();

      extractorAttributes.put("extractor.realtime.mode", "batch");
      extractorAttributes.put("capture.table", "true");
      extractorAttributes.put("capture.tree", "true");
      extractorAttributes.put("user", "root");

      connectorAttributes.put("sink", "iotdb-thrift-sink");
      connectorAttributes.put("sink.batch.enable", "false");
      connectorAttributes.put("sink.ip", receiverIp);
      connectorAttributes.put("sink.port", Integer.toString(receiverPort));
      connectorAttributes.put("sink.tsfile.validation", "false");

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(),
          client
              .createPipe(
                  new TCreatePipeReq("testPipe", connectorAttributes)
                      .setExtractorAttributes(extractorAttributes)
                      .setProcessorAttributes(processorAttributes))
              .getCode());

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("testPipe").getCode());

      // Do not fail if the failure has nothing to do with pipe
      // Because the failures will randomly generate due to resource limitation
      if (!TestUtils.tryExecuteNonQueriesWithRetry(
          senderEnv,
          Arrays.asList(
              "create timeSeries root.vehicle.d0.s1 int32",
              "insert into root.vehicle.d0(time, s1) values (2, 1)",
              "flush"))) {
        return;
      }

      Map<String, List<Tablet>> testResult = new HashMap<>();
      Map<String, List<Tablet>> test1Result = new HashMap<>();

      insertTablet1(testResult, test1Result);

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "select * from root.**",
          "Time,root.vehicle.d0.s1,",
          Collections.unmodifiableSet(new HashSet<>(Arrays.asList("1,1.0,", "2,1.0,"))));

      for (Map.Entry<String, List<Tablet>> entry : testResult.entrySet()) {
        final Set<String> set = new HashSet<>();
        entry
            .getValue()
            .forEach(
                tablet -> {
                  set.addAll(TableModelUtils.generateExpectedResults(tablet));
                });
        TableModelUtils.assertCountData("test0", entry.getKey(), set.size(), receiverEnv, s -> {});
        TableModelUtils.assertData("test0", entry.getKey(), set, receiverEnv, s -> {});
      }

      for (Map.Entry<String, List<Tablet>> entry : test1Result.entrySet()) {
        final Set<String> set = new HashSet<>();
        entry
            .getValue()
            .forEach(
                tablet -> {
                  set.addAll(TableModelUtils.generateExpectedResults(tablet));
                });
        TableModelUtils.assertCountData("test1", entry.getKey(), set.size(), receiverEnv, s -> {});
        TableModelUtils.assertData("test1", entry.getKey(), set, receiverEnv, s -> {});
      }
    }
  }
}
