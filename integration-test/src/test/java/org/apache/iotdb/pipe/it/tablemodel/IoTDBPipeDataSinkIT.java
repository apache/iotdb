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

package org.apache.iotdb.pipe.it.tablemodel;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.confignode.rpc.thrift.TCreatePipeReq;
import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT2TableModel;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.write.record.Tablet;
import org.junit.Assert;
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
import java.util.function.Consumer;

@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2TableModel.class})
public class IoTDBPipeDataSinkIT extends AbstractPipeTableModelTestIT {
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

      extractorAttributes.put("extractor.realtime.mode", "forced-log");
      extractorAttributes.put("capture.table", "true");
      extractorAttributes.put("capture.tree", "true");

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

      TableModelUtils.assertCountData("test", "test", 100, receiverEnv);

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

      TableModelUtils.assertCountData("test", "test", 300, receiverEnv);
    }
  }

  @Test
  public void testWriteBackSink() throws Exception {
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

      extractorAttributes.put("capture.table", "true");
      extractorAttributes.put("capture.tree", "true");
      extractorAttributes.put("forwarding-pipe-requests", "false");

      processorAttributes.put("processor", "rename-database-processor");
      processorAttributes.put("processor.new-db-name", "test1");

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

      TableModelUtils.assertCountData("test1", "test", 100, receiverEnv, handleFailure);
    }
  }

  @Test
  public void testSinkTsFileFormat2() throws Exception {
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

      insertTablet(testResult, test1Result);

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
      TableModelUtils.assertData("test1", entry.getKey(), set, receiverEnv, handleFailure);
    }
  }

  private void insertTablet(
      final Map<String, List<Tablet>> testResult, final Map<String, List<Tablet>> test1Result) {

    final Random random = new Random();
    int deviceIDStartIndex = 0;
    int deviceIDEndIndex = 100;

    for (int i = 0; i < 10; i++) {
      final String tableName = "test" + i;
      for (int j = 0; j < 10; j++) {
        final String dataBaseName = "test" + j % 2;
        Tablet tablet =
            TableModelUtils.generateTablet(
                tableName, deviceIDStartIndex, deviceIDEndIndex, 0, 10, false, true);
        TableModelUtils.insertTablet(dataBaseName, tablet, senderEnv);
        Map<String, List<Tablet>> map = j % 2 == 0 ? testResult : test1Result;
        map.computeIfAbsent(dataBaseName, k -> new ArrayList<>()).add(tablet);
        deviceIDStartIndex += 25;
        deviceIDEndIndex += 25;
      }
    }

    deviceIDStartIndex = 0;
    deviceIDEndIndex = 100;

    for (int i = 0; i < 5; i++) {
      final String tableName = "test" + i;
      for (int j = 0; j < 10; j++) {
        final String dataBaseName = "test" + j % 2;
        Tablet tablet =
            TableModelUtils.generateTablet(
                tableName, deviceIDStartIndex, deviceIDEndIndex, 10, false, true);
        TableModelUtils.insertTablet(dataBaseName, tablet, senderEnv);
        Map<String, List<Tablet>> map = j % 2 == 0 ? testResult : test1Result;
        map.computeIfAbsent(dataBaseName, k -> new ArrayList<>()).add(tablet);
        deviceIDStartIndex += 25;
        deviceIDEndIndex += 25;
      }
    }

    for (int i = 0; i < 5; i++) {
      final String tableName = "test" + i;
      for (int j = 0; j < 10; j++) {
        final String dataBaseName = "test" + j % 2;
        deviceIDStartIndex = random.nextInt(1 << 16) - 25;
        deviceIDEndIndex = deviceIDStartIndex + 25;
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
        Map<String, List<Tablet>> map = j % 2 == 0 ? testResult : test1Result;
        map.computeIfAbsent(dataBaseName, k -> new ArrayList<>()).add(tablet);
      }
    }

    for (int i = 0; i < 5; i++) {
      final String tableName = "test" + i;
      for (int j = 0; j < 10; j++) {
        final String dataBaseName = "test" + j % 2;
        deviceIDStartIndex = random.nextInt(1 << 16) - 25;
        deviceIDEndIndex = deviceIDStartIndex + 25;
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
        Map<String, List<Tablet>> map = j % 2 == 0 ? testResult : test1Result;
        map.computeIfAbsent(dataBaseName, k -> new ArrayList<>()).add(tablet);
      }
    }

    for (int i = 0; i < 5; i++) {
      final String tableName = "test" + i;
      for (int j = 0; j < 10; j++) {
        final String dataBaseName = "test" + j % 2;
        deviceIDStartIndex = random.nextInt(1 << 16) - 25;
        deviceIDEndIndex = deviceIDStartIndex + 25;
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
        Map<String, List<Tablet>> map = j % 2 == 0 ? testResult : test1Result;
        map.computeIfAbsent(dataBaseName, k -> new ArrayList<>()).add(tablet);
      }
    }

    for (int i = 0; i < 5; i++) {
      final String tableName = "test" + i;
      for (int j = 0; j < 10; j++) {
        final String dataBaseName = "test" + j % 2;
        deviceIDStartIndex = random.nextInt(1 << 16) - 25;
        deviceIDEndIndex = deviceIDStartIndex + 25;
        Tablet tablet =
            TableModelUtils.generateTablet(
                tableName, deviceIDStartIndex, deviceIDEndIndex, 100, 110, false, true);
        TableModelUtils.insertTablet(dataBaseName, tablet, senderEnv);
        TableModelUtils.insertTablet(dataBaseName, tablet, senderEnv);
        Map<String, List<Tablet>> map = j % 2 == 0 ? testResult : test1Result;
        map.computeIfAbsent(dataBaseName, k -> new ArrayList<>()).add(tablet);
      }
    }

    deviceIDStartIndex = random.nextInt(1 << 16) - 300;
    deviceIDEndIndex = deviceIDStartIndex + 25;
    for (int i = 0; i < 5; i++) {
      final String tableName = "test" + i;
      for (int j = 0; j < 10; j++) {
        final String dataBaseName = "test" + j % 2;
        Tablet tablet =
            TableModelUtils.generateTablet(
                tableName, deviceIDStartIndex, deviceIDEndIndex, 100, 110, false, true);
        TableModelUtils.insertTablet(dataBaseName, tablet, senderEnv);
        Map<String, List<Tablet>> map = j % 2 == 0 ? testResult : test1Result;
        map.computeIfAbsent(dataBaseName, k -> new ArrayList<>()).add(tablet);
        deviceIDStartIndex += 25;
        deviceIDEndIndex += 25;
      }
    }
  }
}
