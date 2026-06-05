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
import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT2DualTableManualBasic;
import org.apache.iotdb.itbase.env.BaseEnv;
import org.apache.iotdb.pipe.it.dual.tablemodel.manual.AbstractPipeTableModelDualManualIT;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.write.record.Tablet;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2DualTableManualBasic.class})
public class IoTDBPipeObjectDataSinkIT extends AbstractPipeTableModelDualManualIT {

  private static final String DB = "db_obj";
  private static final String TABLE = "t_obj";
  private static final int HISTORY_START = 1;
  private static final int HISTORY_END = 20;
  private static final int REALTIME_START = 21;
  private static final int REALTIME_END = 40;
  private static final String PIPE_NAME = "p_obj_sink";
  private static final String ROOT_USER = "root";
  private static final String ROOT_PASSWORD = "TimechoDB@2021";

  @Override
  protected void setupConfig() {
    super.setupConfig();
    receiverEnv.getConfig().getCommonConfig().setPipeAirGapReceiverEnabled(true);
  }

  @Test
  public void testThriftSyncSinkMixTabletBatchNoPattern() throws Exception {
    doTestReceiverSinkScenario(PIPE_NAME, "iotdb-thrift-sync-sink", false, "tablet", true);
  }

  @Test
  public void testThriftSyncSinkMixTabletNonBatchNoPattern() throws Exception {
    doTestReceiverSinkScenario(PIPE_NAME, "iotdb-thrift-sync-sink", false, "tablet", false);
  }

  @Test
  public void testThriftSyncSinkMixTsFileBatchNoPattern() throws Exception {
    doTestReceiverSinkScenario(PIPE_NAME, "iotdb-thrift-sync-sink", false, "tsfile", true);
  }

  @Test
  public void testThriftSyncSinkMixHybridBatchNoPattern() throws Exception {
    doTestReceiverSinkScenario(PIPE_NAME, "iotdb-thrift-sync-sink", false, "hybrid", true);
  }

  @Test
  public void testThriftSyncSinkMixHybridNonBatchNoPattern() throws Exception {
    doTestReceiverSinkScenario(PIPE_NAME, "iotdb-thrift-sync-sink", false, "hybrid", false);
  }

  @Test
  public void testThriftAsyncSinkMixTabletBatchNoPattern() throws Exception {
    doTestReceiverSinkScenario(PIPE_NAME, "iotdb-thrift-async-sink", false, "tablet", true);
  }

  @Test
  public void testThriftAsyncSinkMixTabletNonBatchNoPattern() throws Exception {
    doTestReceiverSinkScenario(PIPE_NAME, "iotdb-thrift-async-sink", false, "tablet", false);
  }

  @Test
  public void testThriftAsyncSinkMixTsFileBatchNoPattern() throws Exception {
    doTestReceiverSinkScenario(PIPE_NAME, "iotdb-thrift-async-sink", false, "tsfile", true);
  }

  @Test
  public void testThriftAsyncSinkMixHybridBatchNoPattern() throws Exception {
    doTestReceiverSinkScenario(PIPE_NAME, "iotdb-thrift-async-sink", false, "hybrid", true);
  }

  @Test
  public void testThriftAsyncSinkMixHybridNonBatchNoPattern() throws Exception {
    doTestReceiverSinkScenario(PIPE_NAME, "iotdb-thrift-async-sink", false, "hybrid", false);
  }

  @Test
  public void testAirGapSinkMixTabletBatchNoPattern() throws Exception {
    doTestReceiverSinkScenario(PIPE_NAME, "iotdb-air-gap-sink", true, "tablet", true);
  }

  @Test
  public void testAirGapSinkMixTabletNonBatchNoPattern() throws Exception {
    doTestReceiverSinkScenario(PIPE_NAME, "iotdb-air-gap-sink", true, "tablet", false);
  }

  @Test
  public void testAirGapSinkMixTsFileBatchNoPattern() throws Exception {
    doTestReceiverSinkScenario(PIPE_NAME, "iotdb-air-gap-sink", true, "tsfile", true);
  }

  @Test
  public void testAirGapSinkMixHybridBatchNoPattern() throws Exception {
    doTestReceiverSinkScenario(PIPE_NAME, "iotdb-air-gap-sink", true, "hybrid", true);
  }

  @Test
  public void testAirGapSinkMixHybridNonBatchNoPattern() throws Exception {
    doTestReceiverSinkScenario(PIPE_NAME, "iotdb-air-gap-sink", true, "hybrid", false);
  }

  @Test
  public void testWriteBackSinkWithObjectHistoryAndRealtime() throws Exception {
    doTestWriteBackSinkScenario("p_obj_wb", "db_obj_wb", "hybrid", true, true, true, true);
  }

  private void doTestReceiverSinkScenario(
      final String pipeName,
      final String sinkName,
      final boolean useAirGapPort,
      final String sinkFormat,
      final boolean batchEnabled)
      throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);
    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort =
        useAirGapPort ? receiverDataNode.getPipeAirGapReceiverPort() : receiverDataNode.getPort();
    try (final ITableSession senderSession = senderEnv.getTableSessionConnection();
        final SyncConfigNodeIServiceClient client =
            (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      createObjectTable(senderSession, DB, TABLE);
      insertObjectData(senderSession, TABLE, HISTORY_START, HISTORY_END);
      senderSession.executeNonQueryStatement("flush");
      TestUtils.executeNonQueryWithRetry(senderEnv, "flush");

      final Map<String, String> sourceAttributes =
          buildSourceAttributes(DB, TABLE, false, true, true);
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> sinkAttributes = new HashMap<>();
      sinkAttributes.put("sink", sinkName);
      sinkAttributes.put("sink.ip", receiverIp);
      sinkAttributes.put("sink.port", String.valueOf(receiverPort));
      sinkAttributes.put("sink.batch.enable", String.valueOf(batchEnabled));
      sinkAttributes.put("sink.format", sinkFormat);
      sinkAttributes.put("sink.realtime-first", "false");

      final TSStatus createStatus =
          client.createPipe(
              new TCreatePipeReq(pipeName, sinkAttributes)
                  .setExtractorAttributes(sourceAttributes)
                  .setProcessorAttributes(processorAttributes));
      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), createStatus.getCode());

      insertObjectData(senderSession, TABLE, REALTIME_START, REALTIME_END);
      TestUtils.executeNonQueryWithRetry(senderEnv, "flush");
      TestUtils.executeNonQueryWithRetry(receiverEnv, "flush");

      assertObjectRangeEventually(receiverEnv, DB, TABLE, HISTORY_START, HISTORY_END);
      assertObjectRangeEventually(receiverEnv, DB, TABLE, REALTIME_START, REALTIME_END);

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.dropPipe(pipeName).getCode());
    }
  }

  private void doTestWriteBackSinkScenario(
      final String pipeName,
      final String targetDb,
      final String sinkFormat,
      final boolean batchEnabled,
      final boolean withPattern,
      final boolean historyEnabled,
      final boolean realtimeEnabled)
      throws Exception {
    try (final ITableSession senderSession = senderEnv.getTableSessionConnection();
        final SyncConfigNodeIServiceClient client =
            (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      createObjectTable(senderSession, DB, TABLE);
      if (historyEnabled) {
        insertObjectData(senderSession, TABLE, HISTORY_START, HISTORY_END);
        TestUtils.executeNonQueryWithRetry(senderEnv, "flush");
      }

      final Map<String, String> sourceAttributes =
          buildSourceAttributes(DB, TABLE, withPattern, historyEnabled, realtimeEnabled);
      sourceAttributes.put("extractor.forwarding-pipe-requests", "false");
      final Map<String, String> processorAttributes = new HashMap<>();
      processorAttributes.put("processor", "rename-database-processor");
      processorAttributes.put("processor.new-db-name", targetDb);
      final Map<String, String> sinkAttributes = new HashMap<>();
      sinkAttributes.put("sink", "write-back-sink");
      sinkAttributes.put("sink.batch.enable", String.valueOf(batchEnabled));
      sinkAttributes.put("sink.format", sinkFormat);
      sinkAttributes.put("sink.username", ROOT_USER);
      sinkAttributes.put("sink.password", ROOT_PASSWORD);

      final TSStatus createStatus =
          client.createPipe(
              new TCreatePipeReq(pipeName, sinkAttributes)
                  .setExtractorAttributes(sourceAttributes)
                  .setProcessorAttributes(processorAttributes));
      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), createStatus.getCode());

      if (realtimeEnabled) {
        insertObjectData(senderSession, TABLE, REALTIME_START, REALTIME_END);
        TestUtils.executeNonQueryWithRetry(senderEnv, "flush");
      }

      if (historyEnabled) {
        assertObjectRangeEventually(senderEnv, targetDb, TABLE, HISTORY_START, HISTORY_END);
      }
      if (realtimeEnabled) {
        assertObjectRangeEventually(senderEnv, targetDb, TABLE, REALTIME_START, REALTIME_END);
      }

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.dropPipe(pipeName).getCode());
    }
  }

  private static Map<String, String> buildSourceAttributes(
      final String db,
      final String table,
      final boolean withPattern,
      final boolean historyEnabled,
      final boolean realtimeEnabled) {
    final Map<String, String> sourceAttributes = new HashMap<>();
    sourceAttributes.put("extractor.capture.table", "true");
    if (withPattern) {
      sourceAttributes.put("extractor.database-name", db);
      sourceAttributes.put("extractor.table-name", table);
    }
    sourceAttributes.put("extractor.inclusion", "data.insert");
    sourceAttributes.put("extractor.history.enable", String.valueOf(historyEnabled));
    sourceAttributes.put("extractor.realtime.enable", String.valueOf(realtimeEnabled));
    sourceAttributes.put("extractor.realtime.mode", "stream");
    sourceAttributes.put("extractor.forwarding-pipe-requests", "false");
    sourceAttributes.put("extractor.user", ROOT_USER);
    return sourceAttributes;
  }

  private static void createObjectTable(
      final ITableSession session, final String db, final String table) throws Exception {
    session.executeNonQueryStatement("CREATE DATABASE IF NOT EXISTS " + db);
    session.executeNonQueryStatement("USE " + db);
    session.executeNonQueryStatement(
        "CREATE TABLE IF NOT EXISTS " + table + " (id STRING TAG, file OBJECT FIELD)");
  }

  private static void insertObjectData(
      final ITableSession session, final String tableName, final int startTs, final int endTs)
      throws Exception {
    final List<String> columnNames = Arrays.asList("id", "file");
    final List<TSDataType> dataTypes = Arrays.asList(TSDataType.STRING, TSDataType.OBJECT);
    final List<ColumnCategory> columnCategories =
        Arrays.asList(ColumnCategory.TAG, ColumnCategory.FIELD);
    final Tablet tablet = new Tablet(tableName, columnNames, dataTypes, columnCategories, 16);

    for (int ts = startTs; ts <= endTs; ts++) {
      final int rowIndex = tablet.getRowSize();
      tablet.addTimestamp(rowIndex, ts);
      tablet.addValue(rowIndex, 0, "device_1");
      final byte[] bytes = objectPayload(ts);
      tablet.addValue(rowIndex, 1, true, 0, bytes);
      if (tablet.getRowSize() == tablet.getMaxRowNumber()) {
        session.insert(tablet);
        tablet.reset();
      }
    }
    if (tablet.getRowSize() > 0) {
      session.insert(tablet);
    }
  }

  private static byte[] objectPayload(final long ts) {
    return ("object-payload-" + ts).getBytes(StandardCharsets.UTF_8);
  }

  private static void assertObjectRangeEventually(
      final BaseEnv env, final String db, final String table, final int startTs, final int endTs)
      throws Exception {
    final long deadline = System.currentTimeMillis() + 60_000L;
    Throwable lastError = null;
    while (System.currentTimeMillis() < deadline) {
      try (ITableSession session = env.getTableSessionConnection()) {
        final String sql =
            String.format(
                "SELECT time, READ_OBJECT(file) FROM %s.%s WHERE id='device_1' AND time >= %d AND time <= %d ORDER BY time",
                db, table, startTs, endTs);
        try (SessionDataSet dataSet = session.executeQueryStatement(sql)) {
          final SessionDataSet.DataIterator iterator = dataSet.iterator();
          int count = 0;
          while (iterator.next()) {
            final long actualTs = iterator.getLong(1);
            final Binary objectBinary = iterator.getBlob(2);
            Assert.assertArrayEquals(objectPayload(actualTs), objectBinary.getValues());
            count++;
          }
          final int expected = endTs - startTs + 1;
          if (count == expected) {
            return;
          }
          lastError =
              new AssertionError(
                  String.format(
                      "Row count not enough for %s.%s [%d,%d], expected %d but got %d",
                      db, table, startTs, endTs, expected, count));
        }
      } catch (Throwable t) {
        lastError = t;
      }
      Thread.sleep(1000L);
    }
    if (lastError instanceof Exception) {
      throw (Exception) lastError;
    }
    throw new AssertionError(lastError);
  }
}
