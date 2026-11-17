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
import org.apache.iotdb.confignode.rpc.thrift.TDropPipeReq;
import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.isession.ISession;
import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.isession.pool.ITableSessionPool;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestLogger;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT2DualTableManualBasic;
import org.apache.iotdb.itbase.env.BaseEnv;
import org.apache.iotdb.pipe.it.dual.tablemodel.TableModelUtils;
import org.apache.iotdb.pipe.it.dual.tablemodel.manual.AbstractPipeTableModelSharedClusterIT;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.write.record.Tablet;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.LongSummaryStatistics;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.function.Consumer;

@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2DualTableManualBasic.class})
public class IoTDBPipeDataSinkIT extends AbstractPipeTableModelSharedClusterIT {

  // Two full rounds cover both databases and five tables. One more Tablet per database keeps
  // overlapping writes across flushes, with 12 writes instead of 125 (approximately one tenth).
  private static final int[] TABLETS_PER_ROUND = {5, 5, 1, 1};

  @Rule public final TestName testName = new TestName();

  @Override
  @Before
  public void setUp() {
    super.setUp();
  }

  // With realtime-first disabled, batched thrift transfer must deliver both history/realtime
  // table rows and tree points.
  @Test
  public void testThriftConnectorWithRealtimeFirstDisabled() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();
    try (final RetryFlushHandler handleFailure = new RetryFlushHandler(null);
        final SyncConfigNodeIServiceClient client =
            (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {

      TableModelUtils.createDataBaseAndTable(senderEnv, "test", database("test"));
      TableModelUtils.insertData(database("test"), "test", 0, 50, senderEnv, true);

      TestUtils.executeNonQueries(
          senderEnv,
          isolateDatabasePaths(
              Arrays.asList("insert into root.vehicle.d0(time, s1) values (0, 1)", "flush")),
          null);

      final Map<String, String> sourceAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> sinkAttributes = new HashMap<>();

      sourceAttributes.put("source.realtime.mode", "log");
      sourceAttributes.put("capture.table", "true");
      sourceAttributes.put("__system.sql-dialect", "table");
      sourceAttributes.put("capture.tree", "true");
      sourceAttributes.put("mode.double-living", "true");
      sourceAttributes.put("user", "root");

      sinkAttributes.put("sink", "iotdb-thrift-sink");
      sinkAttributes.put("sink.batch.enable", "true");
      sinkAttributes.put("sink.ip", receiverIp);
      sinkAttributes.put("sink.port", Integer.toString(receiverPort));
      sinkAttributes.put("sink.realtime-first", "false");

      final TSStatus status =
          client.createPipe(
              new TCreatePipeReq("testPipe", sinkAttributes)
                  .setExtractorAttributes(sourceAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("testPipe").getCode());

      TableModelUtils.insertData(database("test"), "test", 50, 100, senderEnv, true);

      TestUtils.executeNonQueries(
          senderEnv,
          isolateDatabasePaths(
              Arrays.asList("insert into root.vehicle.d0(time, s1) values (1, 1)", "flush")),
          null);

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          isolateDatabasePaths("select * from root.vehicle.**"),
          isolateDatabasePaths("Time,root.vehicle.d0.s1,"),
          Collections.unmodifiableSet(new HashSet<>(Arrays.asList("0,1.0,", "1,1.0,"))),
          handleFailure);

      TableModelUtils.assertCountData(database("test"), "test", 100, receiverEnv, handleFailure);
    }
  }

  // Tablet-format transfer must preserve the mixed history/realtime data in both models.
  @Test
  public void testSinkTabletFormat() throws Exception {
    testSinkFormat("tablet", false);
  }

  // TsFile-format transfer must preserve the mixed history/realtime data in both models.
  @Test
  public void testSinkTsFileFormat() throws Exception {
    testSinkFormat("tsfile", false);
  }

  // Asynchronously loading transferred TsFiles must eventually expose every expected table row
  // and tree point.
  @Test
  public void testTsFileFormatAndAsyncLoad() throws Exception {
    testSinkFormat("tsfile", true);
  }

  // Hybrid-format transfer must preserve all history/realtime rows across the tested flush
  // boundaries.
  @Test
  public void testSinkHybridFormat() throws Exception {
    testSinkFormat("hybrid", false);
  }

  private void testSinkFormat(final String format, final boolean isAsyncLoad) throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();
    try (final RetryFlushHandler handleFailure = new RetryFlushHandler(null);
        final SyncConfigNodeIServiceClient client =
            (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {

      TableModelUtils.createDataBaseAndTable(senderEnv, "test", database("test"));
      TableModelUtils.insertData(database("test"), "test", 0, 50, senderEnv, true);

      TestUtils.executeNonQueries(
          senderEnv,
          isolateDatabasePaths(
              Arrays.asList("insert into root.vehicle.d0(time, s1) values (1, 1)", "flush")),
          null);

      final Map<String, String> sourceAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> sinkAttributes = new HashMap<>();

      sourceAttributes.put("capture.table", "true");
      sourceAttributes.put("__system.sql-dialect", "table");
      sourceAttributes.put("capture.tree", "true");
      sourceAttributes.put("mode.double-living", "true");
      sourceAttributes.put("user", "root");

      sinkAttributes.put("sink", "iotdb-thrift-sink");
      sinkAttributes.put("sink.batch.enable", "true");
      sinkAttributes.put("sink.ip", receiverIp);
      sinkAttributes.put("sink.port", Integer.toString(receiverPort));
      sinkAttributes.put("sink.format", format);
      if ("tsfile".equals(format)) {
        // Keep TsFile batching, but avoid its 5-second default wait for these small workloads.
        // Tablet/hybrid formats already use a shorter default and must not inherit this delay.
        sinkAttributes.put("sink.batch.max-delay-ms", "1000");
      }
      sinkAttributes.put("sink.load-tsfile-strategy", isAsyncLoad ? "async" : "sync");
      sinkAttributes.put("sink.realtime-first", "false");

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(),
          client
              .createPipe(
                  new TCreatePipeReq("testPipe", sinkAttributes)
                      .setExtractorAttributes(sourceAttributes)
                      .setProcessorAttributes(processorAttributes))
              .getCode());

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("testPipe").getCode());

      TableModelUtils.insertData(database("test"), "test", 50, 150, senderEnv, true);

      TestUtils.executeNonQueries(
          senderEnv,
          isolateDatabasePaths(
              Arrays.asList("insert into root.vehicle.d0(time, s1) values (2, 1)", "flush")),
          null);

      TableModelUtils.assertCountData(database("test"), "test", 150, receiverEnv, handleFailure);

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          isolateDatabasePaths("select * from root.vehicle.**"),
          isolateDatabasePaths("Time,root.vehicle.d0.s1,"),
          Collections.unmodifiableSet(new HashSet<>(Arrays.asList("1,1.0,", "2,1.0,"))),
          handleFailure);

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(),
          client.dropPipeExtended(new TDropPipeReq("testPipe").setIsTableModel(false)).getCode());
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(),
          client.dropPipeExtended(new TDropPipeReq("testPipe").setIsTableModel(true)).getCode());

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(),
          client
              .createPipe(
                  new TCreatePipeReq("testPipe", sinkAttributes)
                      .setExtractorAttributes(sourceAttributes)
                      .setProcessorAttributes(processorAttributes))
              .getCode());

      // Do not fail if the failure has nothing to do with pipe
      // Because the failures will randomly generate due to resource limitation
      TestUtils.executeNonQueries(
          senderEnv,
          Arrays.asList(
              isolateDatabasePaths("insert into root.vehicle.d0(time, s1) values (4, 1)"),
              isolateDatabasePaths("insert into root.vehicle.d0(time, s1) values (3, 1), (0, 1)"),
              "flush"),
          null);

      TableModelUtils.insertData(database("test"), "test", 150, 200, senderEnv, true);
      TableModelUtils.insertTablet(database("test"), "test", 200, 250, senderEnv, true);
      TableModelUtils.insertTablet(database("test"), "test", 250, 300, senderEnv, true);
      TableModelUtils.insertTablet(database("test"), "test", 300, 350, senderEnv, true);

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          isolateDatabasePaths("select * from root.vehicle.**"),
          isolateDatabasePaths("Time,root.vehicle.d0.s1,"),
          Collections.unmodifiableSet(
              new HashSet<>(Arrays.asList("0,1.0,", "1,1.0,", "2,1.0,", "3,1.0,", "4,1.0,"))),
          handleFailure);

      TableModelUtils.assertCountData(database("test"), "test", 350, receiverEnv, handleFailure);
    }
  }

  // Writing back to database Test1 with forwarding disabled must produce all 400 table rows
  // without a forwarding cycle.
  @Test
  public void testWriteBackSink() throws Exception {
    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      final Map<String, String> sourceAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> sinkAttributes = new HashMap<>();

      sourceAttributes.put("capture.table", "true");
      sourceAttributes.put("__system.sql-dialect", "table");
      sourceAttributes.put("capture.tree", "true");
      sourceAttributes.put("mode.double-living", "true");
      sourceAttributes.put("forwarding-pipe-requests", "false");
      sourceAttributes.put("source.database-name", databasePattern("test.*"));
      sourceAttributes.put("source.table-name", "test.*");
      sourceAttributes.put("user", "root");

      sinkAttributes.put("sink", "write-back-sink");
      sinkAttributes.put("sink.database", database("Test1"));
      sinkAttributes.put("user", "root");

      final TSStatus status =
          client.createPipe(
              new TCreatePipeReq("testPipe", sinkAttributes)
                  .setExtractorAttributes(sourceAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("testPipe").getCode());

      TestUtils.executeNonQueries(
          senderEnv,
          isolateDatabasePaths(
              Arrays.asList("insert into root.vehicle.d0(time, s1) values (1, 1)", "flush")),
          null);

      TableModelUtils.createDataBaseAndTable(senderEnv, "test", database("test"));
      TableModelUtils.insertDataNotThrowError(database("test"), "test", 0, 20, senderEnv);

      TableModelUtils.insertTablet(database("test"), "test", 20, 200, senderEnv, true);

      TableModelUtils.insertTablet(database("test"), "test", 200, 400, senderEnv, true);

      TableModelUtils.assertCountData(database("Test1"), "test", 400, senderEnv);
    }
  }

  // TsFile transfer of shifting device ranges with fixed timestamps must match all generated
  // Tablet values.
  @Test
  public void testSinkTsFileFormat2() throws Exception {
    doTest(this::insertTablet1);
  }

  // TsFile transfer of shifting device ranges with generated timestamps must match all generated
  // Tablet values.
  @Test
  public void testSinkTsFileFormat3() throws Exception {
    doTest(this::insertTablet2);
  }

  // TsFile transfer of random device/time ranges must preserve every value from the third Tablet
  // generator.
  @Test
  public void testSinkTsFileFormat4() throws Exception {
    doTest(this::insertTablet3);
  }

  // TsFile transfer of random device/time ranges must preserve every value from the fourth Tablet
  // generator.
  @Test
  public void testSinkTsFileFormat5() throws Exception {
    doTest(this::insertTablet4);
  }

  // TsFile transfer with fixed device IDs and random time ranges must match all generated values.
  @Test
  public void testSinkTsFileFormat6() throws Exception {
    doTest(this::insertTablet5);
  }

  // TsFile transfer with random device ranges and timestamps [100, 110) must match all generated
  // values.
  @Test
  public void testSinkTsFileFormat7() throws Exception {
    doTest(this::insertTablet6);
  }

  // TsFile transfer with shifting device ranges and timestamps [100, 110) must match all
  // generated values.
  @Test
  public void testSinkTsFileFormat8() throws Exception {
    doTest(this::insertTablet7);
  }

  // TsFile transfer with device IDs [100, 110) and shifting time ranges must match all generated
  // values.
  @Test
  public void testSinkTsFileFormat9() throws Exception {
    doTest(this::insertTablet8);
  }

  // TsFile transfer of Tablets with entirely null device IDs must match all generated rows.
  @Test
  public void testSinkTsFileFormat10() throws Exception {
    doTest(this::insertTablet9);
  }

  private void doTest(final TabletWriter consumer) throws Exception {
    // This covers doTest only; cluster setup/cleanup is reported by IoTDBTestRunner separately.
    // Close the recorder on assertion failures too, so slow failures retain their partial timings.
    try (final DoTestTimings timings = new DoTestTimings(testName.getMethodName())) {
      try (final RetryFlushHandler handleFailure = new RetryFlushHandler(timings)) {
        doTest(consumer, timings, handleFailure);
      }
      // A Session close failure also fails the test, so mark success only after closing clients.
      timings.passed = true;
    }
  }

  private void doTest(
      final TabletWriter consumer,
      final DoTestTimings timings,
      final RetryFlushHandler handleFailure)
      throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    Map<String, List<Tablet>> testResult = new HashMap<>();
    Map<String, List<Tablet>> test1Result = new HashMap<>();

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {

      timings.startPhase(DoTestPhase.CREATE_SCHEMA);
      for (int i = 0; i < 5; i++) {
        TableModelUtils.createDataBaseAndTable(senderEnv, "test" + i, database("test0"));
        TableModelUtils.createDataBaseAndTable(senderEnv, "test" + i, database("test1"));
      }

      timings.startPhase(DoTestPhase.HISTORY_TREE_WRITE_AND_FLUSH);
      TestUtils.executeNonQueries(
          senderEnv,
          isolateDatabasePaths(
              Arrays.asList("insert into root.vehicle.d0(time, s1) values (1, 1)", "flush")),
          null);

      timings.startPhase(DoTestPhase.PIPE_ATTRIBUTES);
      final Map<String, String> sourceAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> sinkAttributes = new HashMap<>();

      sourceAttributes.put("capture.table", "true");
      sourceAttributes.put("__system.sql-dialect", "table");
      sourceAttributes.put("capture.tree", "true");
      sourceAttributes.put("mode.double-living", "true");
      sourceAttributes.put("source.database-name", databasePattern("test.*"));
      sourceAttributes.put("source.table-name", "test.*");
      sourceAttributes.put("user", "root");

      sinkAttributes.put("sink", "iotdb-thrift-sink");
      sinkAttributes.put("sink.ip", receiverIp);
      sinkAttributes.put("sink.port", Integer.toString(receiverPort));
      sinkAttributes.put("sink.format", "tsfile");
      // Retain batching and every flush boundary without waiting 5 seconds per small batch.
      sinkAttributes.put("sink.batch.max-delay-ms", "1000");
      sinkAttributes.put("sink.realtime-first", "true");

      timings.startPhase(DoTestPhase.CREATE_PIPE);
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(),
          client
              .createPipe(
                  new TCreatePipeReq("testPipe", sinkAttributes)
                      .setExtractorAttributes(sourceAttributes)
                      .setProcessorAttributes(processorAttributes))
              .getCode());

      timings.startPhase(DoTestPhase.START_PIPE);
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("testPipe").getCode());

      timings.startPhase(DoTestPhase.REALTIME_TREE_WRITE_AND_FLUSH);
      TestUtils.executeNonQueries(
          senderEnv,
          isolateDatabasePaths(
              Arrays.asList("insert into root.vehicle.d0(time, s1) values (2, 1)", "flush")),
          null);

      timings.startPhase(DoTestPhase.TABLET_WORKLOAD);
      writeTablets(consumer, testResult, test1Result, timings);

      timings.startPhase(DoTestPhase.INITIAL_TREE_CHECK);
      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          isolateDatabasePaths("select * from root.vehicle.**"),
          isolateDatabasePaths("Time,root.vehicle.d0.s1,"),
          Collections.unmodifiableSet(new HashSet<>(Arrays.asList("1,1.0,", "2,1.0,"))),
          handleFailure);

      timings.startPhase(DoTestPhase.OUT_OF_ORDER_TREE_WRITE_AND_FLUSH);
      TestUtils.executeNonQueries(
          senderEnv,
          Arrays.asList(
              isolateDatabasePaths("insert into root.vehicle.d0(time, s1) values (4, 1)"),
              isolateDatabasePaths("insert into root.vehicle.d0(time, s1) values (3, 1), (0, 1)"),
              "flush"),
          null);

      timings.startPhase(DoTestPhase.FINAL_TREE_CHECK);
      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          isolateDatabasePaths("select * from root.vehicle.**"),
          isolateDatabasePaths("Time,root.vehicle.d0.s1,"),
          Collections.unmodifiableSet(
              new HashSet<>(Arrays.asList("0,1.0,", "1,1.0,", "2,1.0,", "3,1.0,", "4,1.0,"))),
          handleFailure);
      timings.startPhase(DoTestPhase.CLOSE_CONFIG_CLIENT);
    }

    for (Map.Entry<String, List<Tablet>> entry : testResult.entrySet()) {
      timings.startPhase(DoTestPhase.EXPECTED_RESULTS);
      final Set<String> set = new HashSet<>();
      entry
          .getValue()
          .forEach(
              tablet -> {
                set.addAll(TableModelUtils.generateExpectedResults(tablet));
              });
      timings.startPhase(DoTestPhase.COUNT_CHECK_WITH_RECEIVER_FLUSH);
      TableModelUtils.assertCountData(
          database("test0"), entry.getKey(), set.size(), receiverEnv, handleFailure);
      timings.startPhase(DoTestPhase.DATA_CHECK);
      TableModelUtils.assertData(
          database("test0"), entry.getKey(), set, receiverEnv, handleFailure);
    }

    for (Map.Entry<String, List<Tablet>> entry : test1Result.entrySet()) {
      timings.startPhase(DoTestPhase.EXPECTED_RESULTS);
      final Set<String> set = new HashSet<>();
      entry
          .getValue()
          .forEach(
              tablet -> {
                set.addAll(TableModelUtils.generateExpectedResults(tablet));
              });
      timings.startPhase(DoTestPhase.COUNT_CHECK_WITH_RECEIVER_FLUSH);
      TableModelUtils.assertCountData(
          database("test1"), entry.getKey(), set.size(), receiverEnv, handleFailure);
      timings.startPhase(DoTestPhase.DATA_CHECK);
      TableModelUtils.assertData(
          database("test1"), entry.getKey(), set, receiverEnv, handleFailure);
    }
  }

  @FunctionalInterface
  private interface TabletWriter {
    void accept(
        Map<String, List<Tablet>> testResult,
        Map<String, List<Tablet>> test1Result,
        DoTestTimings timings,
        ITableSession session)
        throws Exception;
  }

  private final class RetryFlushHandler implements Consumer<String>, AutoCloseable {
    // Index 0 owns the sender Session, index 1 owns the receiver Session. Open lazily because
    // assertions that pass immediately need no retry clients.
    private final BaseEnv[] environments = {senderEnv, receiverEnv};
    private final ISession[] sessions = new ISession[2];
    private final DoTestTimings timings;

    private RetryFlushHandler(final DoTestTimings timings) {
      this.timings = timings;
    }

    @Override
    public void accept(final String ignored) {
      // A failed poll can run every 100 ms. Reuse the flush connections instead of repeatedly
      // opening multi-node JDBC connections and exhausting the client's ephemeral ports.
      for (int i = 0; i < sessions.length; i++) {
        final long started = System.nanoTime();
        try {
          if (sessions[i] == null) {
            sessions[i] = environments[i].getSessionConnection();
          }
          sessions[i].executeNonQueryStatement("flush");
        } catch (Exception e) {
          throw new AssertionError(e);
        } finally {
          if (timings != null) {
            timings.recordDetail(
                i == 0 ? "RETRY_SENDER_FLUSH" : "RETRY_RECEIVER_FLUSH",
                System.nanoTime() - started);
          }
        }
      }
    }

    @Override
    public void close() throws Exception {
      try {
        if (sessions[0] != null) {
          sessions[0].close();
        }
      } finally {
        if (sessions[1] != null) {
          sessions[1].close();
        }
      }
    }
  }

  private enum DoTestPhase {
    OPEN_CONFIG_CLIENT,
    CREATE_SCHEMA,
    HISTORY_TREE_WRITE_AND_FLUSH,
    PIPE_ATTRIBUTES,
    CREATE_PIPE,
    START_PIPE,
    REALTIME_TREE_WRITE_AND_FLUSH,
    TABLET_WORKLOAD,
    INITIAL_TREE_CHECK,
    OUT_OF_ORDER_TREE_WRITE_AND_FLUSH,
    FINAL_TREE_CHECK,
    CLOSE_CONFIG_CLIENT,
    EXPECTED_RESULTS,
    COUNT_CHECK_WITH_RECEIVER_FLUSH,
    DATA_CHECK
  }

  // These diagnostics belong to the test source set, which has no compile-time i18n source sets.
  private static final class TimingMessages {
    public static final String SUMMARY =
        "DataSink doTest timing: test={}, passed={}, total_ms={}, scope=doTest-only; "
            + "STAGE rows partition total time; DETAIL rows are included in STAGE rows; "
            + "checks include JDBC/query/polling/retry time, not isolated Pipe transfer time";
    public static final String ROW =
        "DataSink doTest timing: test={}, kind={}, phase={}, calls={}, total_ms={}, "
            + "avg_ms={}, max_ms={}, percent_of_doTest={}";
    public static final String NUMBER = "%.3f";
  }

  private static final class DoTestTimings implements AutoCloseable {
    private final String method;
    private final long started = System.nanoTime();
    // Keys name client-side stages/details; values accumulate nanoseconds for this doTest only.
    private final Map<String, LongSummaryStatistics> stages = new LinkedHashMap<>();
    private final Map<String, LongSummaryStatistics> details = new LinkedHashMap<>();
    private DoTestPhase phase = DoTestPhase.OPEN_CONFIG_CLIENT;
    private long phaseStarted = started;
    private boolean passed;

    private DoTestTimings(final String method) {
      this.method = method;
    }

    private void startPhase(final DoTestPhase next) {
      final long now = System.nanoTime();
      stages
          .computeIfAbsent(phase.name(), key -> new LongSummaryStatistics())
          .accept(now - phaseStarted);
      phase = next;
      phaseStarted = now;
    }

    private void recordDetail(final String step, final long nanos) {
      details.computeIfAbsent(step, key -> new LongSummaryStatistics()).accept(nanos);
    }

    @Override
    public void close() {
      final long finished = System.nanoTime();
      stages
          .computeIfAbsent(phase.name(), key -> new LongSummaryStatistics())
          .accept(finished - phaseStarted);
      final long total = finished - started;
      IoTDBTestLogger.logger.info(
          TimingMessages.SUMMARY, method, passed, format(total / 1_000_000.0));
      logRows("STAGE", stages, total);
      logRows("DETAIL", details, total);
    }

    private void logRows(
        final String kind, final Map<String, LongSummaryStatistics> rows, final long total) {
      rows.forEach(
          (step, statistics) ->
              IoTDBTestLogger.logger.info(
                  TimingMessages.ROW,
                  method,
                  kind,
                  step,
                  statistics.getCount(),
                  format(statistics.getSum() / 1_000_000.0),
                  format(statistics.getAverage() / 1_000_000.0),
                  format(statistics.getMax() / 1_000_000.0),
                  format(100.0 * statistics.getSum() / total)));
    }

    private static String format(final double value) {
      return String.format(Locale.ROOT, TimingMessages.NUMBER, value);
    }
  }

  private void writeTablets(
      final TabletWriter consumer,
      final Map<String, List<Tablet>> testResult,
      final Map<String, List<Tablet>> test1Result,
      final DoTestTimings timings)
      throws Exception {
    long stepStarted = System.nanoTime();
    boolean opened = false;
    // Own one pool/session for the whole workload, including when writing or an assertion fails.
    try (final ITableSessionPool pool = senderEnv.getTableSessionPool(1);
        final ITableSession session = pool.getSession()) {
      recordTabletTiming(timings, "TABLET_OPEN_SESSION", stepStarted);
      opened = true;
      try {
        consumer.accept(testResult, test1Result, timings, session);
      } finally {
        stepStarted = System.nanoTime();
      }
    } finally {
      recordTabletTiming(
          timings, opened ? "TABLET_CLOSE_SESSION" : "TABLET_OPEN_SESSION", stepStarted);
    }
  }

  private void insertTabletWithTiming(
      final String database,
      final Tablet tablet,
      final DoTestTimings timings,
      final ITableSession session,
      final long generationStarted)
      throws Exception {
    long stepStarted = recordTabletTiming(timings, "TABLET_GENERATION", generationStarted);
    String phase = "TABLET_USE_DATABASE";
    try {
      session.executeNonQueryStatement("use " + this.database(database));
      stepStarted = recordTabletTiming(timings, phase, stepStarted);
      phase = "TABLET_INSERT";
      session.insert(tablet);
      stepStarted = recordTabletTiming(timings, phase, stepStarted);
      phase = "TABLET_FLUSH";
      // Keep a flush after each retained Tablet so the workload still transfers multiple TsFiles.
      session.executeNonQueryStatement("flush");
    } finally {
      recordTabletTiming(timings, phase, stepStarted);
    }
  }

  private static long recordTabletTiming(
      final DoTestTimings timings, final String phase, final long started) {
    final long finished = System.nanoTime();
    if (timings != null) {
      timings.recordDetail(phase, finished - started);
    }
    return finished;
  }

  // Completion assertions wait for asynchronous transfer; per-Tablet sleeps add no coverage.
  // Validate every retained Tablet while reusing the workload's Session for insert and flush.
  private void insertTablet1(
      final Map<String, List<Tablet>> testResult,
      final Map<String, List<Tablet>> test1Result,
      final DoTestTimings timings,
      final ITableSession session)
      throws Exception {

    int deviceIDStartIndex = 0;
    int deviceIDEndIndex = 10;

    for (int j = 0; j < TABLETS_PER_ROUND.length; j++) {
      final String dataBaseName = "test" + j % 2;
      for (int i = 0; i < TABLETS_PER_ROUND[j]; i++) {
        final String tableName = "test" + i;
        final long generationStarted = System.nanoTime();
        Tablet tablet =
            TableModelUtils.generateTablet(
                tableName, deviceIDStartIndex, deviceIDEndIndex, 0, 10, false, false);
        insertTabletWithTiming(dataBaseName, tablet, timings, session, generationStarted);
        Map<String, List<Tablet>> map = j % 2 == 0 ? testResult : test1Result;
        map.computeIfAbsent(tableName, k -> new ArrayList<>()).add(tablet);
      }
      deviceIDStartIndex += 2;
      deviceIDEndIndex += 2;
    }
  }

  private void insertTablet2(
      final Map<String, List<Tablet>> testResult,
      final Map<String, List<Tablet>> test1Result,
      final DoTestTimings timings,
      final ITableSession session)
      throws Exception {
    int deviceIDStartIndex = 0;
    int deviceIDEndIndex = 10;

    for (int j = 0; j < TABLETS_PER_ROUND.length; j++) {
      final String dataBaseName = "test" + j % 2;
      for (int i = 0; i < TABLETS_PER_ROUND[j]; i++) {
        final String tableName = "test" + i;
        final long generationStarted = System.nanoTime();
        Tablet tablet =
            TableModelUtils.generateTablet(
                tableName, deviceIDStartIndex, deviceIDEndIndex, 10, false, false);
        insertTabletWithTiming(dataBaseName, tablet, timings, session, generationStarted);
        Map<String, List<Tablet>> map = j % 2 == 0 ? testResult : test1Result;
        map.computeIfAbsent(tableName, k -> new ArrayList<>()).add(tablet);
      }
      deviceIDStartIndex += 2;
      deviceIDEndIndex += 2;
    }
  }

  private void insertTablet3(
      final Map<String, List<Tablet>> testResult,
      final Map<String, List<Tablet>> test1Result,
      final DoTestTimings timings,
      final ITableSession session)
      throws Exception {
    final Random random = new Random();
    int deviceIDStartIndex = 0;
    int deviceIDEndIndex = 100;

    for (int j = 0; j < TABLETS_PER_ROUND.length; j++) {
      for (int i = 0; i < TABLETS_PER_ROUND[j]; i++) {
        final String tableName = "test" + i;
        final String dataBaseName = "test" + j % 2;
        deviceIDStartIndex = random.nextInt(1 << 16) - 10;
        deviceIDEndIndex = deviceIDStartIndex + 10;
        final long generationStarted = System.nanoTime();
        Tablet tablet =
            TableModelUtils.generateTablet(
                tableName,
                deviceIDStartIndex,
                deviceIDEndIndex,
                deviceIDStartIndex,
                deviceIDEndIndex,
                false,
                true);
        insertTabletWithTiming(dataBaseName, tablet, timings, session, generationStarted);
        Map<String, List<Tablet>> map = j % 2 == 0 ? testResult : test1Result;
        map.computeIfAbsent(tableName, k -> new ArrayList<>()).add(tablet);
      }
    }
  }

  private void insertTablet4(
      final Map<String, List<Tablet>> testResult,
      final Map<String, List<Tablet>> test1Result,
      final DoTestTimings timings,
      final ITableSession session)
      throws Exception {
    final Random random = new Random();
    int deviceIDStartIndex = 0;
    int deviceIDEndIndex = 100;

    for (int j = 0; j < TABLETS_PER_ROUND.length; j++) {
      final String dataBaseName = "test" + j % 2;
      for (int i = 0; i < TABLETS_PER_ROUND[j]; i++) {
        final String tableName = "test" + i;
        deviceIDStartIndex = random.nextInt(1 << 16) - 10;
        deviceIDEndIndex = deviceIDStartIndex + 10;
        final long generationStarted = System.nanoTime();
        Tablet tablet =
            TableModelUtils.generateTablet(
                tableName,
                deviceIDStartIndex,
                deviceIDEndIndex,
                deviceIDStartIndex,
                deviceIDEndIndex,
                false,
                false);
        insertTabletWithTiming(dataBaseName, tablet, timings, session, generationStarted);
        Map<String, List<Tablet>> map = j % 2 == 0 ? testResult : test1Result;
        map.computeIfAbsent(tableName, k -> new ArrayList<>()).add(tablet);
      }
    }
  }

  private void insertTablet5(
      final Map<String, List<Tablet>> testResult,
      final Map<String, List<Tablet>> test1Result,
      final DoTestTimings timings,
      final ITableSession session)
      throws Exception {
    final Random random = new Random();
    int deviceIDStartIndex = 0;
    int deviceIDEndIndex = 100;
    for (int j = 0; j < TABLETS_PER_ROUND.length; j++) {
      for (int i = 0; i < TABLETS_PER_ROUND[j]; i++) {
        final String tableName = "test" + i;
        final String dataBaseName = "test" + j % 2;
        deviceIDStartIndex = random.nextInt(1 << 16) - 10;
        deviceIDEndIndex = deviceIDStartIndex + 10;
        final long generationStarted = System.nanoTime();
        Tablet tablet =
            TableModelUtils.generateTablet(
                tableName, 0, 10, deviceIDStartIndex, deviceIDEndIndex, false, true);
        insertTabletWithTiming(dataBaseName, tablet, timings, session, generationStarted);
        Map<String, List<Tablet>> map = j % 2 == 0 ? testResult : test1Result;
        map.computeIfAbsent(tableName, k -> new ArrayList<>()).add(tablet);
      }
    }
  }

  private void insertTablet6(
      final Map<String, List<Tablet>> testResult,
      final Map<String, List<Tablet>> test1Result,
      final DoTestTimings timings,
      final ITableSession session)
      throws Exception {
    final Random random = new Random();
    int deviceIDStartIndex = 0;
    int deviceIDEndIndex = 100;

    for (int j = 0; j < TABLETS_PER_ROUND.length; j++) {
      final String dataBaseName = "test" + j % 2;
      for (int i = 0; i < TABLETS_PER_ROUND[j]; i++) {
        final String tableName = "test" + i;
        deviceIDStartIndex = random.nextInt(1 << 16) - 10;
        deviceIDEndIndex = deviceIDStartIndex + 10;
        final long generationStarted = System.nanoTime();
        Tablet tablet =
            TableModelUtils.generateTablet(
                tableName, deviceIDStartIndex, deviceIDEndIndex, 100, 110, false, true);
        insertTabletWithTiming(dataBaseName, tablet, timings, session, generationStarted);
        Map<String, List<Tablet>> map = j % 2 == 0 ? testResult : test1Result;
        map.computeIfAbsent(tableName, k -> new ArrayList<>()).add(tablet);
      }
    }
  }

  private void insertTablet7(
      final Map<String, List<Tablet>> testResult,
      final Map<String, List<Tablet>> test1Result,
      final DoTestTimings timings,
      final ITableSession session)
      throws Exception {

    final Random random = new Random();
    int deviceIDStartIndex = 0;
    int deviceIDEndIndex = 100;
    deviceIDStartIndex = random.nextInt(1 << 16) - 10;
    deviceIDEndIndex = deviceIDStartIndex + 10;
    for (int j = 0; j < TABLETS_PER_ROUND.length; j++) {
      final String dataBaseName = "test" + j % 2;
      for (int i = 0; i < TABLETS_PER_ROUND[j]; i++) {
        final String tableName = "test" + i;
        final long generationStarted = System.nanoTime();
        Tablet tablet =
            TableModelUtils.generateTablet(
                tableName, deviceIDStartIndex, deviceIDEndIndex, 100, 110, false, true);
        insertTabletWithTiming(dataBaseName, tablet, timings, session, generationStarted);
        Map<String, List<Tablet>> map = j % 2 == 0 ? testResult : test1Result;
        map.computeIfAbsent(tableName, k -> new ArrayList<>()).add(tablet);
      }
      deviceIDStartIndex += 2;
      deviceIDEndIndex += 2;
    }
  }

  private void insertTablet8(
      final Map<String, List<Tablet>> testResult,
      final Map<String, List<Tablet>> test1Result,
      final DoTestTimings timings,
      final ITableSession session)
      throws Exception {
    final Random random = new Random();
    int deviceIDStartIndex = random.nextInt(1 << 16);
    int deviceIDEndIndex = deviceIDStartIndex + 10;
    for (int j = 0; j < TABLETS_PER_ROUND.length; j++) {
      final String dataBaseName = "test" + j % 2;
      for (int i = 0; i < TABLETS_PER_ROUND[j]; i++) {
        final String tableName = "test" + i;
        final long generationStarted = System.nanoTime();
        Tablet tablet =
            TableModelUtils.generateTablet(
                tableName, 100, 110, deviceIDStartIndex, deviceIDEndIndex, false, true);
        insertTabletWithTiming(dataBaseName, tablet, timings, session, generationStarted);
        Map<String, List<Tablet>> map = j % 2 == 0 ? testResult : test1Result;
        map.computeIfAbsent(tableName, k -> new ArrayList<>()).add(tablet);
      }
      deviceIDStartIndex += 2;
      deviceIDEndIndex += 2;
    }
  }

  private void insertTablet9(
      final Map<String, List<Tablet>> testResult,
      final Map<String, List<Tablet>> test1Result,
      final DoTestTimings timings,
      final ITableSession session)
      throws Exception {
    final Random random = new Random();
    for (int j = 0; j < TABLETS_PER_ROUND.length; j++) {
      final String dataBaseName = "test" + j % 2;
      for (int i = 0; i < TABLETS_PER_ROUND[j]; i++) {
        final String tableName = "test" + i;
        final long generationStarted = System.nanoTime();
        Tablet tablet =
            TableModelUtils.generateTabletDeviceIDAllIsNull(tableName, 100, 110, 10, false);
        insertTabletWithTiming(dataBaseName, tablet, timings, session, generationStarted);
        Map<String, List<Tablet>> map = j % 2 == 0 ? testResult : test1Result;
        map.computeIfAbsent(tableName, k -> new ArrayList<>()).add(tablet);
      }
    }
  }

  // Disabling receiver TsFile validation must still preserve the tree points and all generated
  // table values.
  @Test
  public void testLoadTsFileWithoutVerify() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {

      // Do not fail if the failure has nothing to do with pipe
      // Because the failures will randomly generate due to resource limitation
      TestUtils.executeNonQueries(
          senderEnv,
          isolateDatabasePaths(
              Arrays.asList("insert into root.vehicle.d0(time, s1) values (1, 1)", "flush")),
          null);

      for (int i = 0; i < 5; i++) {
        TableModelUtils.createDataBaseAndTable(senderEnv, "test" + i, database("test0"));
        TableModelUtils.createDataBaseAndTable(senderEnv, "test" + i, database("test1"));
      }

      final Map<String, String> sourceAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> sinkAttributes = new HashMap<>();

      sourceAttributes.put("source.realtime.mode", "batch");
      sourceAttributes.put("capture.table", "true");
      sourceAttributes.put("__system.sql-dialect", "table");
      sourceAttributes.put("capture.tree", "true");
      sourceAttributes.put("mode.double-living", "true");
      sourceAttributes.put("user", "root");

      sinkAttributes.put("sink", "iotdb-thrift-sink");
      sinkAttributes.put("sink.batch.enable", "false");
      sinkAttributes.put("sink.ip", receiverIp);
      sinkAttributes.put("sink.port", Integer.toString(receiverPort));
      sinkAttributes.put("sink.tsfile.validation", "false");

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(),
          client
              .createPipe(
                  new TCreatePipeReq("testPipe", sinkAttributes)
                      .setExtractorAttributes(sourceAttributes)
                      .setProcessorAttributes(processorAttributes))
              .getCode());

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("testPipe").getCode());

      // Do not fail if the failure has nothing to do with pipe
      // Because the failures will randomly generate due to resource limitation
      TestUtils.executeNonQueries(
          senderEnv,
          isolateDatabasePaths(
              Arrays.asList("insert into root.vehicle.d0(time, s1) values (2, 1)", "flush")),
          null);

      Map<String, List<Tablet>> testResult = new HashMap<>();
      Map<String, List<Tablet>> test1Result = new HashMap<>();

      writeTablets(this::insertTablet1, testResult, test1Result, null);

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          isolateDatabasePaths("select * from root.vehicle.**"),
          isolateDatabasePaths("Time,root.vehicle.d0.s1,"),
          Collections.unmodifiableSet(new HashSet<>(Arrays.asList("1,1.0,", "2,1.0,"))));

      for (Map.Entry<String, List<Tablet>> entry : testResult.entrySet()) {
        final Set<String> set = new HashSet<>();
        entry
            .getValue()
            .forEach(tablet -> set.addAll(TableModelUtils.generateExpectedResults(tablet)));
        TableModelUtils.assertCountData(
            database("test0"), entry.getKey(), set.size(), receiverEnv, s -> {});
        TableModelUtils.assertData(database("test0"), entry.getKey(), set, receiverEnv, s -> {});
      }

      for (Map.Entry<String, List<Tablet>> entry : test1Result.entrySet()) {
        final Set<String> set = new HashSet<>();
        entry
            .getValue()
            .forEach(tablet -> set.addAll(TableModelUtils.generateExpectedResults(tablet)));
        TableModelUtils.assertCountData(
            database("test1"), entry.getKey(), set.size(), receiverEnv, s -> {});
        TableModelUtils.assertData(database("test1"), entry.getKey(), set, receiverEnv, s -> {});
      }
    }
  }

  // Shared-cluster cleanup is asynchronous, so suffix each workload's databases with its method.
  private String database(final String name) {
    return name + "_" + testName.getMethodName().toLowerCase(Locale.ENGLISH);
  }

  private String databasePattern(final String pattern) {
    return pattern + "_" + testName.getMethodName().toLowerCase(Locale.ENGLISH);
  }

  private String isolateDatabasePaths(final String value) {
    return value.replace("root.vehicle", "root.vehicle_" + testName.getMethodName());
  }

  private List<String> isolateDatabasePaths(final List<String> values) {
    final List<String> isolatedValues = new ArrayList<>(values.size());
    for (final String value : values) {
      isolatedValues.add(isolateDatabasePaths(value));
    }
    return isolatedValues;
  }
}
