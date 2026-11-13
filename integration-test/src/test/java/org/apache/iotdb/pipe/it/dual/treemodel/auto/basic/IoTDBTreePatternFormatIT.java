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

package org.apache.iotdb.pipe.it.dual.treemodel.auto.basic;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.confignode.rpc.thrift.TCreatePipeReq;
import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT2DualTreeAutoBasic;
import org.apache.iotdb.pipe.it.dual.treemodel.auto.AbstractPipeTreeModelSharedClusterIT;
import org.apache.iotdb.rpc.TSStatusCode;

import org.awaitility.Awaitility;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2DualTreeAutoBasic.class})
public class IoTDBTreePatternFormatIT extends AbstractPipeTreeModelSharedClusterIT {

  @Rule public final TestName testName = new TestName();

  // A legacy prefix root.db.d1.s includes both s and s1, excluding other devices and databases.
  @Test
  public void testPrefixPattern() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {

      TestUtils.executeNonQueries(
          senderEnv,
          Arrays.asList(
              isolateDatabasePaths("insert into root.db.d1(time, s, s1, t) values (1, 1, 1, 1)"),
              isolateDatabasePaths("insert into root.db.d2(time, s) values (1, 1)"),
              isolateDatabasePaths("insert into root.db2.d1(time, s) values (1, 1)")),
          null);
      flushAndAwaitTsFile();

      final Map<String, String> sourceAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> sinkAttributes = new HashMap<>();

      sourceAttributes.put("source.pattern", isolateDatabasePaths("root.db.d1.s"));
      sourceAttributes.put("source.inclusion", "data.insert");
      sourceAttributes.put("user", "root");

      sinkAttributes.put("sink", "iotdb-thrift-sink");
      sinkAttributes.put("sink.batch.enable", "false");
      sinkAttributes.put("sink.ip", receiverIp);
      sinkAttributes.put("sink.port", Integer.toString(receiverPort));

      final TSStatus status =
          client.createPipe(
              new TCreatePipeReq("p1", sinkAttributes)
                  .setExtractorAttributes(sourceAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("p1").getCode());

      final Set<String> expectedResSet = new HashSet<>();
      expectedResSet.add("1,1.0,1.0,");
      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          isolateDatabasePaths("select * from root.db.**"),
          isolateDatabasePaths("Time,root.db.d1.s,root.db.d1.s1,"),
          expectedResSet);
    }
  }

  // The path root.**.d1.s* selects s and s1 in both databases, excluding d2 and t.
  @Test
  public void testIoTDBPattern() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {

      TestUtils.executeNonQueries(
          senderEnv,
          Arrays.asList(
              isolateDatabasePaths("insert into root.db.d1(time, s, s1, t) values (1, 1, 1, 1)"),
              isolateDatabasePaths("insert into root.db.d2(time, s) values (1, 1)"),
              isolateDatabasePaths("insert into root.db2.d1(time, s) values (1, 1)")),
          null);
      flushAndAwaitTsFile();

      final Map<String, String> sourceAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> sinkAttributes = new HashMap<>();

      sourceAttributes.put("source.path", "root.**.d1.s*");
      sourceAttributes.put("source.inclusion", "data.insert");
      sourceAttributes.put("user", "root");

      sinkAttributes.put("sink", "iotdb-thrift-sink");
      sinkAttributes.put("sink.batch.enable", "false");
      sinkAttributes.put("sink.ip", receiverIp);
      sinkAttributes.put("sink.port", Integer.toString(receiverPort));

      final TSStatus status =
          client.createPipe(
              new TCreatePipeReq("p1", sinkAttributes)
                  .setExtractorAttributes(sourceAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("p1").getCode());

      final Set<String> expectedResSet = new HashSet<>();
      expectedResSet.add("1,1.0,1.0,1.0,");
      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          isolateDatabasePaths("select * from root.db2.**,root.db.**"),
          isolateDatabasePaths("Time,root.db2.d1.s,root.db.d1.s,root.db.d1.s1,"),
          expectedResSet);
    }
  }

  // Legacy pattern + iotdb format must select the same d1.s* historical data as the path option.
  @Test
  public void testIoTDBPatternWithLegacySyntax() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {

      TestUtils.executeNonQueries(
          senderEnv,
          Arrays.asList(
              isolateDatabasePaths("insert into root.db.d1(time, s, s1, t) values (1, 1, 1, 1)"),
              isolateDatabasePaths("insert into root.db.d2(time, s) values (1, 1)"),
              isolateDatabasePaths("insert into root.db2.d1(time, s) values (1, 1)")),
          null);
      flushAndAwaitTsFile();

      final Map<String, String> sourceAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> sinkAttributes = new HashMap<>();

      sourceAttributes.put("source.pattern", "root.**.d1.s*");
      sourceAttributes.put("source.pattern.format", "iotdb");
      sourceAttributes.put("source.inclusion", "data.insert");
      sourceAttributes.put("user", "root");

      sinkAttributes.put("sink", "iotdb-thrift-sink");
      sinkAttributes.put("sink.batch.enable", "false");
      sinkAttributes.put("sink.ip", receiverIp);
      sinkAttributes.put("sink.port", Integer.toString(receiverPort));

      final TSStatus status =
          client.createPipe(
              new TCreatePipeReq("p1", sinkAttributes)
                  .setExtractorAttributes(sourceAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("p1").getCode());

      final Set<String> expectedResSet = new HashSet<>();
      expectedResSet.add("1,1.0,1.0,1.0,");
      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          isolateDatabasePaths("select * from root.db2.**,root.db.**"),
          isolateDatabasePaths("Time,root.db2.d1.s,root.db.d1.s,root.db.d1.s1,"),
          expectedResSet);
    }
  }

  //////////////////////////// Multiple & Exclusion ////////////////////////////

  private void flushAndAwaitTsFile() {
    // Shared clusters retain parent directories after database deletion. Check an actual TsFile
    // after FLUSH, without imposing the old two-second delay on every history/realtime scenario.
    Awaitility.await()
        .pollDelay(Duration.ZERO)
        .pollInterval(Duration.ofMillis(100))
        .atMost(Duration.ofMinutes(1))
        .until(
            () -> {
              TestUtils.executeNonQuery(senderEnv, "flush", null);
              for (final DataNodeWrapper node : senderEnv.getDataNodeWrapperList()) {
                final Path dataDirectory =
                    Path.of(
                        node.getNodePath(),
                        IoTDBConstant.DATA_FOLDER_NAME,
                        "datanode",
                        IoTDBConstant.DATA_FOLDER_NAME);
                if (Files.isDirectory(dataDirectory)) {
                  try (final Stream<Path> files = Files.walk(dataDirectory)) {
                    if (files.anyMatch(
                        file ->
                            Files.isRegularFile(file)
                                && file.getFileName().toString().endsWith(".tsfile"))) {
                      return true;
                    }
                  }
                }
              }
              return false;
            });
  }

  private void testPipeWithMultiplePatterns(
      final Map<String, String> sourceAttributes,
      final List<String> insertQueries,
      final boolean isHistorical,
      final String validationSelectQuery,
      final String validationSelectHeader,
      final Set<String> expectedResultSet)
      throws Exception {

    // 1. Get receiver connection details
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);
    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {

      // 2. Define standard processor and connector attributes
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> connectorAttributes = new HashMap<>();
      connectorAttributes.put("connector", "iotdb-thrift-connector");
      connectorAttributes.put("connector.batch.enable", "false");
      connectorAttributes.put("connector.ip", receiverIp);
      connectorAttributes.put("connector.port", Integer.toString(receiverPort));

      // 3. Handle historical data insertion (if applicable)
      if (isHistorical) {
        TestUtils.executeNonQueries(senderEnv, insertQueries, null);
        flushAndAwaitTsFile();
      }

      // 4. Create the pipe
      final TSStatus createStatus =
          client.createPipe(
              new TCreatePipeReq("p1", connectorAttributes)
                  .setExtractorAttributes(sourceAttributes)
                  .setProcessorAttributes(processorAttributes));
      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), createStatus.getCode());

      // 5. Start the pipe
      final TSStatus startStatus = client.startPipe("p1");
      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), startStatus.getCode());

      // 6. Handle realtime data insertion (if applicable)
      if (!isHistorical) {
        TestUtils.executeNonQueries(senderEnv, insertQueries, null);
        flushAndAwaitTsFile();
      }

      // 7. Validate data eventually arrives on the receiver
      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv, validationSelectQuery, validationSelectHeader, expectedResultSet);
    }
  }

  // Historical data matching either inclusion pattern must sync while root.db3 stays excluded.
  @Test
  public void testMultipleIoTDBPatternHistoricalData() throws Exception {
    final Map<String, String> sourceAttributes = new HashMap<>();
    sourceAttributes.put(
        "source.pattern.inclusion", isolateDatabasePaths("root.db.**, root.db2.d1.*"));
    sourceAttributes.put("source.inclusion", "data.insert");
    sourceAttributes.put("user", "root");

    final List<String> insertQueries =
        Arrays.asList(
            isolateDatabasePaths("insert into root.db.d1(time, s, s1) values (1, 1, 1)"),
            isolateDatabasePaths("insert into root.db.d2(time, s) values (2, 2)"),
            isolateDatabasePaths("insert into root.db2.d1(time, s, t) values (3, 3, 3)"),
            isolateDatabasePaths("insert into root.db3.d1(time, s) values (4, 4)"));

    final Set<String> expectedResSet = new HashSet<>();
    expectedResSet.add("1,null,null,1.0,1.0,null,");
    expectedResSet.add("2,null,null,null,null,2.0,");
    expectedResSet.add("3,3.0,3.0,null,null,null,");

    testPipeWithMultiplePatterns(
        sourceAttributes,
        insertQueries,
        true, // isHistorical = true
        isolateDatabasePaths("select * from root.db2.**,root.db.**"),
        isolateDatabasePaths(
            "Time,root.db2.d1.s,root.db2.d1.t,root.db.d1.s,root.db.d1.s1,root.db.d2.s,"),
        expectedResSet);
  }

  // Realtime data matching either inclusion pattern must sync while root.db3 stays excluded.
  @Test
  public void testMultipleIoTDBPatternRealtimeData() throws Exception {
    final Map<String, String> sourceAttributes = new HashMap<>();
    sourceAttributes.put(
        "source.pattern.inclusion", isolateDatabasePaths("root.db.**, root.db2.d1.*"));
    sourceAttributes.put("source.inclusion", "data.insert");
    sourceAttributes.put("user", "root");

    final List<String> insertQueries =
        Arrays.asList(
            isolateDatabasePaths("insert into root.db.d1(time, s, s1) values (1, 1, 1)"),
            isolateDatabasePaths("insert into root.db.d2(time, s) values (2, 2)"),
            isolateDatabasePaths("insert into root.db2.d1(time, s, t) values (3, 3, 3)"),
            isolateDatabasePaths("insert into root.db3.d1(time, s) values (4, 4)"));

    final Set<String> expectedResSet = new HashSet<>();
    expectedResSet.add("1,null,null,1.0,1.0,null,");
    expectedResSet.add("2,null,null,null,null,2.0,");
    expectedResSet.add("3,3.0,3.0,null,null,null,");

    testPipeWithMultiplePatterns(
        sourceAttributes,
        insertQueries,
        false, // isHistorical = false
        isolateDatabasePaths("select * from root.db2.**,root.db.**"),
        isolateDatabasePaths(
            "Time,root.db2.d1.s,root.db2.d1.t,root.db.d1.s,root.db.d1.s1,root.db.d2.s,"),
        expectedResSet);
  }

  // Historical inclusion root.db.** must exclude s* under d1 and every measurement under d3.
  @Test
  public void testIoTDBPatternWithExclusionHistoricalData() throws Exception {
    final Map<String, String> sourceAttributes = new HashMap<>();
    // Inclusion: Match everything under root.db
    sourceAttributes.put("source.pattern.inclusion", isolateDatabasePaths("root.db.**"));
    // Exclusion: Exclude root.db.d1.s* and root.db.d3.*
    sourceAttributes.put(
        "source.pattern.exclusion", isolateDatabasePaths("root.db.d1.s*, root.db.d3.*"));
    sourceAttributes.put("source.inclusion", "data.insert");
    sourceAttributes.put("user", "root");

    final List<String> insertQueries =
        Arrays.asList(
            // s, s1 excluded, t matches
            isolateDatabasePaths("insert into root.db.d1(time, s, s1, t) values (1, 1, 1, 1)"),
            // s matches
            isolateDatabasePaths("insert into root.db.d2(time, s) values (2, 2)"),
            // s excluded
            isolateDatabasePaths("insert into root.db.d3(time, s) values (3, 3)"),
            isolateDatabasePaths("insert into root.db1.d1(time, s) values (4, 4)"));

    final Set<String> expectedResSet = new HashSet<>();
    expectedResSet.add("1,1.0,null,");
    expectedResSet.add("2,null,2.0,");

    testPipeWithMultiplePatterns(
        sourceAttributes,
        insertQueries,
        true, // isHistorical = true
        isolateDatabasePaths("select * from root.db.**"),
        isolateDatabasePaths("Time,root.db.d1.t,root.db.d2.s,"),
        expectedResSet);
  }

  // Realtime inclusion root.db.** must exclude s* under d1 and every measurement under d3.
  @Test
  public void testIoTDBPatternWithExclusionRealtimeData() throws Exception {
    final Map<String, String> sourceAttributes = new HashMap<>();
    sourceAttributes.put("source.pattern.inclusion", isolateDatabasePaths("root.db.**"));
    sourceAttributes.put(
        "source.pattern.exclusion", isolateDatabasePaths("root.db.d1.s*, root.db.d3.*"));
    sourceAttributes.put("source.inclusion", "data.insert");
    sourceAttributes.put("user", "root");

    final List<String> insertQueries =
        Arrays.asList(
            isolateDatabasePaths("insert into root.db.d1(time, s, s1, t) values (1, 1, 1, 1)"),
            isolateDatabasePaths("insert into root.db.d2(time, s) values (2, 2)"),
            isolateDatabasePaths("insert into root.db.d3(time, s) values (3, 3)"),
            isolateDatabasePaths("insert into root.db1.d1(time, s) values (4, 4)"));

    final Set<String> expectedResSet = new HashSet<>();
    expectedResSet.add("1,1.0,null,");
    expectedResSet.add("2,null,2.0,");

    testPipeWithMultiplePatterns(
        sourceAttributes,
        insertQueries,
        false, // isHistorical = false
        isolateDatabasePaths("select * from root.db.**"),
        isolateDatabasePaths("Time,root.db.d1.t,root.db.d2.s,"),
        expectedResSet);
  }

  // With only historical exclusions, unmatched databases still sync while d1.s* and d3 stay out.
  @Test
  public void testIoTDBPatternWithExclusionOnlyHistoricalData() throws Exception {
    final Map<String, String> sourceAttributes = new HashMap<>();
    sourceAttributes.put(
        "source.pattern.exclusion", isolateDatabasePaths("root.db.d1.s*, root.db.d3.*"));
    sourceAttributes.put("source.inclusion", "data.insert");
    sourceAttributes.put("user", "root");

    final List<String> insertQueries =
        Arrays.asList(
            isolateDatabasePaths("insert into root.db.d1(time, s, s1, t) values (1, 1, 1, 1)"),
            isolateDatabasePaths("insert into root.db.d2(time, s) values (2, 2)"),
            isolateDatabasePaths("insert into root.db.d3(time, s) values (3, 3)"),
            isolateDatabasePaths("insert into root.db1.d1(time, s) values (4, 4)"));

    final Set<String> expectedResSet = new HashSet<>();
    expectedResSet.add("1,null,1.0,null,");
    expectedResSet.add("2,null,null,2.0,");
    expectedResSet.add("4,4.0,null,null,");

    testPipeWithMultiplePatterns(
        sourceAttributes,
        insertQueries,
        true, // isHistorical = true
        isolateDatabasePaths("select * from root.db1.**,root.db.**"),
        isolateDatabasePaths("Time,root.db1.d1.s,root.db.d1.t,root.db.d2.s,"),
        expectedResSet);
  }

  // With only realtime exclusions, unmatched databases still sync while d1.s* and d3 stay out.
  @Test
  public void testIoTDBPatternWithExclusionOnlyRealtimeData() throws Exception {
    final Map<String, String> sourceAttributes = new HashMap<>();
    sourceAttributes.put(
        "source.pattern.exclusion", isolateDatabasePaths("root.db.d1.s*, root.db.d3.*"));
    sourceAttributes.put("source.inclusion", "data.insert");
    sourceAttributes.put("user", "root");

    final List<String> insertQueries =
        Arrays.asList(
            isolateDatabasePaths("insert into root.db.d1(time, s, s1, t) values (1, 1, 1, 1)"),
            isolateDatabasePaths("insert into root.db.d2(time, s) values (2, 2)"),
            isolateDatabasePaths("insert into root.db.d3(time, s) values (3, 3)"),
            isolateDatabasePaths("insert into root.db1.d1(time, s) values (4, 4)"));

    final Set<String> expectedResSet = new HashSet<>();
    expectedResSet.add("1,null,1.0,null,");
    expectedResSet.add("2,null,null,2.0,");
    expectedResSet.add("4,4.0,null,null,");

    testPipeWithMultiplePatterns(
        sourceAttributes,
        insertQueries,
        false, // isHistorical = false
        isolateDatabasePaths("select * from root.db1.**,root.db.**"),
        isolateDatabasePaths("Time,root.db1.d1.s,root.db.d1.t,root.db.d2.s,"),
        expectedResSet);
  }

  // Historical exclusion of d_20 must take precedence over both wildcard and exact inclusions.
  @Test
  public void testIoTDBPatternWithInclusionAndExclusionHistoricalData() throws Exception {
    final Map<String, String> sourceAttributes = new HashMap<>();
    sourceAttributes.put(
        "source.pattern.inclusion",
        isolateDatabasePaths("root.test.g_0.d_2*.**,root.test.g_0.d_20.s_0"));
    sourceAttributes.put("source.pattern.exclusion", isolateDatabasePaths("root.test.g_0.d_20.**"));
    sourceAttributes.put("source.inclusion", "data.insert");
    sourceAttributes.put("user", "root");

    final List<String> insertQueries =
        Arrays.asList(
            isolateDatabasePaths("insert into root.test.g_0.d_21(time, s_1) values (1, 1)"),
            isolateDatabasePaths("insert into root.test.g_0.d_22(time, s_1) values (2, 2)"),
            isolateDatabasePaths("insert into root.test.g_0.d_20(time, s_0, s_1) values (3, 3, 3)"),
            isolateDatabasePaths("insert into root.test.g_0.d_30(time, s_1) values (4, 4)"));

    final Set<String> expectedResSet = new HashSet<>();
    expectedResSet.add("1,1.0,null,");
    expectedResSet.add("2,null,2.0,");

    testPipeWithMultiplePatterns(
        sourceAttributes,
        insertQueries,
        true, // isHistorical = true
        isolateDatabasePaths("select * from root.test.**"),
        isolateDatabasePaths("Time,root.test.g_0.d_21.s_1,root.test.g_0.d_22.s_1,"),
        expectedResSet);
  }

  // Realtime exclusion of d_20 must take precedence over both wildcard and exact inclusions.
  @Test
  public void testIoTDBPatternWithInclusionAndExclusionRealtimeData() throws Exception {
    final Map<String, String> sourceAttributes = new HashMap<>();
    sourceAttributes.put(
        "source.pattern.inclusion",
        isolateDatabasePaths("root.test.g_0.d_2*.**,root.test.g_0.d_20.s_0"));
    sourceAttributes.put("source.pattern.exclusion", isolateDatabasePaths("root.test.g_0.d_20.**"));
    sourceAttributes.put("source.inclusion", "data.insert");
    sourceAttributes.put("user", "root");

    final List<String> insertQueries =
        Arrays.asList(
            isolateDatabasePaths("insert into root.test.g_0.d_21(time, s_1) values (1, 1)"),
            isolateDatabasePaths("insert into root.test.g_0.d_22(time, s_1) values (2, 2)"),
            isolateDatabasePaths("insert into root.test.g_0.d_20(time, s_0, s_1) values (3, 3, 3)"),
            isolateDatabasePaths("insert into root.test.g_0.d_30(time, s_1) values (4, 4)"));

    final Set<String> expectedResSet = new HashSet<>();
    expectedResSet.add("1,1.0,null,");
    expectedResSet.add("2,null,2.0,");

    testPipeWithMultiplePatterns(
        sourceAttributes,
        insertQueries,
        false, // isHistorical = false
        isolateDatabasePaths("select * from root.test.**"),
        isolateDatabasePaths("Time,root.test.g_0.d_21.s_1,root.test.g_0.d_22.s_1,"),
        expectedResSet);
  }

  private String isolateDatabasePaths(final String value) {
    // Suffix the database node only, retaining wildcards and the devices/measurements under test.
    return value.replaceAll("root\\.(db[0-9]*|test)(?=[.,]|$)", "$0_" + databaseSuffix());
  }

  private String databaseSuffix() {
    // Stay below the receiver's 64-character database path limit, including root and the prefix.
    // Retain a hash when shortening names so methods with the same prefix use different databases.
    final String method = testName.getMethodName().toLowerCase(Locale.ROOT);
    return method.length() <= 40
        ? method
        : method.substring(0, 31) + "_" + Integer.toUnsignedString(method.hashCode(), 16);
  }
}
