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
import org.apache.iotdb.confignode.rpc.thrift.TCreatePipeReq;
import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT2DualTreeAutoBasic;
import org.apache.iotdb.pipe.it.dual.treemodel.auto.AbstractPipeDualTreeModelAutoIT;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2DualTreeAutoBasic.class})
public class IoTDBTreePatternFormatIT extends AbstractPipeDualTreeModelAutoIT {

  @Override
  @Before
  public void setUp() {
    super.setUp();
  }

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
              "insert into root.db.d1(time, s, s1, t) values (1, 1, 1, 1)",
              "insert into root.db.d2(time, s) values (1, 1)",
              "insert into root.db2.d1(time, s) values (1, 1)"),
          null);
      awaitUntilFlush(senderEnv);

      final Map<String, String> sourceAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> sinkAttributes = new HashMap<>();

      sourceAttributes.put("source.pattern", "root.db.d1.s");
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
          "select * from root.db.**",
          "Time,root.db.d1.s,root.db.d1.s1,",
          expectedResSet);
    }
  }

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
              "insert into root.db.d1(time, s, s1, t) values (1, 1, 1, 1)",
              "insert into root.db.d2(time, s) values (1, 1)",
              "insert into root.db2.d1(time, s) values (1, 1)"),
          null);
      awaitUntilFlush(senderEnv);

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
          "select * from root.db2.**,root.db.**",
          "Time,root.db2.d1.s,root.db.d1.s,root.db.d1.s1,",
          expectedResSet);
    }
  }

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
              "insert into root.db.d1(time, s, s1, t) values (1, 1, 1, 1)",
              "insert into root.db.d2(time, s) values (1, 1)",
              "insert into root.db2.d1(time, s) values (1, 1)"),
          null);
      awaitUntilFlush(senderEnv);

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
          "select * from root.db2.**,root.db.**",
          "Time,root.db2.d1.s,root.db.d1.s,root.db.d1.s1,",
          expectedResSet);
    }
  }

  //////////////////////////// Multiple & Exclusion ////////////////////////////

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
        awaitUntilFlush(senderEnv);
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
        awaitUntilFlush(senderEnv);
      }

      // 7. Validate data eventually arrives on the receiver
      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv, validationSelectQuery, validationSelectHeader, expectedResultSet);
    }
  }

  @Test
  public void testMultiplePrefixPatternHistoricalData() throws Exception {
    // Define source attributes
    final Map<String, String> sourceAttributes = new HashMap<>();
    sourceAttributes.put("source.pattern", "root.db.d1.s, root.db2.d1.s");
    sourceAttributes.put("source.inclusion", "data.insert");
    sourceAttributes.put("user", "root");

    // Define data to be inserted
    final List<String> insertQueries =
        Arrays.asList(
            "insert into root.db.d1(time, s, s1) values (1, 1, 1)",
            "insert into root.db.d2(time, s) values (2, 2)",
            "insert into root.db2.d1(time, s) values (3, 3)");

    // Define expected results on receiver
    final Set<String> expectedResSet = new HashSet<>();
    expectedResSet.add("1,null,1.0,1.0,");
    expectedResSet.add("3,3.0,null,null,");

    // Execute the common test logic
    testPipeWithMultiplePatterns(
        sourceAttributes,
        insertQueries,
        true, // isHistorical = true
        "select * from root.db2.**,root.db.**",
        "Time,root.db2.d1.s,root.db.d1.s,root.db.d1.s1,",
        expectedResSet);
  }

  @Test
  public void testMultiplePrefixPatternRealtimeData() throws Exception {
    final Map<String, String> sourceAttributes = new HashMap<>();
    sourceAttributes.put("source.pattern", "root.db.d1.s, root.db2.d1.s");
    sourceAttributes.put("source.inclusion", "data.insert");
    sourceAttributes.put("user", "root");

    final List<String> insertQueries =
        Arrays.asList(
            "insert into root.db.d1(time, s, s1) values (1, 1, 1)",
            "insert into root.db.d2(time, s) values (2, 2)",
            "insert into root.db2.d1(time, s) values (3, 3)");

    final Set<String> expectedResSet = new HashSet<>();
    expectedResSet.add("1,null,1.0,1.0,");
    expectedResSet.add("3,3.0,null,null,");

    testPipeWithMultiplePatterns(
        sourceAttributes,
        insertQueries,
        false, // isHistorical = false
        "select * from root.db2.**,root.db.**",
        "Time,root.db2.d1.s,root.db.d1.s,root.db.d1.s1,",
        expectedResSet);
  }

  @Test
  public void testMultipleIoTDBPatternHistoricalData() throws Exception {
    final Map<String, String> sourceAttributes = new HashMap<>();
    sourceAttributes.put("source.path", "root.db.**, root.db2.d1.*");
    sourceAttributes.put("source.inclusion", "data.insert");
    sourceAttributes.put("user", "root");

    final List<String> insertQueries =
        Arrays.asList(
            "insert into root.db.d1(time, s, s1) values (1, 1, 1)",
            "insert into root.db.d2(time, s) values (2, 2)",
            "insert into root.db2.d1(time, s, t) values (3, 3, 3)",
            "insert into root.db3.d1(time, s) values (4, 4)");

    final Set<String> expectedResSet = new HashSet<>();
    expectedResSet.add("1,null,null,1.0,1.0,null,");
    expectedResSet.add("2,null,null,null,null,2.0,");
    expectedResSet.add("3,3.0,3.0,null,null,null,");

    testPipeWithMultiplePatterns(
        sourceAttributes,
        insertQueries,
        true, // isHistorical = true
        "select * from root.db2.**,root.db.**",
        "Time,root.db2.d1.s,root.db2.d1.t,root.db.d1.s,root.db.d1.s1,root.db.d2.s,",
        expectedResSet);
  }

  @Test
  public void testMultipleIoTDBPatternRealtimeData() throws Exception {
    final Map<String, String> sourceAttributes = new HashMap<>();
    sourceAttributes.put("source.path", "root.db.**, root.db2.d1.*");
    sourceAttributes.put("source.inclusion", "data.insert");
    sourceAttributes.put("user", "root");

    final List<String> insertQueries =
        Arrays.asList(
            "insert into root.db.d1(time, s, s1) values (1, 1, 1)",
            "insert into root.db.d2(time, s) values (2, 2)",
            "insert into root.db2.d1(time, s, t) values (3, 3, 3)",
            "insert into root.db3.d1(time, s) values (4, 4)");

    final Set<String> expectedResSet = new HashSet<>();
    expectedResSet.add("1,null,null,1.0,1.0,null,");
    expectedResSet.add("2,null,null,null,null,2.0,");
    expectedResSet.add("3,3.0,3.0,null,null,null,");

    testPipeWithMultiplePatterns(
        sourceAttributes,
        insertQueries,
        false, // isHistorical = false
        "select * from root.db2.**,root.db.**",
        "Time,root.db2.d1.s,root.db2.d1.t,root.db.d1.s,root.db.d1.s1,root.db.d2.s,",
        expectedResSet);
  }

  @Test
  public void testMultipleHybridPatternHistoricalData() throws Exception {
    final Map<String, String> sourceAttributes = new HashMap<>();
    sourceAttributes.put("source.path", "root.db.d1.*");
    sourceAttributes.put("source.pattern", "root.db2.d1.s");
    sourceAttributes.put("source.inclusion", "data.insert");
    sourceAttributes.put("user", "root");

    final List<String> insertQueries =
        Arrays.asList(
            "insert into root.db.d1(time, s, s1) values (1, 1, 1)",
            "insert into root.db2.d1(time, s) values (2, 2)",
            "insert into root.db3.d1(time, s) values (3, 3)");

    final Set<String> expectedResSet = new HashSet<>();
    expectedResSet.add("1,1.0,1.0,null,");
    expectedResSet.add("2,null,null,2.0,");

    testPipeWithMultiplePatterns(
        sourceAttributes,
        insertQueries,
        true, // isHistorical = true
        "select * from root.db.**,root.db2.**",
        "Time,root.db.d1.s,root.db.d1.s1,root.db2.d1.s,",
        expectedResSet);
  }

  @Test
  public void testMultipleHybridPatternRealtimeData() throws Exception {
    final Map<String, String> sourceAttributes = new HashMap<>();
    sourceAttributes.put("source.path", "root.db.d1.*");
    sourceAttributes.put("source.pattern", "root.db2.d1.s");
    sourceAttributes.put("source.inclusion", "data.insert");
    sourceAttributes.put("user", "root");

    final List<String> insertQueries =
        Arrays.asList(
            "insert into root.db.d1(time, s, s1) values (1, 1, 1)",
            "insert into root.db2.d1(time, s) values (2, 2)",
            "insert into root.db3.d1(time, s) values (3, 3)");

    final Set<String> expectedResSet = new HashSet<>();
    expectedResSet.add("1,1.0,1.0,null,");
    expectedResSet.add("2,null,null,2.0,");

    testPipeWithMultiplePatterns(
        sourceAttributes,
        insertQueries,
        false, // isHistorical = false
        "select * from root.db.**,root.db2.**",
        "Time,root.db.d1.s,root.db.d1.s1,root.db2.d1.s,",
        expectedResSet);
  }

  @Test
  public void testPrefixPatternWithExclusionHistoricalData() throws Exception {
    final Map<String, String> sourceAttributes = new HashMap<>();
    // Inclusion: Match everything under root.db.d1 and root.db.d2
    sourceAttributes.put("source.pattern", "root.db.d1, root.db.d2");
    // Exclusion: Exclude anything with the prefix root.db.d1.s1
    sourceAttributes.put("source.pattern.exclusion", "root.db.d1.s1");
    sourceAttributes.put("source.inclusion", "data.insert");
    sourceAttributes.put("user", "root");

    final List<String> insertQueries =
        Arrays.asList(
            // s matches, s1 is excluded
            "insert into root.db.d1(time, s, s1) values (1, 1, 1)",
            // s matches
            "insert into root.db.d2(time, s) values (2, 2)",
            "insert into root.db1.d1(time, s) values (3, 3)");

    final Set<String> expectedResSet = new HashSet<>();
    expectedResSet.add("1,1.0,null,");
    expectedResSet.add("2,null,2.0,");

    testPipeWithMultiplePatterns(
        sourceAttributes,
        insertQueries,
        true, // isHistorical = true
        "select * from root.db.**",
        "Time,root.db.d1.s,root.db.d2.s,",
        expectedResSet);
  }

  @Test
  public void testPrefixPatternWithExclusionRealtimeData() throws Exception {
    final Map<String, String> sourceAttributes = new HashMap<>();
    sourceAttributes.put("source.pattern", "root.db.d1, root.db.d2");
    sourceAttributes.put("source.pattern.exclusion", "root.db.d1.s1");
    sourceAttributes.put("source.inclusion", "data.insert");
    sourceAttributes.put("user", "root");

    final List<String> insertQueries =
        Arrays.asList(
            "insert into root.db.d1(time, s, s1) values (1, 1, 1)",
            "insert into root.db.d2(time, s) values (2, 2)",
            "insert into root.db1.d1(time, s) values (3, 3)");

    final Set<String> expectedResSet = new HashSet<>();
    expectedResSet.add("1,1.0,null,");
    expectedResSet.add("2,null,2.0,");

    testPipeWithMultiplePatterns(
        sourceAttributes,
        insertQueries,
        false, // isHistorical = false
        "select * from root.db.**",
        "Time,root.db.d1.s,root.db.d2.s,",
        expectedResSet);
  }

  @Test
  public void testIoTDBPatternWithExclusionHistoricalData() throws Exception {
    final Map<String, String> sourceAttributes = new HashMap<>();
    // Inclusion: Match everything under root.db
    sourceAttributes.put("source.path", "root.db.**");
    // Exclusion: Exclude root.db.d1.s* and root.db.d3.*
    sourceAttributes.put("source.path.exclusion", "root.db.d1.s*, root.db.d3.*");
    sourceAttributes.put("source.inclusion", "data.insert");
    sourceAttributes.put("user", "root");

    final List<String> insertQueries =
        Arrays.asList(
            // s, s1 excluded, t matches
            "insert into root.db.d1(time, s, s1, t) values (1, 1, 1, 1)",
            // s matches
            "insert into root.db.d2(time, s) values (2, 2)",
            // s excluded
            "insert into root.db.d3(time, s) values (3, 3)",
            "insert into root.db1.d1(time, s) values (4, 4)");

    final Set<String> expectedResSet = new HashSet<>();
    expectedResSet.add("1,1.0,null,");
    expectedResSet.add("2,null,2.0,");

    testPipeWithMultiplePatterns(
        sourceAttributes,
        insertQueries,
        true, // isHistorical = true
        "select * from root.db.**",
        "Time,root.db.d1.t,root.db.d2.s,",
        expectedResSet);
  }

  @Test
  public void testIoTDBPatternWithExclusionRealtimeData() throws Exception {
    final Map<String, String> sourceAttributes = new HashMap<>();
    sourceAttributes.put("source.path", "root.db.**");
    sourceAttributes.put("source.path.exclusion", "root.db.d1.s*, root.db.d3.*");
    sourceAttributes.put("source.inclusion", "data.insert");
    sourceAttributes.put("user", "root");

    final List<String> insertQueries =
        Arrays.asList(
            "insert into root.db.d1(time, s, s1, t) values (1, 1, 1, 1)",
            "insert into root.db.d2(time, s) values (2, 2)",
            "insert into root.db.d3(time, s) values (3, 3)",
            "insert into root.db1.d1(time, s) values (4, 4)");

    final Set<String> expectedResSet = new HashSet<>();
    expectedResSet.add("1,1.0,null,");
    expectedResSet.add("2,null,2.0,");

    testPipeWithMultiplePatterns(
        sourceAttributes,
        insertQueries,
        false, // isHistorical = false
        "select * from root.db.**",
        "Time,root.db.d1.t,root.db.d2.s,",
        expectedResSet);
  }

  @Test
  public void testHybridPatternWithHybridExclusionHistoricalData() throws Exception {
    final Map<String, String> sourceAttributes = new HashMap<>();
    // Inclusion: Match root.db.** (IoTDB) AND root.db2.d1 (Prefix)
    sourceAttributes.put("source.path", "root.db.**");
    sourceAttributes.put("source.pattern", "root.db2.d1");
    // Exclusion: Exclude root.db.d1.* (IoTDB) AND root.db2.d1.s (Prefix)
    sourceAttributes.put("source.path.exclusion", "root.db.d1.*");
    sourceAttributes.put("source.pattern.exclusion", "root.db2.d1.s");
    sourceAttributes.put("source.inclusion", "data.insert");
    sourceAttributes.put("user", "root");

    final List<String> insertQueries =
        Arrays.asList(
            // s, s1 excluded by path.exclusion
            "insert into root.db.d1(time, s, s1) values (1, 1, 1)",
            // s matches
            "insert into root.db.d2(time, s) values (2, 2)",
            // s excluded by pattern.exclusion, t matches
            "insert into root.db2.d1(time, s, t) values (3, 3, 3)",
            "insert into root.db3.d1(time, s) values (4, 4)");

    final Set<String> expectedResSet = new HashSet<>();
    expectedResSet.add("2,2.0,null,");
    expectedResSet.add("3,null,3.0,");

    testPipeWithMultiplePatterns(
        sourceAttributes,
        insertQueries,
        true, // isHistorical = true
        "select * from root.db.**,root.db2.**",
        "Time,root.db.d2.s,root.db2.d1.t,",
        expectedResSet);
  }

  @Test
  public void testHybridPatternWithHybridExclusionRealtimeData() throws Exception {
    final Map<String, String> sourceAttributes = new HashMap<>();
    sourceAttributes.put("source.path", "root.db.**");
    sourceAttributes.put("source.pattern", "root.db2.d1");
    sourceAttributes.put("source.path.exclusion", "root.db.d1.*");
    sourceAttributes.put("source.pattern.exclusion", "root.db2.d1.s");
    sourceAttributes.put("source.inclusion", "data.insert");
    sourceAttributes.put("user", "root");

    final List<String> insertQueries =
        Arrays.asList(
            "insert into root.db.d1(time, s, s1) values (1, 1, 1)",
            "insert into root.db.d2(time, s) values (2, 2)",
            "insert into root.db2.d1(time, s, t) values (3, 3, 3)",
            "insert into root.db3.d1(time, s) values (4, 4)");

    final Set<String> expectedResSet = new HashSet<>();
    expectedResSet.add("2,2.0,null,");
    expectedResSet.add("3,null,3.0,");

    testPipeWithMultiplePatterns(
        sourceAttributes,
        insertQueries,
        false, // isHistorical = false
        "select * from root.db.**,root.db2.**",
        "Time,root.db.d2.s,root.db2.d1.t,",
        expectedResSet);
  }
}
