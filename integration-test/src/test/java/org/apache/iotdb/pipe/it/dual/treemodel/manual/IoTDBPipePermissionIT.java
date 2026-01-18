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

package org.apache.iotdb.pipe.it.dual.treemodel.manual;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.confignode.rpc.thrift.TCreatePipeReq;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.it.env.MultiEnvFactory;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT2DualTreeManual;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2DualTreeManual.class})
public class IoTDBPipePermissionIT extends AbstractPipeDualTreeModelManualIT {
  @Override
  @Before
  public void setUp() {
    MultiEnvFactory.createEnv(2);
    senderEnv = MultiEnvFactory.getEnv(0);
    receiverEnv = MultiEnvFactory.getEnv(1);

    senderEnv
        .getConfig()
        .getCommonConfig()
        .setAutoCreateSchemaEnabled(false)
        .setDefaultSchemaRegionGroupNumPerDatabase(1)
        .setTimestampPrecision("ms")
        .setConfigNodeConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setSchemaRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setDnConnectionTimeoutMs(600000)
        .setPipeMemoryManagementEnabled(false)
        .setIsPipeEnableMemoryCheck(false)
        .setPipeAutoSplitFullEnabled(false);
    senderEnv.getConfig().getDataNodeConfig().setDataNodeMemoryProportion("3:3:1:1:3:1");
    receiverEnv
        .getConfig()
        .getCommonConfig()
        .setAutoCreateSchemaEnabled(false)
        .setTimestampPrecision("ms")
        .setConfigNodeConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setSchemaRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setDataRegionConsensusProtocolClass(ConsensusFactory.IOT_CONSENSUS)
        .setSchemaReplicationFactor(3)
        .setDataReplicationFactor(2)
        .setDnConnectionTimeoutMs(600000)
        .setPipeMemoryManagementEnabled(false)
        .setIsPipeEnableMemoryCheck(false)
        .setPipeAutoSplitFullEnabled(false);

    senderEnv.initClusterEnvironment();
    receiverEnv.initClusterEnvironment(3, 3);
  }

  @Test
  public void testWithSyncSink() throws Exception {
    testWithSink("iotdb-thrift-sync-sink");
  }

  @Test
  public void testWithAsyncSink() throws Exception {
    testWithSink("iotdb-thrift-async-sink");
  }

  private void testWithSink(final String sink) throws Exception {
    TestUtils.executeNonQueries(
        receiverEnv,
        Arrays.asList(
            "create user `thulab` 'passwd123456'",
            "create role `admin`",
            "grant role `admin` to `thulab`",
            "grant WRITE, READ, SYSTEM, SECURITY on root.** to role `admin`"),
        null);

    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);
    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      TestUtils.executeNonQueries(
          senderEnv,
          Arrays.asList(
              "create user user 'passwd123456'",
              "create timeseries root.ln.wf02.wt01.temperature with datatype=INT64,encoding=PLAIN",
              "create timeseries root.ln.wf02.wt01.status with datatype=BOOLEAN,encoding=PLAIN",
              "insert into root.ln.wf02.wt01(time, temperature, status) values (1800000000000, 23, true)"),
          null);
      awaitUntilFlush(senderEnv);

      final Map<String, String> sourceAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> sinkAttributes = new HashMap<>();

      sourceAttributes.put("source.inclusion", "all");
      sourceAttributes.put("user", "root");

      sinkAttributes.put("sink", sink);
      sinkAttributes.put("sink.ip", receiverIp);
      sinkAttributes.put("sink.port", Integer.toString(receiverPort));
      sinkAttributes.put("sink.username", "thulab");
      sinkAttributes.put("sink.password", "passwd123456");

      final TSStatus status =
          client.createPipe(
              new TCreatePipeReq("testPipe", sinkAttributes)
                  .setExtractorAttributes(sourceAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("testPipe").getCode());

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "list user",
          "UserId,User,",
          new HashSet<>(Arrays.asList("0,root,", "10001,user,", "10000,thulab,")));
      final Set<String> expectedResSet = new HashSet<>();
      expectedResSet.add(
          "root.ln.wf02.wt01.temperature,null,root.ln,INT64,PLAIN,LZ4,null,null,null,null,BASE,");
      expectedResSet.add(
          "root.ln.wf02.wt01.status,null,root.ln,BOOLEAN,PLAIN,LZ4,null,null,null,null,BASE,");
      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "show timeseries root.ln.**",
          "Timeseries,Alias,Database,DataType,Encoding,Compression,Tags,Attributes,Deadband,DeadbandParameters,ViewType,",
          expectedResSet);
      expectedResSet.clear();

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "select * from root.ln.**",
          "Time,root.ln.wf02.wt01.temperature,root.ln.wf02.wt01.status,",
          Collections.singleton("1800000000000,23,true,"));
    }
  }

  @Test
  public void testNoPermission() throws Exception {
    TestUtils.executeNonQueries(
        receiverEnv,
        Arrays.asList(
            "create user `thulab` 'passwd123456'",
            "create role `admin`",
            "grant role `admin` to `thulab`",
            "grant READ on root.ln.** to role `admin`"),
        null);

    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);
    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      TestUtils.executeNonQueries(
          senderEnv,
          Arrays.asList(
              "create user someUser 'passwd'",
              "create timeseries root.noPermission.wf02.wt01.status with datatype=BOOLEAN,encoding=PLAIN"),
          null);
      awaitUntilFlush(senderEnv);

      final Map<String, String> sourceAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> sinkAttributes = new HashMap<>();

      sourceAttributes.put("source.inclusion", "all");
      sourceAttributes.put("user", "root");

      sinkAttributes.put("sink", "iotdb-thrift-async-sink");
      sinkAttributes.put("sink.ip", receiverIp);
      sinkAttributes.put("sink.port", Integer.toString(receiverPort));
      sinkAttributes.put("sink.username", "thulab");
      sinkAttributes.put("sink.password", "passwd123456");

      final TSStatus status =
          client.createPipe(
              new TCreatePipeReq("testPipe", sinkAttributes)
                  .setExtractorAttributes(sourceAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("testPipe").getCode());

      TestUtils.assertDataAlwaysOnEnv(
          receiverEnv,
          "show timeseries",
          "Timeseries,Alias,Database,DataType,Encoding,Compression,Tags,Attributes,Deadband,DeadbandParameters,ViewType,",
          Collections.emptySet());
      TestUtils.assertDataAlwaysOnEnv(
          receiverEnv, "list user", "User,", Collections.singleton("root,"));
    }
  }

  @Test
  public void testSourcePermission() {
    TestUtils.executeNonQuery(senderEnv, "create user `thulab` 'passwD@123456'", null);

    // Shall fail if username is specified without password
    try (final Connection connection = senderEnv.getConnection();
        final Statement statement = connection.createStatement()) {
      statement.execute(
          String.format(
              "create pipe a2b"
                  + " with source ("
                  + "'user'='thulab')"
                  + " with sink ("
                  + "'node-urls'='%s')",
              receiverEnv.getDataNodeWrapperList().get(0).getIpAndPortString()));
      fail("When the 'user' or 'username' is specified, password must be specified too.");
    } catch (final SQLException e) {
      Assert.assertEquals(
          "701: Failed to create pipe a2b, in iotdb-source, password must be set when the username is specified.",
          e.getMessage());
    }

    // Shall fail if password is wrong
    try (final Connection connection = senderEnv.getConnection();
        final Statement statement = connection.createStatement()) {
      statement.execute(
          String.format(
              "create pipe a2b"
                  + " with source ("
                  + "'user'='thulab'"
                  + ", 'password'='hack')"
                  + " with sink ("
                  + "'node-urls'='%s')",
              receiverEnv.getDataNodeWrapperList().get(0).getIpAndPortString()));
      fail("Shall fail if password is wrong.");
    } catch (final SQLException e) {
      Assert.assertTrue(
          e.getMessage().contains("Fail to CREATE_PIPE because Authentication failed."));
    }

    // Use current session, user is root
    try (final Connection connection = senderEnv.getConnection();
        final Statement statement = connection.createStatement()) {
      statement.execute(
          String.format(
              "create pipe a2b"
                  + " with source ("
                  + "'inclusion'='all')"
                  + " with sink ("
                  + "'node-urls'='%s')",
              receiverEnv.getDataNodeWrapperList().get(0).getIpAndPortString()));
    } catch (final SQLException e) {
      e.printStackTrace();
      fail("Create pipe without user shall succeed if use the current session");
    }

    // Alter to another user, shall fail because of lack of password
    try (final Connection connection = senderEnv.getConnection();
        final Statement statement = connection.createStatement()) {
      statement.execute("alter pipe a2b modify source ('username'='thulab')");
      fail("Alter pipe shall fail if only user is specified");
    } catch (final SQLException e) {
      Assert.assertEquals(
          "1107: Failed to alter pipe a2b, in iotdb-source, password must be set when the username is specified.",
          e.getMessage());
    }

    // Successfully alter
    try (final Connection connection = senderEnv.getConnection();
        final Statement statement = connection.createStatement()) {
      statement.execute(
          "alter pipe a2b modify source ('username'='thulab', 'password'='passwD@123456')");
    } catch (final SQLException e) {
      e.printStackTrace();
      fail("Alter pipe shall not fail if user and password are specified");
    }

    TestUtils.executeNonQuery(senderEnv, "create database root.test");

    // root.test / root.db shall not be transferred
    TestUtils.assertDataAlwaysOnEnv(
        receiverEnv, "count databases", "count,", Collections.singleton("0,"));

    // Test snapshot filter
    TestUtils.executeNonQueries(
        senderEnv,
        Arrays.asList(
            "create database root.db", "create timeSeries root.db.device.measurement int32"),
        null);

    // Transfer snapshot
    try (final Connection connection = senderEnv.getConnection();
        final Statement statement = connection.createStatement()) {
      statement.execute("drop pipe a2b");
      statement.execute(
          String.format(
              "create pipe a2b"
                  + " with source ("
                  + "'inclusion'='all', "
                  + "'username'='thulab', "
                  + "'password'='passwD@123456')"
                  + " with sink ("
                  + "'node-urls'='%s')",
              receiverEnv.getDataNodeWrapperList().get(0).getIpAndPortString()));
    } catch (final SQLException e) {
      e.printStackTrace();
      fail("Shall not fail if user and password are specified");
    }

    TestUtils.executeNonQuery(receiverEnv, "create database root.db");

    // root.db.device.measurement shall not be transferred
    TestUtils.assertDataAlwaysOnEnv(
        receiverEnv,
        "count timeSeries root.db.**",
        "count(timeseries),",
        Collections.singleton("0,"));

    // GRANT privileges ON prefixPath (COMMA prefixPath)* TO USER userName=usernameWithRoot
    // (grantOpt)?
    // Grant some privilege
    TestUtils.executeNonQuery(senderEnv, "grant SYSTEM on root.** to user thulab");

    TestUtils.executeNonQuery(senderEnv, "create database root.test1");

    // Shall be transferred
    // The root.test may or may not be transferred due to delayed start of the config source
    TestUtils.assertDataEventuallyOnEnv(
        receiverEnv, "count databases root.test1", "count,", Collections.singleton("1,"));

    // Write some data
    TestUtils.executeNonQueries(
        senderEnv,
        Arrays.asList(
            "create timeSeries root.vehicle.car.temperature DOUBLE",
            "insert into root.vehicle.car(temperature) values (36.5)"));

    // Exception, skip
    TestUtils.assertDataAlwaysOnEnv(
        receiverEnv, "count timeSeries", "count(timeseries),", Collections.singleton("0,"));

    // Provide time series
    TestUtils.executeNonQuery(receiverEnv, "create timeSeries root.vehicle.car.temperature DOUBLE");

    // Exception, skip
    TestUtils.assertDataAlwaysOnEnv(
        receiverEnv,
        "select count(temperature) from root.vehicle.car",
        "count(root.vehicle.car.pressure),",
        Collections.singleton("0,"));

    // Alter pipe, throw exception if no privileges
    try (final Connection connection = senderEnv.getConnection();
        final Statement statement = connection.createStatement()) {
      statement.execute("alter pipe a2b modify source ('skipif'='')");
    } catch (final SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    // Write some data
    TestUtils.executeNonQueries(
        senderEnv,
        Arrays.asList(
            "create timeSeries root.vehicle.car.pressure DOUBLE",
            "insert into root.vehicle.car(pressure) values (36.5)"));

    // Exception, block here
    TestUtils.assertDataAlwaysOnEnv(
        receiverEnv,
        "select count(pressure) from root.vehicle.car",
        "count(root.vehicle.car.pressure),",
        Collections.singleton("0,"));

    // Grant SELECT privilege
    TestUtils.executeNonQueries(
        senderEnv, Arrays.asList("grant READ on root.** to user thulab", "start pipe a2b"));

    // Will finally pass
    TestUtils.assertDataEventuallyOnEnv(
        receiverEnv,
        "select count(pressure) from root.vehicle.car",
        "count(root.vehicle.car.pressure),",
        Collections.singleton("1,"));

    // test showing pipe
    // Create another pipe, user is root
    try (final Connection connection = senderEnv.getConnection();
        final Statement statement = connection.createStatement()) {
      statement.execute(
          String.format(
              "create pipe a2c"
                  + " with source ("
                  + "'inclusion'='all',"
                  + "'capture.tree'='true',"
                  + "'capture.table'='true')"
                  + " with sink ("
                  + "'node-urls'='%s')",
              receiverEnv.getDataNodeWrapperList().get(0).getIpAndPortString()));
    } catch (final SQLException e) {
      e.printStackTrace();
      fail("Create pipe without user shall succeed if use the current session");
    }

    TestUtils.executeNonQuery(senderEnv, "revoke SYSTEM on root.** from user thulab");

    // A user shall only see its own pipe
    try (final Connection connection = senderEnv.getConnection("thulab", "passwD@123456");
        final Statement statement = connection.createStatement()) {
      // Will not throw any exception
      final ResultSet resultSet = statement.executeQuery("show pipes");
      Assert.assertTrue(resultSet.next());
      Assert.assertFalse(resultSet.next());
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }
}
