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

package com.timecho.iotdb.db.it.audit;

import org.apache.iotdb.commons.audit.AuditEventType;
import org.apache.iotdb.commons.audit.AuditLogOperation;
import org.apache.iotdb.commons.audit.PrivilegeLevel;
import org.apache.iotdb.commons.auth.entity.User;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.env.BaseEnv;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;

import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Verifies the persisted minimum audit record for real inter-DataNode user-data transfers. */
@RunWith(IoTDBTestRunner.class)
@Category({ClusterIT.class})
public class TimechoDBUserDataTransferAuditLogIT {

  private static final String DATABASE = "root.user_data_transfer_audit_it";
  private static final String DEVICE = DATABASE + ".d1";
  private static final long POLL_TIMEOUT_MS = TimeUnit.MINUTES.toMillis(1);
  private static final long POLL_INTERVAL_MS = 500L;

  private static final Pattern TRANSFER_LOG_PATTERN =
      Pattern.compile(
          "^.*(?:time|时间)=(\\d+)[,，]\\s*(?:initiator|发起者)=([^,，]+)[,，]\\s*"
              + "(?:source|源端)=([^,，]+)[,，]\\s*(?:target|目标端)=([^,，]+)[,，]\\s*"
              + "(?:protection_method|保护方法)=(TLS|NONE)[,，]\\s*"
              + "(?:result|结果)=(true|false)[,，]\\s*(?:error|错误)=(.*)$");

  @Rule public final TestName testName = new TestName();

  @Before
  public void setUp() {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setTimestampPrecision("ns")
        .setDataRegionConsensusProtocolClass(dataRegionConsensusProtocol())
        .setIoTConsensusV2Mode(iotConsensusV2Mode())
        .setSchemaRegionConsensusProtocolClass(
            isFailedTransferTest()
                ? ConsensusFactory.RATIS_CONSENSUS
                : ConsensusFactory.SIMPLE_CONSENSUS)
        .setSchemaReplicationFactor(isFailedTransferTest() ? 3 : 1)
        .setDataReplicationFactor(isQueryAndWriteTransferTest() ? 1 : 2)
        .setDataRegionGroupExtensionPolicy(isQueryAndWriteTransferTest() ? "CUSTOM" : "AUTO")
        .setDefaultDataRegionGroupNumPerDatabase(isQueryAndWriteTransferTest() ? 3 : 1)
        .setEnableAuditLog(true)
        .setAuditableOperationType(AuditLogOperation.CONTROL.name())
        .setAuditableOperationLevel(PrivilegeLevel.GLOBAL.name())
        .setAuditableOperationResult("SUCCESS,FAIL")
        .setAuditableControlEventType(AuditEventType.USER_DATA_TRANSFER.name());
    if (isInternalTlsTest()) {
      configureInternalTls();
    }
    EnvFactory.getEnv().initClusterEnvironment(1, 3);
  }

  @After
  public void tearDown() {
    try {
      EnvFactory.getEnv().cleanClusterEnvironment();
    } finally {
      if (isInternalTlsTest()) {
        resetTestProcessInternalTls();
      }
    }
  }

  @Test
  public void testIoTConsensusV1UserDataTransferAuditLog() throws Exception {
    assertConsensusUserDataTransferAuditLog();
  }

  @Test
  public void testIoTConsensusV2BatchUserDataTransferAuditLog() throws Exception {
    assertConsensusUserDataTransferAuditLog();
  }

  @Test
  public void testIoTConsensusV2StreamUserDataTransferAuditLog() throws Exception {
    assertConsensusUserDataTransferAuditLog();
  }

  @Test
  public void testIoTConsensusV1TlsUserDataTransferAuditLog() throws Exception {
    assertConsensusUserDataTransferAuditLog();
  }

  @Test
  public void testIoTConsensusV1FailedUserDataTransferAuditLog() throws Exception {
    final long firstUserDataWriteTime = writeUserData();
    final Set<String> consensusEndpoints = getDataRegionConsensusEndpoints();
    final AuditRecord successfulRecord =
        waitForConsensusTransferAuditRecord(firstUserDataWriteTime, consensusEndpoints);
    final DataNodeWrapper source =
        findDataNodeByConsensusEndpoint(successfulRecord.transferDetails.source);
    final DataNodeWrapper target =
        findDataNodeByConsensusEndpoint(successfulRecord.transferDetails.target);

    target.stopForcibly();
    final long firstFailedTransferTime = System.currentTimeMillis();
    try (Connection connection =
            EnvFactory.getEnv()
                .getConnection(
                    source,
                    SessionConfig.DEFAULT_USER,
                    SessionConfig.DEFAULT_PASSWORD,
                    BaseEnv.TREE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      for (int i = 100; i < 120; i++) {
        statement.execute(
            "INSERT INTO " + DEVICE + "(time, s1) VALUES (" + (i + 1) + ", " + i + ")");
      }
    }

    final AuditRecord failedRecord =
        waitForMatchingTransferAuditRecord(
            firstFailedTransferTime,
            Long.MAX_VALUE,
            source,
            false,
            "a failed IoTConsensus V1 transfer from "
                + successfulRecord.transferDetails.source
                + " to "
                + successfulRecord.transferDetails.target,
            details ->
                successfulRecord.transferDetails.source.equals(details.source)
                    && successfulRecord.transferDetails.target.equals(details.target));
    assertMinimumAuditRecord(failedRecord, "NONE", false);
  }

  @Test
  public void testMppAndInternalWriteUserDataTransferAuditLog() throws Exception {
    final DataNodeWrapper coordinator = EnvFactory.getEnv().getDataNodeWrapper(0);
    final long firstUserDataTransferTime = System.currentTimeMillis();

    try (Connection connection =
            EnvFactory.getEnv()
                .getConnection(
                    coordinator,
                    SessionConfig.DEFAULT_USER,
                    SessionConfig.DEFAULT_PASSWORD,
                    BaseEnv.TREE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE " + DATABASE);
      for (int i = 0; i < 120; i++) {
        statement.execute(
            "INSERT INTO "
                + DATABASE
                + ".d"
                + i
                + "(time, s1) VALUES ("
                + (i + 1)
                + ", "
                + i
                + ")");
      }
      assertUserDataSpansMultipleDataNodes(statement);
      try (ResultSet resultSet =
          statement.executeQuery("SELECT count(s1) FROM " + DATABASE + ".**")) {
        Assert.assertTrue(resultSet.next());
        while (resultSet.next()) {
          // Consume the complete distributed result.
        }
      }
    }

    final long lastUserDataTransferTime = System.currentTimeMillis();
    final AuditRecord internalWriteRecord =
        waitForInternalWriteTransferAuditRecord(
            firstUserDataTransferTime, lastUserDataTransferTime, coordinator);
    final AuditRecord mppRecord =
        waitForMppTransferAuditRecord(
            firstUserDataTransferTime, lastUserDataTransferTime, coordinator);

    assertMinimumAuditRecord(internalWriteRecord, "NONE", true);
    Assert.assertEquals(
        internalWriteRecord.transferDetails.source, internalWriteRecord.cliHostname);
    Assert.assertEquals(
        internalWriteRecord.transferDetails.source, internalWriteRecord.transferDetails.initiator);

    assertMinimumAuditRecord(mppRecord, "NONE", true);
    Assert.assertEquals(mppRecord.transferDetails.initiator, mppRecord.cliHostname);
    Assert.assertEquals(mppRecord.transferDetails.target, mppRecord.transferDetails.initiator);
  }

  private void assertConsensusUserDataTransferAuditLog() throws Exception {
    final long firstUserDataWriteTime = writeUserData();
    final Set<String> consensusEndpoints = getDataRegionConsensusEndpoints();

    final AuditRecord record =
        waitForConsensusTransferAuditRecord(firstUserDataWriteTime, consensusEndpoints);
    assertMinimumAuditRecord(record, expectedProtectionMethod(), true);
    final TransferDetails details = record.transferDetails;
    Assert.assertTrue(details.timestamp >= firstUserDataWriteTime);
    Assert.assertEquals(details.source, details.initiator);
    Assert.assertEquals(details.source, record.cliHostname);
    Assert.assertTrue(consensusEndpoints.contains(details.source));
    Assert.assertTrue(consensusEndpoints.contains(details.target));
    Assert.assertNotEquals(details.source, details.target);
  }

  private String dataRegionConsensusProtocol() {
    return testName.getMethodName().contains("IoTConsensusV2")
        ? ConsensusFactory.IOT_CONSENSUS_V2
        : ConsensusFactory.IOT_CONSENSUS;
  }

  private String iotConsensusV2Mode() {
    return testName.getMethodName().contains("Stream")
        ? ConsensusFactory.IOT_CONSENSUS_V2_STREAM_MODE
        : ConsensusFactory.IOT_CONSENSUS_V2_BATCH_MODE;
  }

  private boolean isQueryAndWriteTransferTest() {
    return testName.getMethodName().contains("MppAndInternalWrite");
  }

  private boolean isInternalTlsTest() {
    return testName.getMethodName().contains("Tls");
  }

  private boolean isFailedTransferTest() {
    return testName.getMethodName().contains("Failed");
  }

  private String expectedProtectionMethod() {
    return isInternalTlsTest() ? "TLS" : "NONE";
  }

  private static long writeUserData() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TREE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE " + DATABASE);
      statement.execute("CREATE TIMESERIES " + DEVICE + ".s1 WITH DATATYPE=INT32, ENCODING=PLAIN");

      final long firstUserDataWriteTime = System.currentTimeMillis();
      for (int i = 0; i < 20; i++) {
        statement.execute(
            "INSERT INTO " + DEVICE + "(time, s1) VALUES (" + (i + 1) + ", " + i + ")");
      }
      return firstUserDataWriteTime;
    }
  }

  private static Set<String> getDataRegionConsensusEndpoints() {
    final Set<String> endpoints = new HashSet<>();
    for (DataNodeWrapper dataNode : EnvFactory.getEnv().getDataNodeWrapperList()) {
      endpoints.add(dataNode.getIp() + ":" + dataNode.getDataRegionConsensusPort());
    }
    Assert.assertEquals(3, endpoints.size());
    return endpoints;
  }

  private static DataNodeWrapper findDataNodeByConsensusEndpoint(String endpoint) {
    for (DataNodeWrapper dataNode : EnvFactory.getEnv().getDataNodeWrapperList()) {
      if (endpoint.equals(dataNode.getIp() + ":" + dataNode.getDataRegionConsensusPort())) {
        return dataNode;
      }
    }
    Assert.fail("No DataNode has DataRegion consensus endpoint " + endpoint);
    return null;
  }

  private static void assertUserDataSpansMultipleDataNodes(Statement statement)
      throws SQLException {
    final Set<Integer> dataNodeIds = new HashSet<>();
    try (ResultSet resultSet = statement.executeQuery("SHOW REGIONS")) {
      while (resultSet.next()) {
        if (DATABASE.equals(resultSet.getString("Database"))
            && "DataRegion".equals(resultSet.getString("Type"))) {
          dataNodeIds.add(resultSet.getInt("DataNodeId"));
        }
      }
    }
    Assert.assertTrue(
        "The test data must span at least two DataNodes, but was on " + dataNodeIds,
        dataNodeIds.size() >= 2);
  }

  private static AuditRecord waitForInternalWriteTransferAuditRecord(
      long firstTransferTime, long lastTransferTime, DataNodeWrapper coordinator) throws Exception {
    final Set<String> internalEndpoints = new HashSet<>();
    for (DataNodeWrapper dataNode : EnvFactory.getEnv().getDataNodeWrapperList()) {
      internalEndpoints.add(dataNode.getIp() + ":" + dataNode.getInternalPort());
    }
    final String coordinatorEndpoint = coordinator.getIp() + ":" + coordinator.getInternalPort();
    return waitForMatchingTransferAuditRecord(
        firstTransferTime,
        lastTransferTime,
        coordinator,
        true,
        "an internal write request from " + coordinatorEndpoint,
        details ->
            coordinatorEndpoint.equals(details.source)
                && internalEndpoints.contains(details.target)
                && !details.source.equals(details.target));
  }

  private static AuditRecord waitForMppTransferAuditRecord(
      long firstTransferTime, long lastTransferTime, DataNodeWrapper coordinator) throws Exception {
    final Set<String> mppEndpoints = new HashSet<>();
    for (DataNodeWrapper dataNode : EnvFactory.getEnv().getDataNodeWrapperList()) {
      mppEndpoints.add(dataNode.getIp() + ":" + dataNode.getMppDataExchangePort());
    }
    final String coordinatorEndpoint =
        coordinator.getIp() + ":" + coordinator.getMppDataExchangePort();
    return waitForMatchingTransferAuditRecord(
        firstTransferTime,
        lastTransferTime,
        coordinator,
        true,
        "an MPP result transfer to " + coordinatorEndpoint,
        details ->
            mppEndpoints.contains(details.source)
                && coordinatorEndpoint.equals(details.target)
                && !details.source.equals(details.target));
  }

  private static AuditRecord waitForMatchingTransferAuditRecord(
      long firstTransferTime,
      long lastTransferTime,
      DataNodeWrapper auditReader,
      boolean expectedResult,
      String description,
      TransferMatcher transferMatcher)
      throws Exception {
    final long deadline = System.currentTimeMillis() + POLL_TIMEOUT_MS;
    final List<String> observedLogs = new ArrayList<>();
    try (Connection connection =
            EnvFactory.getEnv()
                .getConnection(
                    auditReader,
                    SessionConfig.DEFAULT_USER,
                    SessionConfig.DEFAULT_PASSWORD,
                    BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      while (System.currentTimeMillis() < deadline) {
        try (ResultSet resultSet =
            statement.executeQuery(userDataTransferAuditQuery(expectedResult))) {
          while (resultSet.next()) {
            final AuditRecord record = AuditRecord.from(resultSet);
            if (observedLogs.size() < 20 && !observedLogs.contains(record.log)) {
              observedLogs.add(record.log);
            }
            final TransferDetails details = TransferDetails.parse(record.log);
            if (details != null
                && details.timestamp >= firstTransferTime
                && details.timestamp <= lastTransferTime
                && transferMatcher.matches(details)) {
              record.transferDetails = details;
              return record;
            }
          }
        } catch (SQLException ignored) {
          // The audit database and table view are initialized lazily by the first audit event.
        }
        TimeUnit.MILLISECONDS.sleep(POLL_INTERVAL_MS);
      }
    }
    Assert.fail("Timed out waiting for " + description + ". Observed logs: " + observedLogs);
    return null;
  }

  private static AuditRecord waitForConsensusTransferAuditRecord(
      long firstUserDataWriteTime, Set<String> consensusEndpoints) throws Exception {
    final long deadline = System.currentTimeMillis() + POLL_TIMEOUT_MS;
    final List<String> observedLogs = new ArrayList<>();

    try (Connection connection =
            EnvFactory.getEnv()
                .getConnection(
                    EnvFactory.getEnv().getDataNodeWrapper(0),
                    SessionConfig.DEFAULT_USER,
                    SessionConfig.DEFAULT_PASSWORD,
                    BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      while (System.currentTimeMillis() < deadline) {
        try {
          final AuditRecord record =
              findConsensusTransferAuditRecord(
                  statement, firstUserDataWriteTime, consensusEndpoints, observedLogs);
          if (record != null) {
            return record;
          }
        } catch (SQLException ignored) {
          // The audit database and table view are initialized lazily by the first audit event.
        }
        TimeUnit.MILLISECONDS.sleep(POLL_INTERVAL_MS);
      }
    }

    Assert.fail(
        "Timed out waiting for a successful USER_DATA_TRANSFER between DataRegion consensus "
            + "endpoints "
            + consensusEndpoints
            + ". Observed logs: "
            + observedLogs);
    return null;
  }

  private static AuditRecord findConsensusTransferAuditRecord(
      Statement statement,
      long firstUserDataWriteTime,
      Set<String> consensusEndpoints,
      List<String> observedLogs)
      throws SQLException {
    try (ResultSet resultSet = statement.executeQuery(userDataTransferAuditQuery(true))) {
      while (resultSet.next()) {
        final AuditRecord record = AuditRecord.from(resultSet);
        if (observedLogs.size() < 20 && !observedLogs.contains(record.log)) {
          observedLogs.add(record.log);
        }
        final TransferDetails details = TransferDetails.parse(record.log);
        if (details != null
            && details.timestamp >= firstUserDataWriteTime
            && consensusEndpoints.contains(details.source)
            && consensusEndpoints.contains(details.target)
            && !details.source.equals(details.target)) {
          record.transferDetails = details;
          return record;
        }
      }
    }
    return null;
  }

  private static String userDataTransferAuditQuery(boolean result) {
    return "SELECT time, username, cli_hostname, audit_event_type, operation_type,"
        + " privilege_type, privilege_level, result, database, sql_string, log"
        + " FROM __audit.audit_log"
        + " WHERE audit_event_type = 'USER_DATA_TRANSFER' AND result = "
        + result
        + " ORDER BY time DESC";
  }

  private static void assertMinimumAuditRecord(
      AuditRecord record, String expectedProtectionMethod, boolean expectedResult) {
    Assert.assertNotNull(record.time);
    Assert.assertFalse(record.time.isEmpty());
    Assert.assertEquals(User.BUILTIN_INTERNAL_AUDIT_LOG_USERNAME, record.username);
    Assert.assertEquals(AuditEventType.USER_DATA_TRANSFER.name(), record.auditEventType);
    Assert.assertEquals(AuditLogOperation.CONTROL.name(), record.operationType);
    Assert.assertEquals("null", record.privilegeType);
    Assert.assertEquals(PrivilegeLevel.GLOBAL.name(), record.privilegeLevel);
    Assert.assertEquals(expectedResult, record.result);
    Assert.assertEquals("", record.database);
    Assert.assertEquals("", record.sqlString);

    final TransferDetails details = record.transferDetails;
    Assert.assertNotNull(details);
    Assert.assertEquals(expectedProtectionMethod, details.protectionMethod);
    Assert.assertEquals(expectedResult, details.result);
    if (expectedResult) {
      Assert.assertEquals("null", details.error);
    } else {
      Assert.assertNotEquals("null", details.error);
      Assert.assertFalse(details.error.isEmpty());
    }
  }

  private void configureInternalTls() {
    final String keyDirectory =
        System.getProperty("user.dir")
            + File.separator
            + "target"
            + File.separator
            + "test-classes"
            + File.separator;
    final String keyStorePath = keyDirectory + "test-keystore";
    final String trustStorePath = keyDirectory + "test-truststore";
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setEnableInternalSSL(true)
        .setKeyStorePath(keyStorePath)
        .setKeyStorePwd("thrift")
        .setTrustStorePath(trustStorePath)
        .setTrustStorePwd("thrift")
        .setSslProtocol(SessionConfig.DEFAULT_SSL_PROTOCOL);
    CommonDescriptor.getInstance().getConfig().setEnableInternalSSL(true);
    CommonDescriptor.getInstance().getConfig().setKeyStorePath(keyStorePath);
    CommonDescriptor.getInstance().getConfig().setKeyStorePwd("thrift");
    CommonDescriptor.getInstance().getConfig().setTrustStorePath(trustStorePath);
    CommonDescriptor.getInstance().getConfig().setTrustStorePwd("thrift");
    CommonDescriptor.getInstance().getConfig().setSslProtocol(SessionConfig.DEFAULT_SSL_PROTOCOL);
  }

  private static void resetTestProcessInternalTls() {
    CommonDescriptor.getInstance().getConfig().setEnableInternalSSL(false);
    CommonDescriptor.getInstance().getConfig().setKeyStorePath("");
    CommonDescriptor.getInstance().getConfig().setKeyStorePwd("");
    CommonDescriptor.getInstance().getConfig().setTrustStorePath("");
    CommonDescriptor.getInstance().getConfig().setTrustStorePwd("");
    CommonDescriptor.getInstance().getConfig().setSslProtocol(SessionConfig.DEFAULT_SSL_PROTOCOL);
  }

  @FunctionalInterface
  private interface TransferMatcher {
    boolean matches(TransferDetails details);
  }

  private static final class AuditRecord {
    private final String time;
    private final String username;
    private final String cliHostname;
    private final String auditEventType;
    private final String operationType;
    private final String privilegeType;
    private final String privilegeLevel;
    private final boolean result;
    private final String database;
    private final String sqlString;
    private final String log;
    private TransferDetails transferDetails;

    private AuditRecord(
        String time,
        String username,
        String cliHostname,
        String auditEventType,
        String operationType,
        String privilegeType,
        String privilegeLevel,
        boolean result,
        String database,
        String sqlString,
        String log) {
      this.time = time;
      this.username = username;
      this.cliHostname = cliHostname;
      this.auditEventType = auditEventType;
      this.operationType = operationType;
      this.privilegeType = privilegeType;
      this.privilegeLevel = privilegeLevel;
      this.result = result;
      this.database = database;
      this.sqlString = sqlString;
      this.log = log;
    }

    private static AuditRecord from(ResultSet resultSet) throws SQLException {
      return new AuditRecord(
          resultSet.getString("time"),
          resultSet.getString("username"),
          resultSet.getString("cli_hostname"),
          resultSet.getString("audit_event_type"),
          resultSet.getString("operation_type"),
          resultSet.getString("privilege_type"),
          resultSet.getString("privilege_level"),
          resultSet.getBoolean("result"),
          resultSet.getString("database"),
          resultSet.getString("sql_string"),
          resultSet.getString("log"));
    }
  }

  private static final class TransferDetails {
    private final long timestamp;
    private final String initiator;
    private final String source;
    private final String target;
    private final String protectionMethod;
    private final boolean result;
    private final String error;

    private TransferDetails(
        long timestamp,
        String initiator,
        String source,
        String target,
        String protectionMethod,
        boolean result,
        String error) {
      this.timestamp = timestamp;
      this.initiator = initiator;
      this.source = source;
      this.target = target;
      this.protectionMethod = protectionMethod;
      this.result = result;
      this.error = error;
    }

    private static TransferDetails parse(String log) {
      if (log == null) {
        return null;
      }
      final Matcher matcher = TRANSFER_LOG_PATTERN.matcher(log);
      if (!matcher.matches()) {
        return null;
      }
      return new TransferDetails(
          Long.parseLong(matcher.group(1)),
          matcher.group(2),
          matcher.group(3),
          matcher.group(4),
          matcher.group(5),
          Boolean.parseBoolean(matcher.group(6)),
          matcher.group(7));
    }
  }
}
