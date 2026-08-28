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
import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.auth.entity.User;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.cluster.node.ConfigNodeWrapper;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.itbase.env.BaseEnv;
import org.apache.iotdb.jdbc.Config;

import org.bouncycastle.asn1.x509.BasicConstraints;
import org.bouncycastle.asn1.x509.ExtendedKeyUsage;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.KeyPurposeId;
import org.bouncycastle.asn1.x509.KeyUsage;
import org.bouncycastle.x509.X509V3CertificateGenerator;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManagerFactory;
import javax.security.auth.x500.X500Principal;

import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.security.cert.CertificateExpiredException;
import java.security.cert.X509Certificate;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class})
public class TimechoDBTrustedChannelFailureAuditLogIT {

  private static final String STORE_PASSWORD = "thrift";
  private static final long POLL_TIMEOUT_MS = 30_000;
  private static final long POLL_INTERVAL_MS = 500;

  @Rule public final TestName testName = new TestName();

  private String keyDir;
  private Path augmentedTrustStorePath;
  private SSLContext expiredClientSslContext;
  private X509Certificate expiredClientCertificate;

  @Before
  public void setUp() throws Exception {
    keyDir =
        System.getProperty("user.dir")
            + File.separator
            + "target"
            + File.separator
            + "test-classes"
            + File.separator;
    createExpiredClientCertificateMaterial();

    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setTimestampPrecision("ns")
        .setDataRegionConsensusProtocolClass(dataRegionConsensusProtocol())
        .setEnableThriftClientSSL(true)
        .setEnableInternalSSL(true)
        .setKeyStorePath(keyStorePath())
        .setKeyStorePwd(STORE_PASSWORD)
        .setTrustStorePath(trustStorePath())
        .setTrustStorePwd(STORE_PASSWORD)
        .setSslProtocol(SessionConfig.DEFAULT_SSL_PROTOCOL)
        .setEnableAuditLog(true)
        .setAuditableOperationType(AuditLogOperation.CONTROL.toString())
        .setAuditableOperationLevel(PrivilegeLevel.GLOBAL.toString())
        .setAuditableOperationResult("FAIL");
    configureTestProcessInternalSsl();
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @After
  public void tearDown() throws Exception {
    try {
      EnvFactory.getEnv().cleanClusterEnvironment();
    } finally {
      resetTestProcessInternalSsl();
      if (augmentedTrustStorePath != null) {
        Files.deleteIfExists(augmentedTrustStorePath);
      }
    }
  }

  @Test
  public void testTlsHandshakeFailuresArePersistedForNonRatisServices() throws Exception {
    final ConfigNodeWrapper configNode = EnvFactory.getEnv().getConfigNodeWrapper(0);
    final DataNodeWrapper dataNode = EnvFactory.getEnv().getDataNodeWrapper(0);
    final Map<String, ServiceEndpoint> services = new LinkedHashMap<>();
    services.put(
        "ConfigNode internal RPC", new ServiceEndpoint(configNode.getIp(), configNode.getPort()));
    services.put("DataNode client RPC", new ServiceEndpoint(dataNode.getIp(), dataNode.getPort()));
    services.put(
        "DataNode internal RPC", new ServiceEndpoint(dataNode.getIp(), dataNode.getInternalPort()));
    services.put(
        "MPP data exchange RPC",
        new ServiceEndpoint(dataNode.getIp(), dataNode.getMppDataExchangePort()));
    services.put(
        "IoTConsensus data-region RPC",
        new ServiceEndpoint(dataNode.getIp(), dataNode.getDataRegionConsensusPort()));
    triggerFailuresAndAssertAuditRecords(dataNode, services);
  }

  @Test
  public void testIoTConsensusV2TlsHandshakeFailureIsPersisted() throws Exception {
    final DataNodeWrapper dataNode = EnvFactory.getEnv().getDataNodeWrapper(0);
    final Map<String, ServiceEndpoint> services = new LinkedHashMap<>();
    services.put(
        "IoTConsensusV2 data-region RPC",
        new ServiceEndpoint(dataNode.getIp(), dataNode.getDataRegionConsensusPort()));
    triggerFailuresAndAssertAuditRecords(dataNode, services);
  }

  @Test
  public void testExpiredClientCertificateFailureIsPersisted() throws Exception {
    final DataNodeWrapper dataNode = EnvFactory.getEnv().getDataNodeWrapper(0);
    final ServiceEndpoint endpoint =
        new ServiceEndpoint(dataNode.getIp(), dataNode.getInternalPort());
    final Map<String, ServiceEndpoint> services = new LinkedHashMap<>();
    services.put("DataNode internal RPC with expired client certificate", endpoint);

    try (Connection connection = newSslTableConnection(dataNode);
        Statement statement = connection.createStatement()) {
      sendExpiredClientCertificate(endpoint);
      assertAndPrintAuditRecords(statement, services);
    }
  }

  private void triggerFailuresAndAssertAuditRecords(
      DataNodeWrapper dataNode, Map<String, ServiceEndpoint> services) throws Exception {
    try (Connection connection = newSslTableConnection(dataNode);
        Statement statement = connection.createStatement()) {
      for (ServiceEndpoint endpoint : services.values()) {
        sendNonTlsPayload(endpoint);
      }
      assertAndPrintAuditRecords(statement, services);
    }
  }

  private static void assertAndPrintAuditRecords(
      Statement statement, Map<String, ServiceEndpoint> services) throws Exception {
    final List<AuditRecord> records = waitForTrustedChannelFailures(statement, services);
    for (Map.Entry<String, ServiceEndpoint> service : services.entrySet()) {
      final AuditRecord record = findRecord(records, service.getValue());
      assertAuditRecord(record, service.getValue());
      printAuditRecord(service.getKey(), record);
    }
  }

  private String dataRegionConsensusProtocol() {
    return testName.getMethodName().contains("IoTConsensusV2")
        ? ConsensusFactory.IOT_CONSENSUS_V2
        : ConsensusFactory.IOT_CONSENSUS;
  }

  private static void sendNonTlsPayload(ServiceEndpoint endpoint) throws Exception {
    try (Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress(endpoint.host, endpoint.port));
      socket.getOutputStream().write("not-a-tls-client".getBytes(StandardCharsets.UTF_8));
      socket.getOutputStream().flush();
    }
  }

  private void sendExpiredClientCertificate(ServiceEndpoint endpoint) throws Exception {
    try (SSLSocket socket = (SSLSocket) expiredClientSslContext.getSocketFactory().createSocket()) {
      socket.connect(new InetSocketAddress(endpoint.host, endpoint.port));
      socket.setSoTimeout((int) TimeUnit.SECONDS.toMillis(5));
      try {
        socket.startHandshake();
        socket.getInputStream().read();
        Assert.fail("TLS connection unexpectedly accepted an expired client certificate");
      } catch (SSLException expected) {
        System.out.printf(
            "REAL_TLS_FAILURE service=DataNode internal RPC | certificate_not_after=%s | "
                + "client_exception=%s%n",
            expiredClientCertificate.getNotAfter(), exceptionMessages(expected));
      }
    }
  }

  private static String exceptionMessages(Throwable failure) {
    final List<String> messages = new ArrayList<>();
    Throwable current = failure;
    while (current != null) {
      messages.add(current.getClass().getSimpleName() + ": " + current.getMessage());
      current = current.getCause();
    }
    return String.join(" -> ", messages);
  }

  private static List<AuditRecord> waitForTrustedChannelFailures(
      Statement statement, Map<String, ServiceEndpoint> services) throws Exception {
    final long deadline = System.currentTimeMillis() + POLL_TIMEOUT_MS;
    List<AuditRecord> lastRecords = Collections.emptyList();
    while (System.currentTimeMillis() < deadline) {
      try {
        final List<AuditRecord> records = queryTrustedChannelFailures(statement);
        lastRecords = records;
        if (allServicesRecorded(records, services)) {
          return records;
        }
      } catch (SQLException ignored) {
        // The audit database and view are created lazily by the first audit event.
      }
      TimeUnit.MILLISECONDS.sleep(POLL_INTERVAL_MS);
    }
    for (AuditRecord record : lastRecords) {
      printAuditRecord("observed before timeout", record);
    }
    Assert.fail(
        "Timed out waiting for TRUSTED_CHANNEL_FUNCTION_FAILURE audit events from "
            + missingServices(lastRecords, services));
    return Collections.emptyList();
  }

  private static List<AuditRecord> queryTrustedChannelFailures(Statement statement)
      throws SQLException {
    final List<AuditRecord> records = new ArrayList<>();
    try (ResultSet resultSet =
        statement.executeQuery(
            "SELECT time, username, cli_hostname, audit_event_type, operation_type,"
                + " privilege_type, result, log FROM __audit.audit_log"
                + " WHERE audit_event_type = 'TRUSTED_CHANNEL_FUNCTION_FAILURE'"
                + " ORDER BY time DESC")) {
      while (resultSet.next()) {
        records.add(
            new AuditRecord(
                resultSet.getString("time"),
                resultSet.getString("username"),
                resultSet.getString("cli_hostname"),
                resultSet.getString("audit_event_type"),
                resultSet.getString("operation_type"),
                resultSet.getString("privilege_type"),
                resultSet.getBoolean("result"),
                resultSet.getString("log")));
      }
    }
    return records;
  }

  private static boolean allServicesRecorded(
      List<AuditRecord> records, Map<String, ServiceEndpoint> services) {
    for (ServiceEndpoint endpoint : services.values()) {
      if (findRecordOrNull(records, endpoint) == null) {
        return false;
      }
    }
    return true;
  }

  private static List<String> missingServices(
      List<AuditRecord> records, Map<String, ServiceEndpoint> services) {
    final List<String> missing = new ArrayList<>();
    for (Map.Entry<String, ServiceEndpoint> service : services.entrySet()) {
      if (findRecordOrNull(records, service.getValue()) == null) {
        missing.add(service.getKey() + " (" + service.getValue().targetIdentifier() + ")");
      }
    }
    return missing;
  }

  private static AuditRecord findRecord(List<AuditRecord> records, ServiceEndpoint endpoint) {
    final AuditRecord record = findRecordOrNull(records, endpoint);
    Assert.assertNotNull("Missing audit record for target " + endpoint.targetIdentifier(), record);
    return record;
  }

  private static AuditRecord findRecordOrNull(List<AuditRecord> records, ServiceEndpoint endpoint) {
    for (AuditRecord record : records) {
      if (endpoint.matchesTarget(record.log)) {
        return record;
      }
    }
    return null;
  }

  private static void assertAuditRecord(AuditRecord record, ServiceEndpoint endpoint) {
    Assert.assertEquals(User.BUILTIN_INTERNAL_AUDIT_LOG_USERNAME, record.username);
    Assert.assertFalse(record.cliHostname.isEmpty());
    Assert.assertEquals(
        AuditEventType.TRUSTED_CHANNEL_FUNCTION_FAILURE.toString(), record.auditEventType);
    Assert.assertEquals(AuditLogOperation.CONTROL.toString(), record.operationType);
    Assert.assertEquals(
        Collections.singletonList(PrivilegeType.SECURITY).toString(), record.privilegeType);
    Assert.assertFalse(record.result);
    Assert.assertTrue(record.log.contains("Trusted channel function failed"));
    Assert.assertTrue(endpoint.matchesTarget(record.log));
  }

  private static void printAuditRecord(String service, AuditRecord record) {
    System.out.printf(
        "REAL_AUDIT_LOG service=%s | time=%s | username=%s | cli_hostname=%s | "
            + "audit_event_type=%s | operation_type=%s | privilege_type=%s | result=%s | log=%s%n",
        service,
        record.time,
        record.username,
        record.cliHostname,
        record.auditEventType,
        record.operationType,
        record.privilegeType,
        record.result,
        record.log);
  }

  private Connection newSslTableConnection(DataNodeWrapper dataNode) throws SQLException {
    final Properties properties = new Properties();
    properties.put("user", SessionConfig.DEFAULT_USER);
    properties.put("password", SessionConfig.DEFAULT_PASSWORD);
    properties.put(Config.USE_SSL, Boolean.TRUE.toString());
    properties.put(Config.TRUST_STORE, trustStorePath());
    properties.put(Config.TRUST_STORE_PWD, STORE_PASSWORD);
    properties.put(Config.SQL_DIALECT, BaseEnv.TABLE_SQL_DIALECT);
    return DriverManager.getConnection(
        Config.IOTDB_URL_PREFIX + dataNode.getIpAndPortString(), properties);
  }

  private String keyStorePath() {
    return keyDir + "test-keystore";
  }

  private String trustStorePath() {
    return augmentedTrustStorePath.toString();
  }

  private String baseTrustStorePath() {
    return keyDir + "test-truststore";
  }

  private void createExpiredClientCertificateMaterial() throws Exception {
    final KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("RSA");
    keyPairGenerator.initialize(2048);
    final KeyPair caKeyPair = keyPairGenerator.generateKeyPair();
    final X500Principal caPrincipal = new X500Principal("CN=Trusted Channel Audit Test CA");
    final long now = System.currentTimeMillis();
    final X509Certificate caCertificate =
        generateCertificate(
            caPrincipal,
            caPrincipal,
            caKeyPair,
            caKeyPair.getPrivate(),
            new Date(now - TimeUnit.DAYS.toMillis(30)),
            new Date(now + TimeUnit.DAYS.toMillis(365)),
            true);

    final KeyPair clientKeyPair = keyPairGenerator.generateKeyPair();
    expiredClientCertificate =
        generateCertificate(
            new X500Principal("CN=Expired Trusted Channel Audit Client"),
            caPrincipal,
            clientKeyPair,
            caKeyPair.getPrivate(),
            new Date(now - TimeUnit.DAYS.toMillis(2)),
            new Date(now - TimeUnit.DAYS.toMillis(1)),
            false);
    expiredClientCertificate.verify(caKeyPair.getPublic());
    try {
      expiredClientCertificate.checkValidity();
      Assert.fail("Generated client certificate must already be expired");
    } catch (CertificateExpiredException expected) {
      // Expected: the handshake below must fail specifically on certificate validity.
    }

    final KeyStore serverTrustStore = loadKeyStore(baseTrustStorePath());
    serverTrustStore.setCertificateEntry("trusted-channel-audit-test-ca", caCertificate);
    final Path targetDirectory = Paths.get(System.getProperty("user.dir"), "target");
    Files.createDirectories(targetDirectory);
    augmentedTrustStorePath =
        Files.createTempFile(targetDirectory, "trusted-channel-audit-truststore-", ".p12");
    try (OutputStream output = Files.newOutputStream(augmentedTrustStorePath)) {
      serverTrustStore.store(output, STORE_PASSWORD.toCharArray());
    }

    final KeyStore clientKeyStore = KeyStore.getInstance(KeyStore.getDefaultType());
    clientKeyStore.load(null, STORE_PASSWORD.toCharArray());
    clientKeyStore.setKeyEntry(
        "expired-client",
        clientKeyPair.getPrivate(),
        STORE_PASSWORD.toCharArray(),
        new Certificate[] {expiredClientCertificate, caCertificate});
    final KeyManagerFactory keyManagerFactory =
        KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
    keyManagerFactory.init(clientKeyStore, STORE_PASSWORD.toCharArray());

    final KeyStore clientTrustStore = loadKeyStore(baseTrustStorePath());
    final TrustManagerFactory trustManagerFactory =
        TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
    trustManagerFactory.init(clientTrustStore);

    expiredClientSslContext = SSLContext.getInstance(SessionConfig.DEFAULT_SSL_PROTOCOL);
    expiredClientSslContext.init(
        keyManagerFactory.getKeyManagers(), trustManagerFactory.getTrustManagers(), null);
  }

  private static KeyStore loadKeyStore(String path) throws Exception {
    final KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
    try (InputStream input = Files.newInputStream(Paths.get(path))) {
      keyStore.load(input, STORE_PASSWORD.toCharArray());
    }
    return keyStore;
  }

  @SuppressWarnings("deprecation")
  private static X509Certificate generateCertificate(
      X500Principal subject,
      X500Principal issuer,
      KeyPair subjectKeyPair,
      PrivateKey issuerPrivateKey,
      Date notBefore,
      Date notAfter,
      boolean certificateAuthority)
      throws Exception {
    final X509V3CertificateGenerator generator = new X509V3CertificateGenerator();
    generator.setSerialNumber(BigInteger.valueOf(System.nanoTime()).abs());
    generator.setIssuerDN(issuer);
    generator.setNotBefore(notBefore);
    generator.setNotAfter(notAfter);
    generator.setSubjectDN(subject);
    generator.setPublicKey(subjectKeyPair.getPublic());
    generator.setSignatureAlgorithm("SHA256WithRSA");
    generator.addExtension(
        Extension.basicConstraints, true, new BasicConstraints(certificateAuthority));
    generator.addExtension(
        Extension.keyUsage,
        true,
        certificateAuthority
            ? new KeyUsage(KeyUsage.keyCertSign | KeyUsage.cRLSign)
            : new KeyUsage(KeyUsage.digitalSignature | KeyUsage.keyEncipherment));
    if (!certificateAuthority) {
      generator.addExtension(
          Extension.extendedKeyUsage, false, new ExtendedKeyUsage(KeyPurposeId.id_kp_clientAuth));
    }
    return generator.generate(issuerPrivateKey);
  }

  private void configureTestProcessInternalSsl() {
    CommonDescriptor.getInstance().getConfig().setEnableInternalSSL(true);
    CommonDescriptor.getInstance().getConfig().setKeyStorePath(keyStorePath());
    CommonDescriptor.getInstance().getConfig().setKeyStorePwd(STORE_PASSWORD);
    CommonDescriptor.getInstance().getConfig().setTrustStorePath(trustStorePath());
    CommonDescriptor.getInstance().getConfig().setTrustStorePwd(STORE_PASSWORD);
    CommonDescriptor.getInstance().getConfig().setSslProtocol(SessionConfig.DEFAULT_SSL_PROTOCOL);
  }

  private static void resetTestProcessInternalSsl() {
    CommonDescriptor.getInstance().getConfig().setEnableInternalSSL(false);
    CommonDescriptor.getInstance().getConfig().setKeyStorePath("");
    CommonDescriptor.getInstance().getConfig().setKeyStorePwd("");
    CommonDescriptor.getInstance().getConfig().setTrustStorePath("");
    CommonDescriptor.getInstance().getConfig().setTrustStorePwd("");
    CommonDescriptor.getInstance().getConfig().setSslProtocol(SessionConfig.DEFAULT_SSL_PROTOCOL);
  }

  private static class ServiceEndpoint {
    private final String host;
    private final int port;

    private ServiceEndpoint(String host, int port) {
      this.host = host;
      this.port = port;
    }

    private String targetIdentifier() {
      return host + ":" + port;
    }

    private boolean matchesTarget(String log) {
      return log.contains("target=") && log.endsWith(":" + port);
    }
  }

  private static class AuditRecord {
    private final String time;
    private final String username;
    private final String cliHostname;
    private final String auditEventType;
    private final String operationType;
    private final String privilegeType;
    private final boolean result;
    private final String log;

    private AuditRecord(
        String time,
        String username,
        String cliHostname,
        String auditEventType,
        String operationType,
        String privilegeType,
        boolean result,
        String log) {
      this.time = time;
      this.username = username;
      this.cliHostname = cliHostname;
      this.auditEventType = auditEventType;
      this.operationType = operationType;
      this.privilegeType = privilegeType;
      this.result = result;
      this.log = log;
    }
  }
}
