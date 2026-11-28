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

package org.apache.iotdb.postgresql.it;

import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import java.io.DataOutputStream;
import java.io.File;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.security.cert.X509Certificate;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * With a key store and a non-loopback bind the endpoint is TLS-only (TLS 1.2/1.3, AEAD suites);
 * logins are recorded with the protocol.
 */
@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class})
public class IoTDBPostgreSqlTlsIT {

  private static final String STORE_PASSWORD = "PgTls-Store-2026";
  private static int port;

  @BeforeClass
  public static void setUp() throws Exception {
    try (ServerSocket socket = new ServerSocket(0)) {
      port = socket.getLocalPort();
    }
    File store = Files.createTempDirectory("pg-tls").resolve("server.p12").toFile();
    Process keytool =
        new ProcessBuilder(
                System.getProperty("java.home")
                    + File.separator
                    + "bin"
                    + File.separator
                    + "keytool",
                "-genkeypair",
                "-alias",
                "pg",
                "-keyalg",
                "RSA",
                "-keysize",
                "2048",
                "-validity",
                "30",
                "-dname",
                "CN=localhost",
                "-ext",
                "SAN=dns:localhost,ip:127.0.0.1",
                "-storetype",
                "PKCS12",
                "-keystore",
                store.getAbsolutePath(),
                "-storepass",
                STORE_PASSWORD,
                "-keypass",
                STORE_PASSWORD)
            .redirectErrorStream(true)
            .start();
    assertTrue(keytool.waitFor(60, TimeUnit.SECONDS));
    assertEquals(0, keytool.exitValue());
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setKeyStorePath(store.getAbsolutePath())
        .setKeyStorePwd(STORE_PASSWORD);
    EnvFactory.getEnv()
        .getConfig()
        .getDataNodeConfig()
        .setEnablePostgreSqlService(true)
        .setPostgreSqlServiceAddress("0.0.0.0")
        .setPostgreSqlServicePort(port);
    EnvFactory.getEnv().initClusterEnvironment();
    PgItSupport.awaitListening(port);
  }

  @AfterClass
  public static void tearDown() {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  private static Connection connect(boolean tls) throws SQLException {
    Properties props = new Properties();
    props.setProperty("user", SessionConfig.DEFAULT_USER);
    props.setProperty("password", SessionConfig.DEFAULT_PASSWORD);
    props.setProperty("sslmode", tls ? "require" : "disable");
    if (tls) {
      props.setProperty("sslfactory", "org.postgresql.ssl.NonValidatingFactory");
    }
    return DriverManager.getConnection("jdbc:postgresql://127.0.0.1:" + port + "/any", props);
  }

  @Test
  public void plainConnectionsAreRefused() {
    try (Connection ignored = connect(false)) {
      fail("off loopback the endpoint is TLS-only");
    } catch (SQLException e) {
      assertEquals(e.getMessage(), "28000", e.getSQLState());
      assertTrue(e.getMessage(), e.getMessage().contains("encryption required"));
    }
  }

  @Test
  public void tlsLoginWorksAndIsRecorded() throws Exception {
    try (Connection c = connect(true);
        Statement s = c.createStatement();
        ResultSet rs = s.executeQuery("SELECT current_user")) {
      assertTrue(rs.next());
      assertEquals(SessionConfig.DEFAULT_USER, rs.getString(1));
    }
    assertTrue(PgItSupport.dataNodeLog().contains(":postgresql:127.0.0.1:"));
  }

  @Test
  public void onlyModernTlsWithAeadSuites() throws Exception {
    SSLContext tls = SSLContext.getInstance("TLS");
    tls.init(
        null,
        new TrustManager[] {
          new X509TrustManager() {
            @Override
            public void checkClientTrusted(X509Certificate[] chain, String authType) {}

            @Override
            public void checkServerTrusted(X509Certificate[] chain, String authType) {}

            @Override
            public X509Certificate[] getAcceptedIssuers() {
              return new X509Certificate[0];
            }
          }
        },
        null);
    for (String[] protocols : new String[][] {{"TLSv1.3"}, {"TLSv1.2"}, {"TLSv1.2", "TLSv1.3"}}) {
      try (Socket plain = new Socket("127.0.0.1", port)) {
        DataOutputStream out = new DataOutputStream(plain.getOutputStream());
        out.writeInt(8);
        out.writeInt(80877103); // SSLRequest
        out.flush();
        assertEquals('S', plain.getInputStream().read());
        try (SSLSocket secure =
            (SSLSocket) tls.getSocketFactory().createSocket(plain, "127.0.0.1", port, false)) {
          secure.setEnabledProtocols(protocols);
          secure.startHandshake();
          String suite = secure.getSession().getCipherSuite();
          assertTrue(suite, suite.contains("GCM") || suite.contains("CHACHA20"));
        }
      }
    }
    // TLS 1.1 is refused
    try (Socket plain = new Socket("127.0.0.1", port)) {
      DataOutputStream out = new DataOutputStream(plain.getOutputStream());
      out.writeInt(8);
      out.writeInt(80877103);
      out.flush();
      assertEquals('S', plain.getInputStream().read());
      try (SSLSocket secure =
          (SSLSocket) tls.getSocketFactory().createSocket(plain, "127.0.0.1", port, false)) {
        secure.setEnabledProtocols(new String[] {"TLSv1.1"});
        secure.startHandshake();
        fail("TLS 1.1 must not be accepted");
      } catch (javax.net.ssl.SSLException expected) {
        // refused (or not even offered by this JVM)
      }
    }
  }
}
