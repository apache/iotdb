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

package org.apache.iotdb.postgresql;

import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.externalservice.api.IExternalService;
import org.apache.iotdb.postgresql.config.PostgreSqlServiceConfig;
import org.apache.iotdb.postgresql.server.PgServer;
import org.apache.iotdb.postgresql.server.PgServerContext;
import org.apache.iotdb.postgresql.session.SqlProtocol;
import org.apache.iotdb.postgresql.types.TimestampPrecision;
import org.apache.iotdb.rpc.RpcSslUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The PostgreSQL wire protocol as a built-in external service: off by default ({@code
 * enable_postgresql_service}), TLS-only off loopback.
 */
public class PostgreSqlService implements IExternalService {

  private static final Logger LOGGER = LoggerFactory.getLogger(PostgreSqlService.class);

  /** The reported server version: 14, so that clients take their standard paths. */
  static final String SERVER_VERSION = "14.0 (IoTDB " + IoTDBConstant.VERSION + ")";

  private PgServerContext context;
  private PgServer server;
  private ExecutorService workers;

  @Override
  public synchronized void start() {
    PostgreSqlServiceConfig config = PostgreSqlServiceConfig.from(loadProperties());
    if (!config.enabled()) {
      LOGGER.debug(
          "PostgreSQL endpoint is switched off ({}=false)", PostgreSqlServiceConfig.ENABLED);
      return;
    }
    if (!config.problems().isEmpty()) {
      fail("invalid settings: " + config.problems());
      return;
    }
    SSLContext sslContext;
    try {
      sslContext = sslContext(config);
    } catch (IllegalStateException e) {
      fail(e.getMessage());
      return;
    }
    TimestampPrecision precision =
        TimestampPrecision.of(
            org.apache.iotdb.commons.conf.CommonDescriptor.getInstance()
                .getConfig()
                .getTimestampPrecision());
    workers = Executors.newCachedThreadPool(daemonThreads("pg-service-worker"));
    context =
        new PgServerContext(
            config,
            sslContext,
            workers,
            session -> new SqlProtocol(session, config.writeTimeout(), precision),
            SERVER_VERSION);
    server = new PgServer(context);
    try {
      server.start();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      fail("interrupted while opening " + config.address() + ":" + config.port());
    } catch (Exception e) {
      // also checked exceptions Netty rethrows undeclared (BindException: port in use)
      fail(
          "cannot open "
              + config.address()
              + ":"
              + config.port()
              + " ("
              + PostgreSqlServiceConfig.PORT
              + "): "
              + e.getMessage());
    }
  }

  /**
   * The TLS context from the core's key store, or {@code null} when none is configured. Off
   * loopback a key store is required.
   */
  private static SSLContext sslContext(PostgreSqlServiceConfig config) {
    CommonConfig common = CommonDescriptor.getInstance().getConfig();
    String keyStore = common.getKeyStorePath();
    boolean configured = keyStore != null && !keyStore.trim().isEmpty();
    if (!configured) {
      if (!config.isLoopback()) {
        throw new IllegalStateException(
            PostgreSqlServiceConfig.ADDRESS
                + "="
                + config.address()
                + " is not a loopback address and needs TLS, but no key store is configured"
                + " (key_store_path)");
      }
      return null;
    }
    if (!Files.isReadable(Paths.get(keyStore))) {
      throw new IllegalStateException("the key store " + keyStore + " is not readable");
    }
    try {
      return RpcSslUtils.createSSLContext(keyStore, common.getKeyStorePwd(), null, null);
    } catch (Exception e) {
      throw new IllegalStateException(
          "the key store " + keyStore + " cannot be used: " + e.getMessage(), e);
    }
  }

  private void fail(String message) {
    LOGGER.error("PostgreSQL endpoint not started: {}", message);
    stopServer();
  }

  @Override
  public synchronized void stop() {
    stopServer();
  }

  private void stopServer() {
    if (server != null) {
      server.stop();
      server = null;
    }
    if (workers != null) {
      workers.shutdownNow();
      workers = null;
    }
    context = null;
  }

  // ---------------------------------------------------------------- helpers

  private static Map<String, String> loadProperties() {
    Map<String, String> values = new HashMap<>();
    URL url = IoTDBDescriptor.getPropsUrl(CommonConfig.SYSTEM_CONFIG_NAME);
    if (url == null) {
      return values;
    }
    Properties properties = new Properties();
    try (InputStream in = url.openStream()) {
      properties.load(in);
    } catch (IOException e) {
      LOGGER.error("PostgreSQL endpoint: cannot read {}: {}", url, e.toString());
    }
    for (String name : properties.stringPropertyNames()) {
      if (name.startsWith(PostgreSqlServiceConfig.PREFIX)
          || name.equals(PostgreSqlServiceConfig.ENABLED)) {
        values.put(name, properties.getProperty(name));
      }
    }
    return values;
  }

  private static java.util.concurrent.ThreadFactory daemonThreads(String prefix) {
    AtomicInteger counter = new AtomicInteger();
    return runnable -> {
      Thread thread = new Thread(runnable, prefix + "-" + counter.incrementAndGet());
      thread.setDaemon(true);
      return thread;
    };
  }
}
