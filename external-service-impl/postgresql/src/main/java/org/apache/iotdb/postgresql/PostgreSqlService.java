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
import org.apache.iotdb.postgresql.catalog.PgCatalogExtension;
import org.apache.iotdb.postgresql.config.PostgreSqlServiceConfig;
import org.apache.iotdb.postgresql.i18n.PostgreSqlMessages;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The PostgreSQL wire protocol as a built-in external service: off by default ({@code
 * enable_postgresql_service}), TLS-only off loopback.
 */
public class PostgreSqlService implements IExternalService {

  private static final Logger LOGGER = LoggerFactory.getLogger(PostgreSqlService.class);

  /** The reported server version: 14, so that clients take their standard paths. */
  static final String SERVER_VERSION = "14.0 (IoTDB " + IoTDBConstant.VERSION + ")";

  /** How long a stop waits for the open connections' queries and sessions to be released. */
  private static final long RELEASE_WAIT_SECONDS = 10;

  private PgServerContext context;
  private PgServer server;
  private ExecutorService workers;

  @Override
  public synchronized void start() {
    // the core decides at DataNode start whether to start the service
    // (enable_postgresql_service); an explicit START SERVICE starts it in any case
    PostgreSqlServiceConfig config = PostgreSqlServiceConfig.from(loadProperties());
    if (!config.problems().isEmpty()) {
      throw fail(
          String.format(
              PostgreSqlMessages.EXCEPTION_INVALID_SETTINGS_ARG_4D6B78F5, config.problems()));
    }
    SSLContext sslContext;
    try {
      sslContext = sslContext(config);
    } catch (IllegalStateException e) {
      throw fail(e.getMessage());
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
      PgCatalogExtension.setListed(true);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw fail(
          String.format(
              PostgreSqlMessages.EXCEPTION_INTERRUPTED_WHILE_OPENING_ARG_ARG_F80AAF97,
              config.address(),
              config.port()));
    } catch (Exception e) {
      // also checked exceptions Netty rethrows undeclared (BindException: port in use)
      throw fail(
          String.format(
              PostgreSqlMessages.EXCEPTION_CANNOT_OPEN_ARG_ARG_ARG_ARG_63428D45,
              config.address(),
              config.port(),
              PostgreSqlServiceConfig.PORT,
              e.getMessage()));
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
            String.format(
                PostgreSqlMessages
                    .EXCEPTION_ARG_ARG_IS_NOT_A_LOOPBACK_ADDRESS_AND_NEEDS_TLS_BUT_NO_KEY_STORE_IS_CONFIGURED_KEY_STORE_PATH_723B55BF,
                PostgreSqlServiceConfig.ADDRESS,
                config.address()));
      }
      return null;
    }
    if (!Files.isReadable(Paths.get(keyStore))) {
      throw new IllegalStateException(
          String.format(
              PostgreSqlMessages.EXCEPTION_THE_KEY_STORE_ARG_IS_NOT_READABLE_A4A4A28C, keyStore));
    }
    try {
      return RpcSslUtils.createSSLContext(keyStore, common.getKeyStorePwd(), null, null);
    } catch (Exception e) {
      throw new IllegalStateException(
          String.format(
              PostgreSqlMessages.EXCEPTION_THE_KEY_STORE_ARG_CANNOT_BE_USED_ARG_5908F0BB,
              keyStore,
              e.getMessage()),
          e);
    }
  }

  /**
   * Logs a failed start and returns the exception {@link #start()} throws, so that the service does
   * not count as running (START SERVICE reports the error; at DataNode start the core logs it and
   * keeps the service stopped).
   */
  private IllegalStateException fail(String message) {
    LOGGER.error(PostgreSqlMessages.LOG_POSTGRESQL_ENDPOINT_NOT_STARTED_ARG_626B180C, message);
    stopServer();
    return new IllegalStateException(
        String.format(
            PostgreSqlMessages.EXCEPTION_POSTGRESQL_ENDPOINT_NOT_STARTED_ARG_A5563154, message));
  }

  @Override
  public synchronized void stop() {
    stopServer();
  }

  private void stopServer() {
    PgCatalogExtension.setListed(false);
    if (server != null) {
      server.stop();
      server = null;
    }
    if (workers != null) {
      // the connections' releases are queued now (server.stop() waited for the event loops);
      // let them finish, then stop what is left
      workers.shutdown();
      try {
        if (!workers.awaitTermination(RELEASE_WAIT_SECONDS, TimeUnit.SECONDS)) {
          workers.shutdownNow();
        }
      } catch (InterruptedException e) {
        workers.shutdownNow();
        Thread.currentThread().interrupt();
      }
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
      LOGGER.error(
          PostgreSqlMessages.LOG_POSTGRESQL_ENDPOINT_CANNOT_READ_ARG_ARG_B4423CA2,
          url,
          e.toString());
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
