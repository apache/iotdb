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

import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;

import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.Field;
import org.apache.tsfile.read.common.RowRecord;
import org.apache.tsfile.write.record.Tablet;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.net.SocketFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * A large table with every data type is read completely through pgJDBC (extended protocol, binary
 * results, fetch size) and equals what the session API (the CLI) returns; rows stream; a client
 * that stops reading loses its query after {@code write_timeout}. Rows: {@code -Dpg.import.rows}
 * (default 1,000,000; 10,000,000 for manual runs).
 */
@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class})
public class IoTDBPostgreSqlImportIT {

  private static final int ROWS = Integer.getInteger("pg.import.rows", 1_000_000);
  private static final long START = 1_767_225_600_000L; // 2026-01-01T00:00:00Z
  private static final String QUERY =
      "SELECT time, device, site, b, i, l, f, d, t, s, bl, dt, ts FROM imp.all_types ORDER BY time";

  private static int port;

  @BeforeClass
  public static void setUp() throws Exception {
    try (ServerSocket socket = new ServerSocket(0)) {
      port = socket.getLocalPort();
    }
    EnvFactory.getEnv()
        .getConfig()
        .getDataNodeConfig()
        .setEnablePostgreSqlService(true)
        .setPostgreSqlServicePort(port)
        .setPostgreSqlServiceWriteTimeoutInMs(2000);
    EnvFactory.getEnv().initClusterEnvironment();
    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      session.executeNonQueryStatement("CREATE DATABASE imp");
      session.executeNonQueryStatement("USE imp");
      session.executeNonQueryStatement(
          "CREATE TABLE all_types (device STRING TAG, site STRING ATTRIBUTE, b BOOLEAN FIELD,"
              + " i INT32 FIELD, l INT64 FIELD, f FLOAT FIELD, d DOUBLE FIELD, t TEXT FIELD,"
              + " s STRING FIELD, bl BLOB FIELD, dt DATE FIELD, ts TIMESTAMP FIELD)");
      List<String> names =
          Arrays.asList("device", "site", "b", "i", "l", "f", "d", "t", "s", "bl", "dt", "ts");
      List<TSDataType> types =
          Arrays.asList(
              TSDataType.STRING,
              TSDataType.STRING,
              TSDataType.BOOLEAN,
              TSDataType.INT32,
              TSDataType.INT64,
              TSDataType.FLOAT,
              TSDataType.DOUBLE,
              TSDataType.TEXT,
              TSDataType.STRING,
              TSDataType.BLOB,
              TSDataType.DATE,
              TSDataType.TIMESTAMP);
      List<ColumnCategory> categories =
          Arrays.asList(
              ColumnCategory.TAG,
              ColumnCategory.ATTRIBUTE,
              ColumnCategory.FIELD,
              ColumnCategory.FIELD,
              ColumnCategory.FIELD,
              ColumnCategory.FIELD,
              ColumnCategory.FIELD,
              ColumnCategory.FIELD,
              ColumnCategory.FIELD,
              ColumnCategory.FIELD,
              ColumnCategory.FIELD,
              ColumnCategory.FIELD);
      int batch = 10_000;
      Tablet tablet = new Tablet("all_types", names, types, categories, batch);
      for (int n = 0; n < ROWS; n++) {
        int row = tablet.getRowSize();
        tablet.addTimestamp(row, START + n);
        tablet.addValue(row, 0, "d" + (n % 5));
        tablet.addValue(row, 1, "site" + (n % 5));
        tablet.addValue(row, 2, n % 3 == 0);
        tablet.addValue(row, 3, n * 7);
        tablet.addValue(row, 4, (long) n * 1_000_003L);
        tablet.addValue(row, 5, n / 4.0f);
        tablet.addValue(row, 6, n / 3.0);
        tablet.addValue(row, 7, "text " + n);
        tablet.addValue(row, 8, "s" + (n % 1000));
        tablet.addValue(row, 9, new byte[] {(byte) n, (byte) (n >> 8), 0x7f});
        tablet.addValue(row, 10, LocalDate.of(2026, 1, 1).plusDays(n % 365));
        tablet.addValue(row, 11, START + n * 1000L);
        if (tablet.getRowSize() == batch) {
          session.insert(tablet);
          tablet.reset();
        }
      }
      if (tablet.getRowSize() > 0) {
        session.insert(tablet);
      }
    }
  }

  @AfterClass
  public static void tearDown() {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  private static Properties properties() {
    Properties props = new Properties();
    props.setProperty("user", SessionConfig.DEFAULT_USER);
    props.setProperty("password", SessionConfig.DEFAULT_PASSWORD);
    props.setProperty("sslmode", "disable");
    return props;
  }

  private static String url() {
    return "jdbc:postgresql://127.0.0.1:" + port + "/any";
  }

  /** One row as text, the same for both clients. */
  private static String canonical(
      long time,
      String device,
      String site,
      boolean b,
      int i,
      long l,
      float f,
      double d,
      String t,
      String s,
      byte[] bl,
      LocalDate dt,
      long ts) {
    StringBuilder hex = new StringBuilder();
    for (byte x : bl) {
      hex.append(String.format("%02x", x));
    }
    return time + "|" + device + "|" + site + "|" + b + "|" + i + "|" + l + "|" + f + "|" + d + "|"
        + t + "|" + s + "|" + hex + "|" + dt + "|" + ts;
  }

  @Test
  public void everyRowArrivesAsTheSessionApiReturnsIt() throws Exception {
    long expectedHash = 0;
    long expectedRows = 0;
    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnection();
        SessionDataSet rows = session.executeQueryStatement(QUERY)) {
      rows.setFetchSize(10_000);
      while (rows.hasNext()) {
        RowRecord record = rows.next();
        List<Field> f = record.getFields();
        String line =
            canonical(
                f.get(0).getLongV(),
                f.get(1).getStringValue(),
                f.get(2).getStringValue(),
                f.get(3).getBoolV(),
                f.get(4).getIntV(),
                f.get(5).getLongV(),
                f.get(6).getFloatV(),
                f.get(7).getDoubleV(),
                f.get(8).getStringValue(),
                f.get(9).getStringValue(),
                f.get(10).getBinaryV().getValues(),
                f.get(11).getDateV(),
                f.get(12).getLongV());
        expectedHash = expectedHash * 31 + line.hashCode();
        expectedRows++;
      }
    }
    assertEquals(ROWS, expectedRows);

    Properties props = properties();
    // server-side prepared statement from the first execution: binary results
    props.setProperty("prepareThreshold", "-1");
    props.setProperty("binaryTransfer", "true");
    long actualHash = 0;
    long actualRows = 0;
    long started = System.nanoTime();
    long firstRow = -1;
    try (Connection c = DriverManager.getConnection(url(), props)) {
      c.setAutoCommit(false); // pgJDBC streams with a fetch size only inside a transaction
      try (PreparedStatement p = c.prepareStatement(QUERY)) {
        p.setFetchSize(10_000);
        try (ResultSet rs = p.executeQuery()) {
          while (rs.next()) {
            if (firstRow < 0) {
              firstRow = System.nanoTime() - started;
            }
            String line =
                canonical(
                    rs.getTimestamp(1).getTime(),
                    rs.getString(2),
                    rs.getString(3),
                    rs.getBoolean(4),
                    rs.getInt(5),
                    rs.getLong(6),
                    rs.getFloat(7),
                    rs.getDouble(8),
                    rs.getString(9),
                    rs.getString(10),
                    rs.getBytes(11),
                    rs.getObject(12, LocalDate.class),
                    rs.getTimestamp(13).getTime());
            actualHash = actualHash * 31 + line.hashCode();
            actualRows++;
          }
        }
      }
      c.commit();
    }
    long total = System.nanoTime() - started;
    assertEquals("no row limit (REST has one)", ROWS, actualRows);
    assertEquals("the same rows and values", expectedHash, actualHash);
    assertTrue(
        "rows stream: first after " + firstRow / 1_000_000 + " ms of " + total / 1_000_000,
        firstRow < total / 2);
  }

  @Test
  public void aTimeWindowIsAnsweredFromTheWindow() throws Exception {
    try (Connection c = DriverManager.getConnection(url(), properties());
        PreparedStatement p =
            c.prepareStatement(
                "SELECT count(*), min(i), max(i) FROM imp.all_types"
                    + " WHERE time >= ? AND time < ?")) {
      p.setTimestamp(1, new java.sql.Timestamp(START + 1000));
      p.setTimestamp(2, new java.sql.Timestamp(START + 2000));
      try (ResultSet rs = p.executeQuery()) {
        assertTrue(rs.next());
        assertEquals(1000L, rs.getLong(1));
        assertEquals(7000, rs.getInt(2));
        assertEquals(1999 * 7, rs.getInt(3));
      }
    }
  }

  /** A socket whose reads stop while {@code stalled} is set: a client that stops reading. */
  public static final class StallingSocketFactory extends SocketFactory {
    static volatile boolean stalled;
    static final Object LOCK = new Object();

    @Override
    public Socket createSocket() {
      return new Socket() {
        @Override
        public InputStream getInputStream() throws IOException {
          InputStream in = super.getInputStream();
          return new InputStream() {
            private void await() {
              synchronized (LOCK) {
                while (stalled) {
                  try {
                    LOCK.wait();
                  } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                  }
                }
              }
            }

            @Override
            public int read() throws IOException {
              await();
              return in.read();
            }

            @Override
            public int read(byte[] b, int off, int len) throws IOException {
              await();
              return in.read(b, off, len);
            }

            @Override
            public int available() throws IOException {
              return in.available();
            }

            @Override
            public void close() throws IOException {
              in.close();
            }
          };
        }
      };
    }

    @Override
    public Socket createSocket(String host, int port) throws IOException {
      Socket socket = createSocket();
      socket.connect(new java.net.InetSocketAddress(host, port));
      return socket;
    }

    @Override
    public Socket createSocket(String host, int port, InetAddress local, int localPort)
        throws IOException {
      return createSocket(host, port);
    }

    @Override
    public Socket createSocket(InetAddress host, int port) throws IOException {
      return createSocket(host.getHostAddress(), port);
    }

    @Override
    public Socket createSocket(InetAddress host, int port, InetAddress local, int localPort)
        throws IOException {
      return createSocket(host.getHostAddress(), port);
    }

    static void release() {
      synchronized (LOCK) {
        stalled = false;
        LOCK.notifyAll();
      }
    }
  }

  @Test
  public void aClientThatStopsReadingLosesItsQuery() throws Exception {
    Properties props = properties();
    props.setProperty("socketFactory", StallingSocketFactory.class.getName());
    props.setProperty("preferQueryMode", "simple");
    try (Connection c = DriverManager.getConnection(url(), props);
        Statement s = c.createStatement()) {
      StallingSocketFactory.stalled = true;
      AtomicReference<Throwable> outcome = new AtomicReference<>();
      CountDownLatch done = new CountDownLatch(1);
      Thread reader =
          new Thread(
              () -> {
                try (ResultSet rs = s.executeQuery(QUERY)) {
                  while (rs.next()) {
                    // never reached while stalled
                  }
                } catch (Throwable e) {
                  outcome.set(e);
                } finally {
                  done.countDown();
                }
              });
      reader.start();
      // the server fills the socket buffers, waits write_timeout (2 s) and closes the connection
      Thread.sleep(8_000);
      StallingSocketFactory.release();
      assertTrue("the query ended", done.await(60, TimeUnit.SECONDS));
      assertNotNull("the query failed", outcome.get());
      String message = String.valueOf(outcome.get().getMessage());
      assertTrue(
          message,
          (outcome.get() instanceof SQLException
                  && "57P01".equals(((SQLException) outcome.get()).getSQLState()))
              || message.toLowerCase(java.util.Locale.ROOT).contains("connection")
              || message.contains("I/O"));
    } finally {
      StallingSocketFactory.release();
    }
    // the server is fine
    try (Connection c = DriverManager.getConnection(url(), properties());
        Statement s = c.createStatement();
        ResultSet rs = s.executeQuery("SELECT count(*) FROM imp.all_types")) {
      assertTrue(rs.next());
      assertEquals(ROWS, rs.getLong(1));
    }
  }
}
