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

package org.apache.iotdb.db.it;

import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.session.Session;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.Field;
import org.apache.tsfile.read.common.RowRecord;
import org.apache.tsfile.utils.Binary;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * End-to-end coverage of fragmented query results through a real DataNode: the payload bound is set
 * to its minimum so that a single oversized row is split into several responses. Runs on a single
 * node (local result path) and on a cluster (remote result path).
 */
@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBSessionResultChunkingIT {

  private static final int PAYLOAD_SIZE_IN_BYTES = 128 * 1024;
  private static final int LARGE_VALUE_SIZE = 300 * 1024;

  private static final byte[] LARGE_BLOB = createBlob();
  private static final String LARGE_TEXT = createText();
  private static final byte[] SMALL_BLOB = "small".getBytes(TSFileConfig.STRING_CHARSET);

  private static Session session;
  private static String ip;
  private static int port;

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv()
        .getConfig()
        .getDataNodeConfig()
        .setMppDataExchangeMaxPayloadSizeInBytes(PAYLOAD_SIZE_IN_BYTES);
    EnvFactory.getEnv().initClusterEnvironment();
    ip = EnvFactory.getEnv().getIP();
    port = Integer.parseInt(EnvFactory.getEnv().getPort());

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE root.chunk_it");
      statement.execute("CREATE TIMESERIES root.chunk_it.d1.blob BLOB ENCODING=PLAIN");
      // a dedicated database holding only text, so that the prefix-path last query returns a
      // single oversized value
      statement.execute("CREATE DATABASE root.chunk_text_it");
      statement.execute("CREATE TIMESERIES root.chunk_text_it.d1.t TEXT ENCODING=PLAIN");
    }

    session = new Session(ip, port);
    session.open();
  }

  @AfterClass
  public static void tearDown() {
    if (session != null) {
      try {
        session.close();
      } catch (Exception ignored) {
        // best effort
      }
    }
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testOversizedRowThroughSessionAndJdbc() throws Exception {
    assertEquals(
        "a value larger than the payload bound is required", LARGE_VALUE_SIZE, LARGE_BLOB.length);
    session.insertRecord(
        "root.chunk_it.d1",
        1_000_000L,
        List.of("blob"),
        List.of(TSDataType.BLOB),
        new Binary(LARGE_BLOB));

    SessionDataSet dataSet =
        session.executeQueryStatement("SELECT blob FROM root.chunk_it.d1 WHERE time = 1000000");
    try {
      assertTrue(dataSet.hasNext());
      assertArrayEquals(LARGE_BLOB, dataSet.next().getFields().get(0).getBinaryV().getValues());
      assertFalse(dataSet.hasNext());
    } finally {
      dataSet.closeOperationHandle();
    }

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet =
            statement.executeQuery("SELECT blob FROM root.chunk_it.d1 WHERE time = 1000000")) {
      assertTrue(resultSet.next());
      // in the tree model the first JDBC column is Time
      assertArrayEquals(LARGE_BLOB, resultSet.getBytes("root.chunk_it.d1.blob"));
      assertFalse(resultSet.next());
    }
  }

  @Test
  public void testMixedRowsWithSmallFetchSize() throws Exception {
    for (int time = 1; time <= 3; time++) {
      session.insertRecord(
          "root.chunk_it.d1",
          time,
          List.of("blob"),
          List.of(TSDataType.BLOB),
          new Binary(SMALL_BLOB));
    }
    session.insertRecord(
        "root.chunk_it.d1", 4L, List.of("blob"), List.of(TSDataType.BLOB), new Binary(LARGE_BLOB));
    session.insertRecord(
        "root.chunk_it.d1", 5L, List.of("blob"), List.of(TSDataType.BLOB), new Binary(SMALL_BLOB));

    Session smallFetchSession = new Session(ip, port);
    smallFetchSession.open();
    smallFetchSession.setFetchSize(1);
    try {
      SessionDataSet dataSet =
          smallFetchSession.executeQueryStatement(
              "SELECT blob FROM root.chunk_it.d1 WHERE time >= 1 AND time <= 5");
      try {
        for (int time = 1; time <= 5; time++) {
          assertTrue("missing row " + time, dataSet.hasNext());
          RowRecord row = dataSet.next();
          assertEquals(time, row.getTimestamp());
          assertArrayEquals(
              time == 4 ? LARGE_BLOB : SMALL_BLOB, row.getFields().get(0).getBinaryV().getValues());
        }
        assertFalse(dataSet.hasNext());
      } finally {
        dataSet.closeOperationHandle();
      }
    } finally {
      smallFetchSession.close();
    }
  }

  @Test
  public void testBothFastLastRpcsOnCachedOversizedValue() throws Exception {
    session.insertRecord(
        "root.chunk_text_it.d1",
        2_000_000L,
        List.of("t"),
        List.of(TSDataType.TEXT),
        new Binary(LARGE_TEXT, TSFileConfig.STRING_CHARSET));

    // one-device RPC: the second call hits the last cache, i.e. the materialized result path
    for (int i = 0; i < 2; i++) {
      SessionDataSet dataSet =
          session.executeLastDataQueryForOneDevice(
              "root.chunk_text_it", "root.chunk_text_it.d1", List.of("t"), true);
      try {
        assertEquals(LARGE_TEXT, lastTextValue(dataSet));
      } finally {
        dataSet.closeOperationHandle();
      }
    }

    // one-prefix-path RPC: it is served from the last cache of the connected DataNode without
    // redirection, so query every DataNode; the one owning the device must return the full value
    boolean served = false;
    for (DataNodeWrapper dataNode : EnvFactory.getEnv().getDataNodeWrapperList()) {
      try (Session nodeSession = new Session(dataNode.getIp(), dataNode.getPort())) {
        nodeSession.open();
        for (int i = 0; i < 2; i++) {
          SessionDataSet dataSet =
              nodeSession.executeFastLastDataQueryForOnePrefixPath(
                  List.of("root", "chunk_text_it"));
          try {
            String value = lastTextValue(dataSet);
            if (LARGE_TEXT.equals(value)) {
              served = true;
            }
          } finally {
            dataSet.closeOperationHandle();
          }
        }
      }
    }
    assertTrue("no DataNode served the cached oversized value", served);
  }

  @Test
  public void testCloseWhileFragmentIsPending() throws Exception {
    session.insertRecord(
        "root.chunk_it.d1",
        3_000_000L,
        List.of("blob"),
        List.of(TSDataType.BLOB),
        new Binary(LARGE_BLOB));

    // the execute response carries the first fragment only; abandon it without fetching the rest
    for (int i = 0; i < 3; i++) {
      SessionDataSet dataSet =
          session.executeQueryStatement("SELECT blob FROM root.chunk_it.d1 WHERE time = 3000000");
      assertTrue(dataSet.hasNext());
      dataSet.closeOperationHandle();
    }

    // the node still serves the complete result
    SessionDataSet dataSet =
        session.executeQueryStatement("SELECT blob FROM root.chunk_it.d1 WHERE time = 3000000");
    try {
      assertTrue(dataSet.hasNext());
      assertArrayEquals(LARGE_BLOB, dataSet.next().getFields().get(0).getBinaryV().getValues());
      assertFalse(dataSet.hasNext());
    } finally {
      dataSet.closeOperationHandle();
    }
  }

  private static String lastTextValue(SessionDataSet dataSet) throws Exception {
    String value = null;
    while (dataSet.hasNext()) {
      for (Field field : dataSet.next().getFields()) {
        if (field.getBinaryV() != null
            && (value == null || field.getBinaryV().getLength() > value.length())) {
          value = field.getBinaryV().getStringValue(TSFileConfig.STRING_CHARSET);
        }
      }
    }
    return value;
  }

  private static byte[] createBlob() {
    byte[] blob = new byte[LARGE_VALUE_SIZE];
    new Random(42).nextBytes(blob);
    return blob;
  }

  private static String createText() {
    char[] chars = new char[LARGE_VALUE_SIZE];
    Arrays.fill(chars, 'a');
    return new String(chars);
  }
}
