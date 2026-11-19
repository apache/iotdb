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

package org.apache.iotdb.relational.it.db.it;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;
import org.apache.iotdb.itbase.runtime.ClusterTestConnection;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * End-to-end coverage of fragmented query results of the table model through JDBC, with the payload
 * bound at its minimum.
 */
@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBTableResultChunkingIT {

  private static final int PAYLOAD_SIZE_IN_BYTES = 128 * 1024;
  private static final int LARGE_VALUE_SIZE = 300 * 1024;

  private static final byte[] LARGE_BLOB = createBlob();

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv()
        .getConfig()
        .getDataNodeConfig()
        .setMppDataExchangeMaxPayloadSizeInBytes(PAYLOAD_SIZE_IN_BYTES);
    EnvFactory.getEnv().initClusterEnvironment();

    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE chunk_it_table");
      statement.execute(
          "CREATE TABLE chunk_it_table.blob_table(time TIMESTAMP TIME, payload BLOB FIELD)");
    }

    try (Connection connection = getRealTableConnection();
        PreparedStatement preparedStatement =
            connection.prepareStatement(
                "INSERT INTO chunk_it_table.blob_table(time, payload) VALUES (?, ?)")) {
      // three oversized rows mixed with one small row
      for (int i = 0; i < 4; i++) {
        preparedStatement.setLong(1, i);
        preparedStatement.setBytes(2, i == 3 ? new byte[] {1, 2, 3} : LARGE_BLOB);
        preparedStatement.execute();
      }
    }
  }

  @AfterClass
  public static void tearDown() {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  private static Connection getRealTableConnection() throws SQLException, IOException {
    Connection connection = EnvFactory.getEnv().getTableConnection();
    if (connection instanceof ClusterTestConnection) {
      // the test connection proxy does not support prepareStatement
      return ((ClusterTestConnection) connection).writeConnection.getUnderlyingConnection();
    }
    return connection;
  }

  @Test
  public void testOversizedRowsThroughJdbc() throws Exception {
    try (Connection connection = getRealTableConnection();
        Statement statement = connection.createStatement()) {
      statement.setFetchSize(1);
      try (ResultSet resultSet =
          statement.executeQuery("SELECT payload FROM chunk_it_table.blob_table ORDER BY time")) {
        for (int i = 0; i < 4; i++) {
          assertTrue("missing row " + i, resultSet.next());
          assertArrayEquals(i == 3 ? new byte[] {1, 2, 3} : LARGE_BLOB, resultSet.getBytes(1));
        }
        assertFalse(resultSet.next());
      }
    }
  }

  private static byte[] createBlob() {
    byte[] blob = new byte[LARGE_VALUE_SIZE];
    new Random(7).nextBytes(blob);
    return blob;
  }
}
