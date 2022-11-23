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
package org.apache.iotdb.db.integration;

import org.apache.iotdb.db.engine.version.SimpleFileVersionController;
import org.apache.iotdb.integration.env.EnvFactory;
import org.apache.iotdb.itbase.category.LocalStandaloneTest;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

@Category({LocalStandaloneTest.class})
public class IoTDBVersionIT {

  @Before
  public void setUp() throws Exception {
    EnvFactory.getEnv().initBeforeTest();
  }

  @After
  public void tearDown() throws Exception {
    EnvFactory.getEnv().cleanAfterTest();
  }

  @Test
  public void testVersionPersist() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE root.versionTest1");
      statement.execute("CREATE DATABASE root.versionTest2");
      statement.execute(
          "CREATE TIMESERIES root.versionTest1.s0" + " WITH DATATYPE=INT32,ENCODING=PLAIN");
      statement.execute(
          "CREATE TIMESERIES root.versionTest2.s0" + " WITH DATATYPE=INT32,ENCODING=PLAIN");

      // insert and flush enough times to make the version file persist
      for (int i = 0; i < SimpleFileVersionController.getSaveInterval() + 1; i++) {
        statement.execute(
            String.format(
                "INSERT INTO root.versionTest1(timestamp, s0) VALUES (%d, %d)", i * 100, i));
        statement.execute("FLUSH");
        statement.execute("MERGE");
      }
    }
  }
}
