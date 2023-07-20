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

package org.apache.iotdb.db.it.schema;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.util.AbstractSchemaIT;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.Statement;

public class IoTDBDatabaseQuotaIT extends AbstractSchemaIT {
  public IoTDBDatabaseQuotaIT(SchemaTestMode schemaTestMode) {
    super(schemaTestMode);
  }

  @Parameterized.BeforeParam
  public static void before() throws Exception {
    setUpEnvironment();
    EnvFactory.getEnv().getConfig().getCommonConfig().setDatabaseLimitThreshold(2);
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @Parameterized.AfterParam
  public static void after() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
    tearDownEnvironment();
  }

  @Test
  public void testClusterSchemaQuota() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("create database root.db1;");
      statement.execute("insert into root.db2.device(time,s1) values(1,5);");
      try {
        statement.execute("create database root.db3;");
        Assert.fail("root.db3 should not be created.");
      } catch (Exception e) {
        Assert.assertTrue(
            e.getMessage()
                .contains(
                    "The current database number has exceeded the cluster quota. The maximum number of cluster databases allowed is 2, Please review your configuration database_limit_threshold or delete some existing database to comply with the quota."));
      }
      try {
        statement.execute("insert into root.db3.device(time,s1) values(1,5);");
        Assert.fail("root.db3 should not be created.");
      } catch (Exception e) {
        Assert.assertTrue(
            e.getMessage()
                .contains(
                    "The current database number has exceeded the cluster quota. The maximum number of cluster databases allowed is 2, Please review your configuration database_limit_threshold or delete some existing database to comply with the quota."));
      }
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
  }
}
