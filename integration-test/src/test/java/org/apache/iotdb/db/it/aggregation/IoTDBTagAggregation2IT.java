/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.it.aggregation;

import org.apache.iotdb.it.env.EnvFactory;

import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.sql.Connection;
import java.sql.Statement;

public class IoTDBTagAggregation2IT extends IoTDBTagAggregationIT {

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().getConfig().getCommonConfig().setDegreeOfParallelism(4);
    EnvFactory.getEnv().initClusterEnvironment();
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      for (String sql : DATASET) {
        statement.execute(sql);
      }
    } catch (Exception e) {
      e.printStackTrace();
      throw e;
    }
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }
}
