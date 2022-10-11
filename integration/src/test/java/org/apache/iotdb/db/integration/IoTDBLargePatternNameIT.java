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

import org.apache.iotdb.integration.env.EnvFactory;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.Statement;

public class IoTDBLargePatternNameIT {
  @Before
  public void startUp() throws Exception {
    EnvFactory.getEnv().initBeforeClass();
  }

  @After
  public void tearDown() throws Exception {
    EnvFactory.getEnv().cleanAfterClass();
  }

  @Test
  public void testLargePattern() throws Exception {
    StringBuilder sb = new StringBuilder();
    sb.append("insert into root.ln.wf01.wt01(timestamp,status,s) values(1509465780000,false,'");
    for (int i = 0; i < 1024; ++i) {
      sb.append('a');
    }
    sb.append("b');");
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(sb.toString());
      try {
        statement.execute("select s from root.ln.wf01.wt01 where s REGEXP'(a+)+s'");
      } catch (Exception e) {
        Assert.assertTrue(e.getMessage().contains("Pattern access threshold exceeded"));
        return;
      }
      Assert.fail();
    }
  }
}
