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

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.net.InetAddress;
import java.net.ServerSocket;

import static org.junit.Assert.assertTrue;

/** A port already in use fails the service, naming the port key; the database starts anyway. */
@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class})
public class IoTDBPostgreSqlPortInUseIT {

  private static ServerSocket occupied;

  @BeforeClass
  public static void setUp() throws Exception {
    occupied = new ServerSocket(0, 50, InetAddress.getByName("127.0.0.1"));
    EnvFactory.getEnv()
        .getConfig()
        .getDataNodeConfig()
        .setEnablePostgreSqlService(true)
        .setPostgreSqlServicePort(occupied.getLocalPort());
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    try {
      EnvFactory.getEnv().cleanClusterEnvironment();
    } finally {
      occupied.close();
    }
  }

  @Test
  public void theServiceFailsNamingThePort() throws Exception {
    String log =
        PgItSupport.awaitDataNodeLog(
            l ->
                l.contains("PostgreSQL endpoint not started")
                    && l.contains("postgresql_service_port"));
    assertTrue(
        "log names the port key",
        log.contains("PostgreSQL endpoint not started") && log.contains("postgresql_service_port"));
    // the database itself runs
    try (org.apache.iotdb.isession.ITableSession session =
        EnvFactory.getEnv().getTableSessionConnection()) {
      session.executeNonQueryStatement("CREATE DATABASE still_up");
    }
  }
}
