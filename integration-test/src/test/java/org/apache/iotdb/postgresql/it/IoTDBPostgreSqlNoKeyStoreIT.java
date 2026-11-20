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

import java.net.ServerSocket;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * A non-loopback bind without a key store does not open the port; the failure names {@code
 * postgresql_service_address}.
 */
@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class})
public class IoTDBPostgreSqlNoKeyStoreIT {

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
        .setPostgreSqlServiceAddress("0.0.0.0")
        .setPostgreSqlServicePort(port);
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @AfterClass
  public static void tearDown() {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void noPortAndTheAddressIsNamed() {
    assertFalse("no listener without TLS off loopback", PgItSupport.listening(port));
    String log = PgItSupport.dataNodeLog();
    assertTrue(
        log.contains("PostgreSQL endpoint not started")
            && log.contains("postgresql_service_address=0.0.0.0"));
  }
}
