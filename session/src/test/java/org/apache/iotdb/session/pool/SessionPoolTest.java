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
package org.apache.iotdb.session.pool;

import org.apache.iotdb.isession.util.Version;

import org.junit.Test;

import java.time.ZoneOffset;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SessionPoolTest {

  @Test
  public void testBuilder() {
    SessionPool pool =
        new SessionPool.Builder()
            .host("localhost")
            .port(1234)
            .maxSize(10)
            .user("abc")
            .password("123")
            .fetchSize(1)
            .waitToGetSessionTimeoutInMs(2)
            .enableRedirection(true)
            .enableCompression(true)
            .zoneId(ZoneOffset.UTC)
            .connectionTimeoutInMs(3)
            .version(Version.V_1_0)
            .build();

    assertEquals("localhost", pool.getHost());
    assertEquals(1234, pool.getPort());
    assertEquals("abc", pool.getUser());
    assertEquals("123", pool.getPassword());
    assertEquals(10, pool.getMaxSize());
    assertEquals(1, pool.getFetchSize());
    assertEquals(2, pool.getWaitToGetSessionTimeoutInMs());
    assertTrue(pool.isEnableRedirection());
    assertTrue(pool.isEnableCompression());
    assertEquals(3, pool.getConnectionTimeoutInMs());
    assertEquals(ZoneOffset.UTC, pool.getZoneId());
    assertEquals(Version.V_1_0, pool.getVersion());
  }
}
