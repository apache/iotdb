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

package org.apache.iotdb.db.it.utils;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.itbase.env.BaseEnv;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.Statement;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBTestUtilsIT {

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @AfterClass
  public static void tearDown() {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testAssertDataAlwaysFailsOnMismatch() {
    assertThrows(
        AssertionError.class,
        () ->
            TestUtils.assertDataAlwaysOnEnv(
                EnvFactory.getEnv(), "count databases", "unexpected,", Collections.emptySet(), 1));
  }

  @Test
  public void testAssertDataAlwaysObservationPeriod() {
    assertObservationPeriod(
        () ->
            TestUtils.assertDataAlwaysOnEnv(
                EnvFactory.getEnv(), "count databases", "count,", Collections.singleton("0,"), 1));
  }

  @Test
  public void testAssertDataAlwaysIncludesQueryTime() {
    final AtomicInteger queries = new AtomicInteger();
    final BaseEnv slowEnv = delayQueries(BaseEnv.class, EnvFactory.getEnv(), queries);
    assertObservationPeriod(
        () ->
            TestUtils.assertDataAlwaysOnEnv(
                slowEnv, "count databases", "count,", Collections.singleton("0,"), 1));
    // A successful query taking longer than the observation period must not cause a timeout.
    assertEquals(1, queries.get());
  }

  private <T> T delayQueries(final Class<T> type, final T delegate, final AtomicInteger queries) {
    return type.cast(
        Proxy.newProxyInstance(
            type.getClassLoader(),
            new Class<?>[] {type},
            (proxy, method, args) -> {
              if ("executeQuery".equals(method.getName())) {
                queries.incrementAndGet();
                TimeUnit.SECONDS.sleep(7);
              }
              final Object result;
              try {
                result = method.invoke(delegate, args);
              } catch (InvocationTargetException e) {
                throw e.getCause();
              }
              if (result instanceof Connection) {
                return delayQueries(Connection.class, (Connection) result, queries);
              }
              if (result instanceof Statement) {
                return delayQueries(Statement.class, (Statement) result, queries);
              }
              return result;
            }));
  }

  private void assertObservationPeriod(final Runnable assertion) {
    final long start = System.nanoTime();
    assertion.run();
    assertTrue(
        "The assertion must observe queries for the requested period",
        System.nanoTime() - start >= TimeUnit.SECONDS.toNanos(1));
  }
}
