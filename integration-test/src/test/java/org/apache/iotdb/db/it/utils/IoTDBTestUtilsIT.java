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
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;
import org.apache.iotdb.itbase.env.BaseEnv;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
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
  public void testAssertAlwaysFail() {
    assertObservationPeriod(
        () ->
            TestUtils.assertAlwaysFail(
                EnvFactory.getEnv(), "select * from missing_database.missing_table", 1));
    assertThrows(
        AssertionError.class,
        () -> TestUtils.assertAlwaysFail(EnvFactory.getEnv(), "show databases", 1));
  }

  @Test
  public void testHalfTimeoutElapsedUsesSeconds() {
    assertFalse(TestUtils.isHalfTimeoutElapsed(299_999, 600));
    assertTrue(TestUtils.isHalfTimeoutElapsed(300_001, 600));
  }

  @Test
  public void testAssertDataAlwaysFailsOnMismatch() {
    final BaseEnv env = EnvFactory.getEnv();
    final AtomicInteger failures = new AtomicInteger();
    final Consumer<String> handleFailure = message -> failures.incrementAndGet();
    assertThrows(
        AssertionError.class,
        () ->
            TestUtils.assertDataAlwaysOnEnv(
                env, "count databases", "unexpected,", Collections.emptySet(), 1, (String) null));
    assertThrows(
        AssertionError.class,
        () ->
            TestUtils.assertDataAlwaysOnEnv(
                env, "count databases", "unexpected,", Collections.emptySet(), 1, handleFailure));
    assertThrows(
        AssertionError.class,
        () ->
            TestUtils.assertDataAlwaysOnEnv(
                env,
                "show databases",
                "unexpected,",
                Collections.emptySet(),
                1,
                "information_schema",
                handleFailure));
    assertEquals(2, failures.get());
  }

  @Test
  public void testAssertDataAlwaysObservationPeriod() {
    final BaseEnv env = EnvFactory.getEnv();
    final Consumer<String> handleFailure = message -> fail(message);
    assertObservationPeriod(
        () ->
            TestUtils.assertDataAlwaysOnEnv(
                env, "count databases", "count,", Collections.singleton("0,"), 1, (String) null));
    assertObservationPeriod(
        () ->
            TestUtils.assertDataAlwaysOnEnv(
                env, "count databases", "count,", Collections.singleton("0,"), 1, handleFailure));
    assertObservationPeriod(
        () ->
            TestUtils.assertDataAlwaysOnEnv(
                env,
                "count databases",
                "count,",
                Collections.singleton("1,"),
                1,
                "information_schema",
                handleFailure));
  }

  private void assertObservationPeriod(final Runnable assertion) {
    final long start = System.nanoTime();
    assertion.run();
    assertTrue(
        "The assertion must observe queries for the requested period",
        System.nanoTime() - start >= TimeUnit.SECONDS.toNanos(1));
  }
}
