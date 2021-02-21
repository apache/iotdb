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

package org.apache.iotdb.db.utils.datastructure;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.Random;
import java.util.TreeSet;

public class TimeSelectorTest {

  private static final long DEFAULT_ITERATION_TIMES = 10000;

  private TreeSet<Long> selector1;
  private TimeSelector selector2;

  @Before
  public void setUp() {
    selector1 = new TreeSet<>();
    selector2 = new TimeSelector(8, true);
  }

  @Test
  public void testEmpty() {
    Assert.assertTrue(selector2.isEmpty());
  }

  @Test
  public void testAdd1() {
    selector1.add(0L);
    selector2.add(0L);
    assertEquals();
  }

  @Test
  public void testAdd2() {
    for (long i = 0; i < DEFAULT_ITERATION_TIMES; ++i) {
      selector1.add(0L);
      selector2.add(0L);
    }
    assertEquals();
  }

  @Test
  public void testAdd3() {
    for (long i = 0; i < DEFAULT_ITERATION_TIMES; ++i) {
      selector1.add(i);
      selector2.add(i);
    }
    assertEquals();
  }

  @Test
  public void testAdd4() {
    for (long i = 0; i < DEFAULT_ITERATION_TIMES; ++i) {
      selector1.add(DEFAULT_ITERATION_TIMES - i);
      selector2.add(DEFAULT_ITERATION_TIMES - i);
    }
    assertEquals();
  }

  @Test
  public void testAdd5() {
    for (long i = 0; i < DEFAULT_ITERATION_TIMES; ++i) {
      selector1.add(i);
      selector1.add(DEFAULT_ITERATION_TIMES - i);
      selector1.add(i);
      selector1.add(DEFAULT_ITERATION_TIMES - i);

      selector2.add(i);
      selector2.add(DEFAULT_ITERATION_TIMES - i);
      selector2.add(i);
      selector2.add(DEFAULT_ITERATION_TIMES - i);
    }
    assertEquals();
  }

  @Test
  public void testAdd6() {
    for (long i = 0; i < DEFAULT_ITERATION_TIMES; ++i) {
      selector1.add(i);
      selector1.add(i + 2);
      selector1.add(i + 1);
      selector1.add(i - 1);
      selector1.add(i);

      selector2.add(i);
      selector2.add(i + 2);
      selector2.add(i + 1);
      selector2.add(i - 1);
      selector2.add(i);
    }
    assertEquals();
  }

  @Test
  public void testAdd7() {
    Random random = new Random();
    for (long i = 0; i < DEFAULT_ITERATION_TIMES; ++i) {
      long nextRandomLong = random.nextLong();
      selector1.add(nextRandomLong);
      selector2.add(nextRandomLong);
    }
    assertEquals();
  }

  @Test
  public void testAddAndPoll1() {
    for (long i = 0; i < DEFAULT_ITERATION_TIMES; ++i) {
      selector1.add(i);
      selector2.add(i);
      if (i % 3 == 0) {
        selector1.pollFirst();
        selector2.pollFirst();
      }
    }
    assertEquals();
  }

  @Test
  public void testAddAndPoll2() {
    for (long i = 0; i < DEFAULT_ITERATION_TIMES; ++i) {
      selector1.add(DEFAULT_ITERATION_TIMES - i);
      selector1.add(i);

      selector2.add(DEFAULT_ITERATION_TIMES - i);
      selector2.add(i);

      if (i % 3 == 0) {
        selector1.pollFirst();
        selector2.pollFirst();
      }
    }
    assertEquals();
  }

  @Test
  public void testAddAndPoll3() {
    for (long i = 0; i < DEFAULT_ITERATION_TIMES; ++i) {
      selector1.add(i);
      selector1.add(i + 2);
      selector1.pollFirst();
      selector1.add(i + 1);
      selector1.add(i - 1);
      selector1.pollFirst();
      selector1.add(i);
      selector1.pollFirst();

      selector2.add(i);
      selector2.add(i + 2);
      selector2.pollFirst();
      selector2.add(i + 1);
      selector2.add(i - 1);
      selector2.pollFirst();
      selector2.add(i);
      selector2.pollFirst();
    }
    assertEquals();
  }

  @Test
  public void testDescending() {
    selector1 = new TreeSet<>(Collections.reverseOrder());
    selector2 = new TimeSelector(8, false);

    for (long i = 0; i < DEFAULT_ITERATION_TIMES; ++i) {
      selector1.add(i);
      selector1.add(i + 2);
      selector1.pollFirst();
      selector1.add(i + 1);
      selector1.add(i - 1);
      selector1.pollFirst();
      selector1.add(i);
      selector1.pollFirst();

      selector2.add(i);
      selector2.add(i + 2);
      selector2.pollFirst();
      selector2.add(i + 1);
      selector2.add(i - 1);
      selector2.pollFirst();
      selector2.add(i);
      selector2.pollFirst();
    }
    assertEquals();
  }

  private void assertEquals() {
    Assert.assertFalse(selector2.isEmpty());
    Assert.assertFalse(selector2.isEmpty()); // on purpose
    while (!selector1.isEmpty()) {
      Assert.assertFalse(selector2.isEmpty());
      Assert.assertEquals((long) selector1.pollFirst(), selector2.pollFirst());
    }
    Assert.assertTrue(selector2.isEmpty());
    Assert.assertTrue(selector2.isEmpty()); // on purpose
  }
}
