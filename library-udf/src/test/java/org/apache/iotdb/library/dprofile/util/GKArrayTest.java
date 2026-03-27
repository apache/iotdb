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

package org.apache.iotdb.library.dprofile.util;

import org.junit.Assert;
import org.junit.Test;

import java.util.Random;

/** Stress tests for {@link GKArray} merge (including exhausted-additionalEntries path). */
public class GKArrayTest {

  @Test
  public void manyCompressCyclesRandomUniform() {
    GKArray gk = new GKArray(0.01);
    Random rnd = new Random(0);
    for (int i = 0; i < 50_000; i++) {
      gk.insert(rnd.nextDouble());
    }
    double q = gk.query(0.5);
    Assert.assertTrue("median-ish should stay in [0,1]", q >= 0.0 && q <= 1.0);
  }

  @Test
  public void sequentialForcesMergeWithExistingSketch() {
    GKArray gk = new GKArray(0.05);
    for (int i = 0; i < 10_000; i++) {
      gk.insert(i);
    }
    double q = gk.query(0.5);
    Assert.assertTrue(q >= 0 && q < 10_000);
  }

  @Test
  public void queryEndpoints() {
    GKArray gk = new GKArray(0.02);
    for (int i = 1; i <= 100; i++) {
      gk.insert(i);
    }
    double q0 = gk.query(0.01);
    double q1 = gk.query(1.0);
    Assert.assertTrue(q0 <= q1);
  }
}
