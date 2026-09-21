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

package org.apache.iotdb.metrics.core.type;

import org.junit.Test;

import java.lang.ref.WeakReference;
import java.lang.reflect.Field;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

public class IoTDBAutoGaugeTest {

  @Test
  public void testLiveAndClearedReferent() throws Exception {
    AtomicInteger value = new AtomicInteger(7);
    IoTDBAutoGauge<AtomicInteger> gauge = new IoTDBAutoGauge<>(value, AtomicInteger::get);
    assertEquals(7, gauge.getValue(), 0);
    value.set(9);
    assertEquals(9, gauge.getValue(), 0);

    Field field = IoTDBAutoGauge.class.getDeclaredField("refObject");
    field.setAccessible(true);
    ((WeakReference<?>) field.get(gauge)).clear();
    assertEquals(0, gauge.getValue(), 0);
  }

  @Test
  public void testReferentRemainsAvailableForCurrentSample() throws Exception {
    AtomicInteger value = new AtomicInteger(7);
    IoTDBAutoGauge<AtomicInteger> gauge = new IoTDBAutoGauge<>(value, AtomicInteger::get);
    // Deterministically simulate collection between two weak-reference reads, without relying on
    // GC.
    WeakReference<AtomicInteger> reference =
        new WeakReference<AtomicInteger>(value) {
          @Override
          public AtomicInteger get() {
            AtomicInteger referent = super.get();
            clear();
            return referent;
          }
        };
    Field field = IoTDBAutoGauge.class.getDeclaredField("refObject");
    field.setAccessible(true);
    field.set(gauge, reference);

    assertEquals(7, gauge.getValue(), 0);
    assertEquals(0, gauge.getValue(), 0);
  }
}
