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
package org.apache.iotdb.db.protocol.influxdb.dto;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class IoTDBPointTest {

  @Before
  public void setUp() {}

  @After
  public void tearDown() {}

  @Test
  public void testSecondsTime() {
    long currentTime = System.currentTimeMillis() / 1000;
    long time = TimeUnit.MILLISECONDS.convert(currentTime, TimeUnit.SECONDS);
    assertEquals(time, currentTime * 1000);
  }

  @Test
  public void testNanosecondsTime() {
    long currentTime = System.currentTimeMillis() * 1000000;
    long time = TimeUnit.MILLISECONDS.convert(currentTime, TimeUnit.NANOSECONDS);
    assertEquals(time, currentTime / 1000000);
  }

  @Test
  public void testMinuteTime() {
    long currentTime = System.currentTimeMillis() / 1000 / 60;
    long time = TimeUnit.MILLISECONDS.convert(currentTime, TimeUnit.MINUTES);
    assertEquals(time, currentTime * 1000 * 60);
  }
}
