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
package org.apache.iotdb.jdbc;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;

import static org.junit.Assert.assertEquals;

public class StringUtilsTest {

  @Before
  public void setUp() {}

  @After
  public void tearDown() {}

  @Test
  public void testFixDecimalExponent() throws IoTDBURLException {
    String s = StringUtils.fixDecimalExponent("7.894561225674126e19");
    assertEquals(s, "7.894561225674126e+19");
  }

  @Test
  public void testConsistentToString() throws IoTDBURLException {
    BigDecimal bigDecimal = null;
    String s = StringUtils.consistentToString(bigDecimal);
    assertEquals(s, null);
    bigDecimal = new BigDecimal("1.1");
    s = StringUtils.consistentToString(bigDecimal);
    System.out.println(s);
    assertEquals(s, "1.1");
  }
}
