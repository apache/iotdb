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

package org.apache.iotdb.library;

import org.apache.iotdb.library.util.Util;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;

import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class UtilParseTimeTest {

  private static UDFParameters parametersWithPrecision(String precision) {
    Map<String, String> systemAttributes = new HashMap<>();
    systemAttributes.put("timestampPrecision", precision);
    return new UDFParameters(
        Collections.emptyList(), Collections.emptyList(), new HashMap<>(), systemAttributes);
  }

  @Test
  public void testHourAndDayAtNanosecondPrecision() {
    UDFParameters ns = parametersWithPrecision("ns");
    Assert.assertEquals(3_600_000_000_000L, Util.parseTime("1h", ns));
    Assert.assertEquals(86_400_000_000_000L, Util.parseTime("1d", ns));
    Assert.assertEquals(172_800_000_000_000L, Util.parseTime("2d", ns));
  }

  @Test
  public void testUnitsScaleWithPrecision() {
    UDFParameters ms = parametersWithPrecision("ms");
    UDFParameters us = parametersWithPrecision("us");
    UDFParameters ns = parametersWithPrecision("ns");
    for (String unit : new String[] {"1ms", "1s", "1m", "1h", "1d"}) {
      long millis = Util.parseTime(unit, ms);
      Assert.assertEquals(unit, millis * 1_000L, Util.parseTime(unit, us));
      Assert.assertEquals(unit, millis * 1_000_000L, Util.parseTime(unit, ns));
    }
  }
}
