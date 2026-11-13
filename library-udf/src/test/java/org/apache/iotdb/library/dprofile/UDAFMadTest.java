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

package org.apache.iotdb.library.dprofile;

import org.apache.iotdb.udf.api.access.Row;
import org.apache.iotdb.udf.api.collector.PointCollector;
import org.apache.iotdb.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.udf.api.exception.UDFException;
import org.apache.iotdb.udf.api.type.Type;

import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Proxy;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class UDAFMadTest {

  private static final double[] VALUES = {
    2403.71, 2404.36, 2402.88, 2403.95, 2404.12, 2401.63, 2403.52, 2404.77, 2402.34, 2403.18,
    2405.01, 2397.77, 2404.63, 2402.07, 2403.85, 2404.44, 2402.61, 2403.29, 2404.90, 2402.15,
    2403.63, 2404.05, 2402.72, 2403.97
  };
  private static final double EXPECTED_MAD = 0.78;

  @Test
  public void testExactMad() throws Exception {
    UDAFMad mad = new UDAFMad();
    mad.beforeStart(parameters(0), new UDTFConfigurations(ZoneId.systemDefault()));
    RecordingCollector collector = new RecordingCollector();
    for (double value : VALUES) {
      mad.transform(doubleRow(value), collector.collector);
    }
    mad.terminate(collector.collector);
    Assert.assertEquals(EXPECTED_MAD, collector.value, 1e-9);
  }

  @Test
  public void testSketchRejectsDegenerateResult() throws Exception {
    UDAFMad mad = new UDAFMad();
    mad.beforeStart(parameters(0.01), new UDTFConfigurations(ZoneId.systemDefault()));
    RecordingCollector collector = new RecordingCollector();
    for (double value : VALUES) {
      mad.transform(doubleRow(value), collector.collector);
    }
    Assert.assertThrows(UDFException.class, () -> mad.terminate(collector.collector));
  }

  private static UDFParameters parameters(double error) {
    Map<String, String> attributes = new HashMap<>();
    if (error != 0) {
      attributes.put("error", Double.toString(error));
    }
    return new UDFParameters(Arrays.asList("s0"), Arrays.asList(Type.DOUBLE), attributes);
  }

  private static Row doubleRow(double value) {
    return (Row)
        Proxy.newProxyInstance(
            Row.class.getClassLoader(),
            new Class<?>[] {Row.class},
            (proxy, method, args) -> {
              switch (method.getName()) {
                case "getTime":
                  return 0L;
                case "isNull":
                  return false;
                case "getDouble":
                  return value;
                case "getDataType":
                  return Type.DOUBLE;
                case "size":
                  return 1;
                default:
                  return null;
              }
            });
  }

  private static class RecordingCollector {

    private double value = Double.NaN;
    private final PointCollector collector =
        (PointCollector)
            Proxy.newProxyInstance(
                PointCollector.class.getClassLoader(),
                new Class<?>[] {PointCollector.class},
                (proxy, method, args) -> {
                  if ("putDouble".equals(method.getName())) {
                    value = (Double) args[1];
                  }
                  return null;
                });
  }
}
