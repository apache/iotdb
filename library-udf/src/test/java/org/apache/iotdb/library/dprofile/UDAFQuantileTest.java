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
import org.apache.iotdb.udf.api.type.Type;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.time.ZoneId;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Regression: FLOAT input must not use dataToLong(Object) with boxed Double (ClassCastException).
 */
public class UDAFQuantileTest {

  @Test
  public void floatSeriesTransformDoesNotThrow() throws Exception {
    UDAFQuantile udf = new UDAFQuantile();
    Map<String, String> attrs = new HashMap<>();
    attrs.put("K", "100");
    attrs.put("rank", "0.5");
    UDFParameters params =
        new UDFParameters(
            Collections.singletonList("s"), Collections.singletonList(Type.FLOAT), attrs);
    UDTFConfigurations config = new UDTFConfigurations(ZoneId.systemDefault());
    udf.beforeStart(params, config);

    Row row = Mockito.mock(Row.class);
    Mockito.when(row.getDataType(0)).thenReturn(Type.FLOAT);
    Mockito.when(row.getFloat(0)).thenReturn(1.25f);

    udf.transform(row, Mockito.mock(PointCollector.class));
    Assert.assertTrue(true);
  }
}
