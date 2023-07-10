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

package org.apache.iotdb.commons.udf;

import org.apache.iotdb.commons.udf.builtin.UDTFAbs;
import org.apache.iotdb.commons.udf.builtin.UDTFAcos;
import org.apache.iotdb.commons.udf.builtin.UDTFAsin;
import org.apache.iotdb.commons.udf.builtin.UDTFAtan;
import org.apache.iotdb.commons.udf.builtin.UDTFCeil;
import org.apache.iotdb.commons.udf.builtin.UDTFCos;
import org.apache.iotdb.commons.udf.builtin.UDTFCosh;
import org.apache.iotdb.commons.udf.builtin.UDTFDegrees;
import org.apache.iotdb.commons.udf.builtin.UDTFExp;
import org.apache.iotdb.commons.udf.builtin.UDTFFloor;
import org.apache.iotdb.commons.udf.builtin.UDTFLog;
import org.apache.iotdb.commons.udf.builtin.UDTFLog10;
import org.apache.iotdb.commons.udf.builtin.UDTFRadians;
import org.apache.iotdb.commons.udf.builtin.UDTFSign;
import org.apache.iotdb.commons.udf.builtin.UDTFSin;
import org.apache.iotdb.commons.udf.builtin.UDTFSinh;
import org.apache.iotdb.commons.udf.builtin.UDTFSqrt;
import org.apache.iotdb.commons.udf.builtin.UDTFTan;
import org.apache.iotdb.commons.udf.builtin.UDTFTanh;
import org.apache.iotdb.udf.api.UDTF;
import org.apache.iotdb.udf.api.access.Row;
import org.apache.iotdb.udf.api.collector.PointCollector;
import org.apache.iotdb.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.udf.api.type.Type;

import com.google.common.collect.ImmutableList;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

public class IoTDBBuiltInUDFTest {
  private static List<UDFParameters> udfParametersList;
  private static UDTFConfigurations udtfConfigurations;
  private static List<UDFParameterValidator> validators;
  // mock rows of s1 - s6
  private static List<Row> s1_s6Rows;
  // mock rows of s7
  private static List<Row> s7Rows;
  // mock rows of s8
  private static List<Row> s8Rows;
  private static final double error = 0.0001;

  @BeforeClass
  public static void setup() throws IOException {
    udfParametersList =
        ImmutableList.of(
            new UDFParameters(
                ImmutableList.of("root.sg.d1.s1"), ImmutableList.of(Type.INT32), new HashMap<>()),
            new UDFParameters(
                ImmutableList.of("root.sg.d1.s2"), ImmutableList.of(Type.INT64), new HashMap<>()),
            new UDFParameters(
                ImmutableList.of("root.sg.d1.s3"), ImmutableList.of(Type.FLOAT), new HashMap<>()),
            new UDFParameters(
                ImmutableList.of("root.sg.d1.s4"), ImmutableList.of(Type.DOUBLE), new HashMap<>()),
            new UDFParameters(
                ImmutableList.of("root.sg.d1.s5"), ImmutableList.of(Type.BOOLEAN), new HashMap<>()),
            new UDFParameters(
                ImmutableList.of("root.sg.d1.s6"), ImmutableList.of(Type.TEXT), new HashMap<>()),
            new UDFParameters(
                ImmutableList.of("root.sg.d1.s7"), ImmutableList.of(Type.FLOAT), new HashMap<>()),
            new UDFParameters(
                ImmutableList.of("root.sg.d1.s8"), ImmutableList.of(Type.FLOAT), new HashMap<>()));
    udtfConfigurations = new UDTFConfigurations(ZoneId.of("UTC+8"));
    validators =
        udfParametersList.stream().map(UDFParameterValidator::new).collect(Collectors.toList());
    s1_s6Rows =
        ImmutableList.of(
            Mockito.mock(Row.class),
            Mockito.mock(Row.class),
            Mockito.mock(Row.class),
            Mockito.mock(Row.class),
            Mockito.mock(Row.class),
            Mockito.mock(Row.class));

    // row 0
    Mockito.when(s1_s6Rows.get(0).getTime()).thenReturn(0L);
    Mockito.when(s1_s6Rows.get(0).getInt(0)).thenReturn(0);
    Mockito.when(s1_s6Rows.get(0).getLong(0)).thenReturn(0L);
    Mockito.when(s1_s6Rows.get(0).getFloat(0)).thenReturn(0f);
    Mockito.when(s1_s6Rows.get(0).getDouble(0)).thenReturn(0d);
    Mockito.when(s1_s6Rows.get(0).getBoolean(0)).thenReturn(true);
    Mockito.when(s1_s6Rows.get(0).getString(0)).thenReturn("true");

    // row 1
    Mockito.when(s1_s6Rows.get(1).getTime()).thenReturn(2L);
    Mockito.when(s1_s6Rows.get(1).getInt(0)).thenReturn(1);
    Mockito.when(s1_s6Rows.get(1).getLong(0)).thenReturn(1L);
    Mockito.when(s1_s6Rows.get(1).getFloat(0)).thenReturn(1f);
    Mockito.when(s1_s6Rows.get(1).getDouble(0)).thenReturn(1d);
    Mockito.when(s1_s6Rows.get(1).getBoolean(0)).thenReturn(false);
    Mockito.when(s1_s6Rows.get(1).getString(0)).thenReturn("false");

    // row 2
    Mockito.when(s1_s6Rows.get(2).getTime()).thenReturn(4L);
    Mockito.when(s1_s6Rows.get(2).getInt(0)).thenReturn(2);
    Mockito.when(s1_s6Rows.get(2).getLong(0)).thenReturn(2L);
    Mockito.when(s1_s6Rows.get(2).getFloat(0)).thenReturn(2f);
    Mockito.when(s1_s6Rows.get(2).getDouble(0)).thenReturn(2d);
    Mockito.when(s1_s6Rows.get(2).getBoolean(0)).thenReturn(false);
    Mockito.when(s1_s6Rows.get(2).getString(0)).thenReturn("false");

    // row 3
    Mockito.when(s1_s6Rows.get(3).getTime()).thenReturn(6L);
    Mockito.when(s1_s6Rows.get(3).getInt(0)).thenReturn(3);
    Mockito.when(s1_s6Rows.get(3).getLong(0)).thenReturn(3L);
    Mockito.when(s1_s6Rows.get(3).getFloat(0)).thenReturn(3f);
    Mockito.when(s1_s6Rows.get(3).getDouble(0)).thenReturn(3d);
    Mockito.when(s1_s6Rows.get(3).getBoolean(0)).thenReturn(true);
    Mockito.when(s1_s6Rows.get(3).getString(0)).thenReturn("true");

    // row 4
    Mockito.when(s1_s6Rows.get(4).getTime()).thenReturn(8L);
    Mockito.when(s1_s6Rows.get(4).getInt(0)).thenReturn(4);
    Mockito.when(s1_s6Rows.get(4).getLong(0)).thenReturn(4L);
    Mockito.when(s1_s6Rows.get(4).getFloat(0)).thenReturn(4f);
    Mockito.when(s1_s6Rows.get(4).getDouble(0)).thenReturn(4d);
    Mockito.when(s1_s6Rows.get(4).getBoolean(0)).thenReturn(true);
    Mockito.when(s1_s6Rows.get(4).getString(0)).thenReturn("true");

    // row 5
    Mockito.when(s1_s6Rows.get(5).getTime()).thenReturn(10000000000L);
    Mockito.when(s1_s6Rows.get(5).getInt(0)).thenReturn(5);
    Mockito.when(s1_s6Rows.get(5).getLong(0)).thenReturn(5L);
    Mockito.when(s1_s6Rows.get(5).getFloat(0)).thenReturn(5f);
    Mockito.when(s1_s6Rows.get(5).getDouble(0)).thenReturn(5d);
    Mockito.when(s1_s6Rows.get(5).getBoolean(0)).thenReturn(false);
    Mockito.when(s1_s6Rows.get(5).getString(0)).thenReturn("false");
  }

  @AfterClass
  public static void tearDown() throws IOException {
    udfParametersList = null;
    udtfConfigurations = null;
    validators = null;
    s1_s6Rows = null;
    s7Rows = null;
    s8Rows = null;
  }

  @After
  public void cleanAttributes() {
    udfParametersList.forEach(udfParameters -> udfParameters.getAttributes().clear());
  }

  @Test
  public void testMathUDF() throws Exception {
    testMathFunctions(Math::sin, new UDTFSin(), s1_s6Rows);
    testMathFunctions(Math::cos, new UDTFCos(), s1_s6Rows);
    testMathFunctions(Math::tan, new UDTFTan(), s1_s6Rows);
    testMathFunctions(Math::asin, new UDTFAsin(), s1_s6Rows);
    testMathFunctions(Math::acos, new UDTFAcos(), s1_s6Rows);
    testMathFunctions(Math::atan, new UDTFAtan(), s1_s6Rows);
    testMathFunctions(Math::sinh, new UDTFSinh(), s1_s6Rows);
    testMathFunctions(Math::cosh, new UDTFCosh(), s1_s6Rows);
    testMathFunctions(Math::tanh, new UDTFTanh(), s1_s6Rows);
    testMathFunctions(Math::toDegrees, new UDTFDegrees(), s1_s6Rows);
    testMathFunctions(Math::toRadians, new UDTFRadians(), s1_s6Rows);
    testMathFunctions(Math::abs, new UDTFAbs(), s1_s6Rows);
    testMathFunctions(Math::signum, new UDTFSign(), s1_s6Rows);
    testMathFunctions(Math::ceil, new UDTFCeil(), s1_s6Rows);
    testMathFunctions(Math::floor, new UDTFFloor(), s1_s6Rows);
    testMathFunctions(Math::exp, new UDTFExp(), s1_s6Rows);
    testMathFunctions(Math::log, new UDTFLog(), s1_s6Rows);
    testMathFunctions(Math::log10, new UDTFLog10(), s1_s6Rows);
    testMathFunctions(Math::sqrt, new UDTFSqrt(), s1_s6Rows);
  }

  private void testMathFunctions(
      Function<Double, Double> standardFunction, UDTF udf, List<Row> rows) throws Exception {
    // each column
    for (int i = 0; i < 4; i++) {
      udf.validate(validators.get(i));
      udf.beforeStart(udfParametersList.get(i), udtfConfigurations);
      // each row
      for (Row row : rows) {
        udf.transform(row, Mockito.mock(PointCollector.class));
        Assert.assertEquals(
            standardFunction.apply(row.getDouble(0)),
            Double.parseDouble(udf.transform(row).toString()),
            error);
      }
    }
  }
}
