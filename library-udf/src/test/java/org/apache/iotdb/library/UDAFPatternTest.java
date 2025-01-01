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

import org.apache.iotdb.library.match.PatternExecutor;
import org.apache.iotdb.library.match.UDAFPatternMatch;
import org.apache.iotdb.library.match.model.PatternContext;
import org.apache.iotdb.library.match.model.PatternResult;
import org.apache.iotdb.library.match.model.Point;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.udf.api.exception.UDFAttributeNotProvidedException;
import org.apache.iotdb.udf.api.exception.UDFInputSeriesNumberNotValidException;
import org.apache.iotdb.udf.api.exception.UDFParameterNotValidException;
import org.apache.iotdb.udf.api.type.Type;

import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class UDAFPatternTest {
  private final PatternExecutor executor = new PatternExecutor();

  @Test
  public void testPatternExecutor() {
    List<Point> sourcePoints = new ArrayList<>();
    List<Point> queryPoints = new ArrayList<>();

    try (InputStream input = getClass().getClassLoader().getResourceAsStream("patternData")) {
      try (BufferedReader reader = new BufferedReader(new InputStreamReader(input))) {
        String line;
        while ((line = reader.readLine()) != null) {
          if (line.startsWith("#") || StringUtils.isEmpty(line)) {
            continue;
          }
          sourcePoints.add(
              new Point(
                  Double.parseDouble(line.split(",")[0]), Double.parseDouble(line.split(",")[1])));
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    try (InputStream input = getClass().getClassLoader().getResourceAsStream("patternPart")) {
      try (BufferedReader reader = new BufferedReader(new InputStreamReader(input))) {
        String line;
        while ((line = reader.readLine()) != null) {
          if (line.startsWith("#") || StringUtils.isEmpty(line)) {
            continue;
          }
          queryPoints.add(
              new Point(
                  Double.parseDouble(line.split(",")[0]), Double.parseDouble(line.split(",")[1])));
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    List<Point> sourcePointsExtract = executor.scalePoint(sourcePoints);

    List<Point> queryPointsExtract = executor.extractPoints(queryPoints);

    executor.setPoints(queryPointsExtract);
    PatternContext ctx = new PatternContext();
    ctx.setDataPoints(sourcePointsExtract);
    List<PatternResult> results = executor.executeQuery(ctx);
    Assert.assertNotNull(results);
    Assert.assertEquals(1, results.size());
  }

  @Test
  public void testParameterValidator() {
    UDAFPatternMatch patternMatch = new UDAFPatternMatch();
    List<String> stringList = new ArrayList<>();
    List<Type> typeList = new ArrayList<>();
    Map<String, String> userAttributes = new HashMap<>();
    userAttributes.put("timePattern", "1,2,3");
    userAttributes.put("valuePattern", "1.0,2.0");
    userAttributes.put("threshold", "100");

    UDFParameterValidator validator =
        new UDFParameterValidator(new UDFParameters(stringList, typeList, userAttributes));

    Assert.assertThrows(
        UDFInputSeriesNumberNotValidException.class, () -> patternMatch.validate(validator));

    stringList.add("s1");
    typeList.add(Type.FLOAT);
    userAttributes.clear();
    Assert.assertThrows(
        "Illegal parameter, timePattern must be long,long...",
        UDFParameterNotValidException.class,
        () -> patternMatch.validate(validator));

    userAttributes.put("timePattern", "1,3,2");
    Assert.assertThrows(
        "Illegal parameter, valuePattern must be double,double...",
        UDFParameterNotValidException.class,
        () -> patternMatch.validate(validator));

    userAttributes.put("valuePattern", "1.0,2.0");
    Assert.assertThrows(
        "Illegal parameter, timePattern size must equals valuePattern size",
        UDFParameterNotValidException.class,
        () -> patternMatch.validate(validator));

    userAttributes.remove("valuePattern");
    userAttributes.put("valuePattern", "1.0,2.0,3.0");

    Assert.assertThrows(
        "Illegal parameter, timePattern value must be in ascending order.",
        UDFParameterNotValidException.class,
        () -> patternMatch.validate(validator));

    userAttributes.remove("timePattern");
    userAttributes.put("timePattern", "1,2,3");

    Assert.assertThrows(
        "attribute threshold is required but was not provided.",
        UDFAttributeNotProvidedException.class,
        () -> patternMatch.validate(validator));

    userAttributes.put("threshold", "100");

    try {
      patternMatch.validate(validator);
    } catch (Exception e) {
      Assert.fail("Should not throw exception");
    }
  }
}
