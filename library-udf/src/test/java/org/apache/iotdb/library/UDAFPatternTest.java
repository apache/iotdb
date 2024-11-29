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
import org.apache.iotdb.library.match.model.PatternContext;
import org.apache.iotdb.library.match.model.PatternResult;
import org.apache.iotdb.library.match.model.Point;

import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

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
}
