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

package org.apache.iotdb.db.pipe.pattern;

import org.apache.iotdb.commons.pipe.datastructure.pattern.IoTDBTreePattern;
import org.apache.iotdb.pipe.api.exception.PipeException;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.StringArrayDeviceID;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.iotdb.commons.pipe.datastructure.pattern.IoTDBTreePattern.applyReversedIndexesOnList;

public class IoTDBTreePatternTest {

  @Test
  public void testIotdbPipePattern() {
    // Test legal and illegal pattern
    final String[] legalPatterns = {
      "root", "root.db", "root.db.d1.s", "root.db.`1`", "root.*.d.*s.s",
    };
    final String[] illegalPatterns = {
      "root.", "roo", "", "root..", "root./",
    };
    for (final String s : legalPatterns) {
      Assert.assertTrue(new IoTDBTreePattern(s).isLegal());
    }
    for (final String t : illegalPatterns) {
      try {
        Assert.assertFalse(new IoTDBTreePattern(t).isLegal());
      } catch (final Exception e) {
        Assert.assertTrue(e instanceof PipeException);
      }
    }

    // Test pattern cover db
    final String db = "root.db";
    final String[] patternsCoverDb = {
      "root.**", "root.db.**", "root.*db*.**",
    };
    final String[] patternsNotCoverDb = {
      "root.db", "root.*", "root.*.*", "root.db.*.**", "root.db.d1", "root.**.db.**",
    };
    for (final String s : patternsCoverDb) {
      Assert.assertTrue(new IoTDBTreePattern(s).coversDb(db));
    }
    for (final String t : patternsNotCoverDb) {
      Assert.assertFalse(new IoTDBTreePattern(t).coversDb(db));
    }

    final IDeviceID device = new StringArrayDeviceID("root.db.d1");

    // Test pattern cover device
    final String[] patternsCoverDevice = {
      "root.**", "root.db.**", "root.*.*.*", "root.db.d1.*", "root.*db*.*d*.*", "root.**.*1.*",
    };
    final String[] patternsNotCoverDevice = {
      "root.*", "root.*.*", "root.db.d1", "root.db.d2.*", "root.**.d2.**",
    };
    for (final String s : patternsCoverDevice) {
      Assert.assertTrue(new IoTDBTreePattern(s).coversDevice(device));
    }
    for (String t : patternsNotCoverDevice) {
      Assert.assertFalse(new IoTDBTreePattern(t).coversDevice(device));
    }

    // Test pattern may overlap with device
    final String[] patternsOverlapWithDevice = {
      "root.db.**", "root.db.d1", "root.db.d1.*", "root.db.d1.s1", "root.**.d2.**", "root.*.d*.**",
    };
    final String[] patternsNotOverlapWithDevice = {
      "root.db.d2.**", "root.db2.d1.**", "root.db.db.d1.**",
    };
    for (final String s : patternsOverlapWithDevice) {
      Assert.assertTrue(new IoTDBTreePattern(s).mayOverlapWithDevice(device));
    }
    for (final String t : patternsNotOverlapWithDevice) {
      Assert.assertFalse(new IoTDBTreePattern(t).mayOverlapWithDevice(device));
    }

    // Test pattern match measurement
    final String measurement = "s1";
    final String[] patternsMatchMeasurement = {
      "root.db.d1.s1", "root.db.d1.*",
    };
    final String[] patternsNotMatchMeasurement = {
      "root.db.d1", "root.db.d1", "root.db.d1.*.*",
    };
    for (final String s : patternsMatchMeasurement) {
      Assert.assertTrue(new IoTDBTreePattern(s).matchesMeasurement(device, measurement));
    }
    for (final String t : patternsNotMatchMeasurement) {
      Assert.assertFalse(new IoTDBTreePattern(t).matchesMeasurement(device, measurement));
    }
  }

  @Test
  public void testApplyReversedIndexes() {
    final int elementNum = 10_000_000;
    final int filteredNum = elementNum / 10;
    final Random random = new Random();
    final List<Integer> originalList =
        IntStream.range(0, elementNum).boxed().collect(Collectors.toList());
    List<Integer> filteredIndexes = new ArrayList<>(filteredNum);
    for (int i = 0; i < filteredNum; i++) {
      filteredIndexes.add(random.nextInt(elementNum));
    }
    filteredIndexes = filteredIndexes.stream().sorted().distinct().collect(Collectors.toList());

    final long start = System.currentTimeMillis();
    final List<Integer> appliedList = applyReversedIndexesOnList(filteredIndexes, originalList);
    System.out.println(System.currentTimeMillis() - start);
    final Set<Integer> appliedSet = new HashSet<>(appliedList);
    for (Integer filteredIndex : filteredIndexes) {
      if (appliedSet.contains(filteredIndex)) {
        System.out.println("Incorrect implementation");
        System.exit(-1);
      }
    }
    Assert.assertEquals(null, applyReversedIndexesOnList(filteredIndexes, null));
  }
}
