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

import org.apache.iotdb.commons.pipe.pattern.IoTDBPipePattern;
import org.apache.iotdb.pipe.api.exception.PipeException;

import org.junit.Assert;
import org.junit.Test;

public class IoTDBPipePatternTest {

  @Test
  public void testIotdbPipePattern() {
    // Test legal and illegal pattern
    String[] legalPatterns = {
      "root", "root.db", "root.db.d1.s", "root.db.`1`", "root.*.d.*s.s",
    };
    String[] illegalPatterns = {
      "root.", "roo", "", "root..", "root./",
    };
    for (String s : legalPatterns) {
      Assert.assertTrue(new IoTDBPipePattern(s).isLegal());
    }
    for (String t : illegalPatterns) {
      try {
        Assert.assertFalse(new IoTDBPipePattern(t).isLegal());
      } catch (Exception e) {
        Assert.assertTrue(e instanceof PipeException);
      }
    }

    // Test pattern cover db
    String db = "root.db";
    String[] patternsCoverDb = {
      "root.**", "root.db.**", "root.*db*.**",
    };
    String[] patternsNotCoverDb = {
      "root.db", "root.*", "root.*.*", "root.db.*.**", "root.db.d1", "root.**.db.**",
    };
    for (String s : patternsCoverDb) {
      Assert.assertTrue(new IoTDBPipePattern(s).coversDb(db));
    }
    for (String t : patternsNotCoverDb) {
      Assert.assertFalse(new IoTDBPipePattern(t).coversDb(db));
    }

    String device = "root.db.d1";

    // Test pattern cover device
    String[] patternsCoverDevice = {
      "root.**", "root.db.**", "root.*.*.*", "root.db.d1.*", "root.*db*.*d*.*", "root.**.*1.*",
    };
    String[] patternsNotCoverDevice = {
      "root.*", "root.*.*", "root.db.d1", "root.db.d2.*", "root.**.d2.**",
    };
    for (String s : patternsCoverDevice) {
      Assert.assertTrue(new IoTDBPipePattern(s).coversDevice(device));
    }
    for (String t : patternsNotCoverDevice) {
      Assert.assertFalse(new IoTDBPipePattern(t).coversDevice(device));
    }

    // Test pattern may overlap with device
    String[] patternsOverlapWithDevice = {
      "root.db.**", "root.db.d1", "root.db.d1.*", "root.db.d1.s1", "root.**.d2.**", "root.*.d*.**",
    };
    String[] patternsNotOverlapWithDevice = {
      "root.db.d2.**", "root.db2.d1.**", "root.db.db.d1.**",
    };
    for (String s : patternsOverlapWithDevice) {
      Assert.assertTrue(new IoTDBPipePattern(s).matchPrefixPath(device));
    }
    for (String t : patternsNotOverlapWithDevice) {
      Assert.assertFalse(new IoTDBPipePattern(t).matchPrefixPath(device));
    }

    // Test pattern match measurement
    String measurement = "s1";
    String[] patternsMatchMeasurement = {
      "root.db.d1.s1", "root.db.d1.*",
    };
    String[] patternsNotMatchMeasurement = {
      "root.db.d1", "root.db.d1", "root.db.d1.*.*",
    };
    for (String s : patternsMatchMeasurement) {
      Assert.assertTrue(new IoTDBPipePattern(s).matchesMeasurement(device, measurement));
    }
    for (String t : patternsNotMatchMeasurement) {
      Assert.assertFalse(new IoTDBPipePattern(t).matchesMeasurement(device, measurement));
    }
  }
}
