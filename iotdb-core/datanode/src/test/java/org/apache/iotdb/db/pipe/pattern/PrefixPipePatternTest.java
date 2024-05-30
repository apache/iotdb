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

import org.apache.iotdb.commons.pipe.pattern.PrefixPipePattern;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.StringArrayDeviceID;
import org.junit.Assert;
import org.junit.Test;

public class PrefixPipePatternTest {

  @Test
  public void testPrefixPipePattern() {
    // Test legal and illegal pattern
    final String[] legalPatterns = {
      "root", "root.", "root.db", "root.db.d1.s", "root.db.`1`",
    };
    final String[] illegalPatterns = {
      "roo", "", "root..", "root./",
    };
    for (final String s : legalPatterns) {
      Assert.assertTrue(new PrefixPipePattern(s).isLegal());
    }
    for (final String t : illegalPatterns) {
      Assert.assertFalse(new PrefixPipePattern(t).isLegal());
    }

    // Test pattern cover db
    final String db = "root.db";
    final String[] patternsCoverDb = {
      "root", "root.", "root.d", "root.db",
    };
    final String[] patternsNotCoverDb = {
      "root.**", "root.db.",
    };
    for (final String s : patternsCoverDb) {
      Assert.assertTrue(new PrefixPipePattern(s).coversDb(db));
    }
    for (final String t : patternsNotCoverDb) {
      Assert.assertFalse(new PrefixPipePattern(t).coversDb(db));
    }

    final IDeviceID device = new StringArrayDeviceID("root.db.d1");

    // Test pattern cover device
    final String[] patternsCoverDevice = {
      "root", "root.", "root.d", "root.db", "root.db.", "root.db.d", "root.db.d1",
    };
    final String[] patternsNotCoverDevice = {
      "root.db.d1.", "root.db.d1.s1", "root.**", "root.db.d2",
    };
    for (final String s : patternsCoverDevice) {
      Assert.assertTrue(new PrefixPipePattern(s).coversDevice(device));
    }
    for (final String t : patternsNotCoverDevice) {
      Assert.assertFalse(new PrefixPipePattern(t).coversDevice(device));
    }

    // Test pattern may overlap with device
    final String[] patternsOverlapWithDevice = {
      "root", "root.db.d1", "root.db.d1.", "root.db.d1.s1",
    };
    final String[] patternsNotOverlapWithDevice = {
      "root.db.d2", "root.**",
    };
    for (final String s : patternsOverlapWithDevice) {
      Assert.assertTrue(new PrefixPipePattern(s).mayOverlapWithDevice(device));
    }
    for (final String t : patternsNotOverlapWithDevice) {
      Assert.assertFalse(new PrefixPipePattern(t).mayOverlapWithDevice(device));
    }

    // Test pattern match measurement
    final String measurement = "s1";
    final String[] patternsMatchMeasurement = {
      "root.db.d1", "root.db.d1.", "root.db.d1.s", "root.db.d1.s1",
    };
    final String[] patternsNotMatchMeasurement = {
      "root.db.d1.s11", "root.db.d1.s2",
    };
    for (final String s : patternsMatchMeasurement) {
      Assert.assertTrue(new PrefixPipePattern(s).matchesMeasurement(device, measurement));
    }
    for (final String t : patternsNotMatchMeasurement) {
      Assert.assertFalse(new PrefixPipePattern(t).matchesMeasurement(device, measurement));
    }
  }
}
