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

import org.apache.iotdb.db.pipe.pattern.matcher.IotdbPatternMatcher;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class IotdbPatternMatcherTest {

  private IotdbPatternMatcher matcher;

  @Before
  public void setUp() {
    matcher = new IotdbPatternMatcher();
  }

  @After
  public void tearDown() {}

  @Test
  public void testIotdbMatcher() {
    // Test legal and illegal pattern
    Assert.assertTrue(matcher.patternIsLegal("root"));
    Assert.assertTrue(matcher.patternIsLegal("root.db"));
    Assert.assertTrue(matcher.patternIsLegal("root.db.d1.s"));
    Assert.assertTrue(matcher.patternIsLegal("root.db.`1`"));
    Assert.assertTrue(matcher.patternIsLegal("root.**.d*.*s.*s*"));

    Assert.assertFalse(matcher.patternIsLegal("root."));
    Assert.assertFalse(matcher.patternIsLegal("roo"));
    Assert.assertFalse(matcher.patternIsLegal(""));
    Assert.assertFalse(matcher.patternIsLegal("root.."));
    Assert.assertFalse(matcher.patternIsLegal("root./"));

    // Test pattern cover device
    String device = "root.db.d1";

    Assert.assertTrue(matcher.patternCoverDevice("root.**", device));
    Assert.assertTrue(matcher.patternCoverDevice("root.db.**", device));
    Assert.assertTrue(matcher.patternCoverDevice("root.*.*.*", device));
    Assert.assertTrue(matcher.patternCoverDevice("root.db.d1.*", device));
    Assert.assertTrue(matcher.patternCoverDevice("root.*db*.*d*.*", device));
    Assert.assertTrue(matcher.patternCoverDevice("root.**.*1.*", device));

    Assert.assertFalse(matcher.patternCoverDevice("root.*", device));
    Assert.assertFalse(matcher.patternCoverDevice("root.*.*", device));
    Assert.assertFalse(matcher.patternCoverDevice("root.db.d1", device));
    Assert.assertFalse(matcher.patternCoverDevice("root.db.d2.*", device));
    Assert.assertFalse(matcher.patternCoverDevice("root.**.d2.**", device));

    // Test pattern overlap with device
    Assert.assertTrue(matcher.patternMayOverlapWithDevice("root.db.**", device));
    Assert.assertTrue(matcher.patternMayOverlapWithDevice("root.db.d1", device));
    Assert.assertTrue(matcher.patternMayOverlapWithDevice("root.db.d1.*", device));
    Assert.assertTrue(matcher.patternMayOverlapWithDevice("root.db.d1.s1", device));
    Assert.assertTrue(matcher.patternMayOverlapWithDevice("root.**.d2.**", device));

    Assert.assertFalse(matcher.patternMayOverlapWithDevice("root.db.d2.**", device));

    // Test pattern match measurement
    String measurement = "s1";

    Assert.assertTrue(matcher.patternMatchMeasurement("root.db.d1.s1", device, measurement));
    Assert.assertTrue(matcher.patternMatchMeasurement("root.db.d1.*", device, measurement));

    Assert.assertFalse(matcher.patternMatchMeasurement("root.db.d1", device, measurement));
    Assert.assertFalse(matcher.patternMatchMeasurement("root.db.d1.s11", device, measurement));
    Assert.assertFalse(matcher.patternMatchMeasurement("root.db.d1.*.*", device, measurement));
  }
}
