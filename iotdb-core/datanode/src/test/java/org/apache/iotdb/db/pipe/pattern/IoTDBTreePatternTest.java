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

import java.util.Arrays;
import java.util.Collections;
import java.util.function.Predicate;

import static org.apache.iotdb.commons.pipe.datastructure.pattern.IoTDBTreePattern.applyReversedIndexesOnList;

public class IoTDBTreePatternTest {

  private static final String DB = "root.db";
  private static final IDeviceID DEVICE = new StringArrayDeviceID("root.db.d1");
  private static final String MEASUREMENT = "s1";

  @Test
  public void testLegalPattern() {
    assertLegal("root", "root.db", "root.db.d1.s", "root.db.`1`", "root.*.d.*s.s", null);
    assertIllegalOrInvalid("root.", "roo", "", "root..", "root./");
  }

  @Test
  public void testRootPattern() {
    Assert.assertTrue(new IoTDBTreePattern(null).isRoot());
    Assert.assertTrue(new IoTDBTreePattern("root.**").isRoot());
    Assert.assertFalse(new IoTDBTreePattern("root").isRoot());
  }

  @Test
  public void testCoversDb() {
    assertPatternResult(
        true, pattern -> pattern.coversDb(DB), "root.**", "root.db.**", "root.*db*.**");
    assertPatternResult(
        false,
        pattern -> pattern.coversDb(DB),
        "root.db",
        "root.*",
        "root.*.*",
        "root.db.*.**",
        "root.db.d1",
        "root.**.db.**");
  }

  @Test
  public void testMayOverlapWithDb() {
    assertPatternResult(
        true,
        pattern -> pattern.mayOverlapWithDb(DB),
        "root.**",
        "root.db.**",
        "root.db.d1",
        "root.*.d1");
    assertPatternResult(
        false,
        pattern -> pattern.mayOverlapWithDb(DB),
        "root.other.**",
        "root.other.d1",
        "root.db2.d1");
  }

  @Test
  public void testCoversDevice() {
    assertPatternResult(
        true,
        pattern -> pattern.coversDevice(DEVICE),
        "root.**",
        "root.db.**",
        "root.*.*.*",
        "root.db.d1.*",
        "root.*db*.*d*.*",
        "root.**.*1.*");
    assertPatternResult(
        false,
        pattern -> pattern.coversDevice(DEVICE),
        "root.*",
        "root.*.*",
        "root.db.d1",
        "root.db.d2.*",
        "root.**.d2.**");
  }

  @Test
  public void testOverlapsDevice() {
    assertPatternResult(
        true,
        pattern -> pattern.mayOverlapWithDevice(DEVICE),
        "root.db.**",
        "root.db.d1",
        "root.db.d1.*",
        "root.db.d1.s1",
        "root.**.d2.**",
        "root.*.d*.**");
    assertPatternResult(
        false,
        pattern -> pattern.mayOverlapWithDevice(DEVICE),
        "root.db.d2.**",
        "root.db2.d1.**",
        "root.db.db.d1.**");

    final IoTDBTreePattern falsePositivePattern = new IoTDBTreePattern("root.**.d2.**");
    Assert.assertTrue(falsePositivePattern.mayOverlapWithDevice(DEVICE));
    Assert.assertFalse(falsePositivePattern.overlapWithDevice(DEVICE));
  }

  @Test
  public void testMatchesMeasurement() {
    assertPatternResult(
        true,
        pattern -> pattern.matchesMeasurement(DEVICE, MEASUREMENT),
        "root.db.d1.s1",
        "root.db.d1.*");
    assertPatternResult(
        false,
        pattern -> pattern.matchesMeasurement(DEVICE, MEASUREMENT),
        "root.db.d1",
        "root.db.d1.*.*",
        "root.db.d2.s1");

    final IoTDBTreePattern matchAllMeasurements = new IoTDBTreePattern("root.db.d1.*");
    Assert.assertFalse(matchAllMeasurements.matchesMeasurement(DEVICE, null));
    Assert.assertFalse(matchAllMeasurements.matchesMeasurement(DEVICE, ""));
  }

  @Test
  public void testSchemaPatternHelpers() {
    final IoTDBTreePattern prefixPattern = new IoTDBTreePattern("root.db.**");
    Assert.assertTrue(prefixPattern.matchPrefixPath("root.db"));
    Assert.assertFalse(prefixPattern.matchPrefixPath("root.other"));
    Assert.assertTrue(prefixPattern.matchTailNode("any"));
    Assert.assertTrue(prefixPattern.isPrefixOrFullPath());
    Assert.assertTrue(prefixPattern.mayMatchMultipleTimeSeriesInOneDevice());

    final IoTDBTreePattern fullPathPattern = new IoTDBTreePattern("root.db.d1.s1");
    Assert.assertTrue(fullPathPattern.matchTailNode("s1"));
    Assert.assertFalse(fullPathPattern.matchTailNode("s2"));
    Assert.assertTrue(fullPathPattern.isPrefixOrFullPath());
    Assert.assertFalse(fullPathPattern.mayMatchMultipleTimeSeriesInOneDevice());

    final IoTDBTreePattern wildcardMiddlePattern = new IoTDBTreePattern("root.*.**");
    Assert.assertFalse(wildcardMiddlePattern.isPrefixOrFullPath());
  }

  @Test
  public void testMatchDevice() {
    assertPatternResult(
        true, pattern -> pattern.matchDevice("root.db.d1"), "root.db.d1.*", "root.db.**");
    assertPatternResult(
        false, pattern -> pattern.matchDevice("root.db.d1"), "root.db.d2.*", "root.db2.**");
  }

  @Test
  public void testApplyReversedIndexes() {
    Assert.assertEquals(
        Arrays.asList(0, 2, 4),
        applyReversedIndexesOnList(Arrays.asList(1, 3), Arrays.asList(0, 1, 2, 3, 4)));
    Assert.assertEquals(
        Arrays.asList("a", "b"),
        applyReversedIndexesOnList(Collections.emptyList(), Arrays.asList("a", "b")));
    Assert.assertEquals(
        Collections.emptyList(),
        applyReversedIndexesOnList(Arrays.asList(0, 1, 2), Arrays.asList(0, 1, 2)));
    Assert.assertNull(applyReversedIndexesOnList(Collections.singletonList(0), null));
  }

  private static void assertLegal(final String... patterns) {
    for (final String pattern : patterns) {
      Assert.assertTrue(String.valueOf(pattern), new IoTDBTreePattern(pattern).isLegal());
    }
  }

  private static void assertIllegalOrInvalid(final String... patterns) {
    for (final String pattern : patterns) {
      try {
        Assert.assertFalse(String.valueOf(pattern), new IoTDBTreePattern(pattern).isLegal());
      } catch (final Exception e) {
        Assert.assertTrue(e instanceof PipeException);
      }
    }
  }

  private static void assertPatternResult(
      final boolean expected,
      final Predicate<IoTDBTreePattern> assertion,
      final String... patterns) {
    for (final String pattern : patterns) {
      Assert.assertEquals(pattern, expected, assertion.test(new IoTDBTreePattern(pattern)));
    }
  }
}
