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
import org.apache.iotdb.commons.pipe.datastructure.pattern.PrefixTreePattern;
import org.apache.iotdb.commons.pipe.datastructure.pattern.TreePattern;
import org.apache.iotdb.commons.pipe.datastructure.pattern.UnionIoTDBTreePattern;
import org.apache.iotdb.pipe.api.exception.PipeException;

import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertThrows;

/**
 * Test class for {@link TreePattern#checkAndLogPatternCoverage(TreePattern, TreePattern)}. It
 * verifies the returned counts for different coverage scenarios.
 */
public class TreePatternCoverageTest {

  // ========================================================================
  // Helper Methods
  // ========================================================================

  private IoTDBTreePattern iotdb(final String pattern) {
    return new IoTDBTreePattern(true, pattern);
  }

  private PrefixTreePattern prefix(final String pattern) {
    return new PrefixTreePattern(true, pattern);
  }

  private UnionIoTDBTreePattern union(final IoTDBTreePattern... patterns) {
    return new UnionIoTDBTreePattern(true, Arrays.asList(patterns));
  }

  // ========================================================================
  // Test Cases
  // ========================================================================

  @Test
  public void testFullCoverageIoTDB() {
    final TreePattern inclusion = iotdb("root.a.b.c");
    final TreePattern exclusion = iotdb("root.a.**");

    // Verify that a PipeException is thrown
    final PipeException e =
        assertThrows(
            PipeException.class,
            () -> TreePattern.checkAndLogPatternCoverage(inclusion, exclusion));
  }

  @Test
  public void testPartialCoverageIoTDB() {
    final TreePattern inclusion =
        union(
            iotdb("root.a.b.c"), // covered
            iotdb("root.x.y.z") // not covered
            );
    final TreePattern exclusion = iotdb("root.a.**"); // only covers the first one

    // 2 inclusion paths, 1 covered
    final int[] counts = TreePattern.checkAndLogPatternCoverage(inclusion, exclusion);
    assertArrayEquals(new int[] {1, 2}, counts);
  }

  @Test
  public void testNoCoverage() {
    final TreePattern inclusion = iotdb("root.a.b.c");
    final TreePattern exclusion = iotdb("root.x.y.z");

    // 1 inclusion path, 0 covered
    final int[] counts = TreePattern.checkAndLogPatternCoverage(inclusion, exclusion);
    assertArrayEquals(new int[] {0, 1}, counts);
  }

  @Test
  public void testFullCoveragePrefix() {
    // Prefix "root.a.b" corresponds to 4 variants
    // (root.a.b, root.a.b*, root.a.b.**, root.a.b*.**)
    final TreePattern inclusion = prefix("root.a.b");
    // IoTDB "root.a.**" should cover all 4 variants
    final TreePattern exclusion = iotdb("root.a.**");

    // Verify that a PipeException is thrown
    final PipeException e =
        assertThrows(
            PipeException.class,
            () -> TreePattern.checkAndLogPatternCoverage(inclusion, exclusion));
  }

  @Test
  public void testPartialCoveragePrefix() {
    // Prefix "root.a.b" corresponds to 4 variants
    final TreePattern inclusion = prefix("root.a.b");
    // IoTDB "root.a.b" only includes the exact "root.a.b" path
    final TreePattern exclusion = iotdb("root.a.b");

    // 4 inclusion paths, 1 covered
    final int[] counts = TreePattern.checkAndLogPatternCoverage(inclusion, exclusion);
    assertArrayEquals(new int[] {1, 4}, counts);
  }

  @Test
  public void testPrefixNoCoverage() {
    final TreePattern inclusion = prefix("root.a.b");
    final TreePattern exclusion = iotdb("root.x.y.z");

    // 4 inclusion paths, 0 covered
    final int[] counts = TreePattern.checkAndLogPatternCoverage(inclusion, exclusion);
    assertArrayEquals(new int[] {0, 4}, counts);
  }
}
