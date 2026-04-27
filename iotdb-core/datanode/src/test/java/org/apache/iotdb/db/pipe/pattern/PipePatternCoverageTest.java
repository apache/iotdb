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

import org.apache.iotdb.commons.pipe.datastructure.pattern.IoTDBPipePattern;
import org.apache.iotdb.commons.pipe.datastructure.pattern.PipePattern;
import org.apache.iotdb.commons.pipe.datastructure.pattern.PrefixPipePattern;
import org.apache.iotdb.commons.pipe.datastructure.pattern.UnionIoTDBPipePattern;
import org.apache.iotdb.pipe.api.exception.PipeException;

import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertThrows;

/**
 * Test class for {@link PipePattern#checkAndLogPatternCoverage(PipePattern, PipePattern)}. It
 * verifies the returned counts for different coverage scenarios.
 */
public class PipePatternCoverageTest {

  // ========================================================================
  // Helper Methods
  // ========================================================================

  private IoTDBPipePattern iotdb(final String pattern) {
    return new IoTDBPipePattern(pattern);
  }

  private PrefixPipePattern prefix(final String pattern) {
    return new PrefixPipePattern(pattern);
  }

  private UnionIoTDBPipePattern union(final IoTDBPipePattern... patterns) {
    return new UnionIoTDBPipePattern(Arrays.asList(patterns));
  }

  // ========================================================================
  // Test Cases
  // ========================================================================

  @Test
  public void testFullCoverageIoTDB() {
    final PipePattern inclusion = iotdb("root.a.b.c");
    final PipePattern exclusion = iotdb("root.a.**");

    // Verify that a PipeException is thrown
    final PipeException e =
        assertThrows(
            PipeException.class,
            () -> PipePattern.checkAndLogPatternCoverage(inclusion, exclusion));
  }

  @Test
  public void testPartialCoverageIoTDB() {
    final PipePattern inclusion =
        union(
            iotdb("root.a.b.c"), // covered
            iotdb("root.x.y.z") // not covered
            );
    final PipePattern exclusion = iotdb("root.a.**"); // only covers the first one

    // 2 inclusion paths, 1 covered
    final int[] counts = PipePattern.checkAndLogPatternCoverage(inclusion, exclusion);
    assertArrayEquals(new int[] {1, 2}, counts);
  }

  @Test
  public void testNoCoverage() {
    final PipePattern inclusion = iotdb("root.a.b.c");
    final PipePattern exclusion = iotdb("root.x.y.z");

    // 1 inclusion path, 0 covered
    final int[] counts = PipePattern.checkAndLogPatternCoverage(inclusion, exclusion);
    assertArrayEquals(new int[] {0, 1}, counts);
  }

  @Test
  public void testFullCoveragePrefix() {
    // Prefix "root.a.b" corresponds to 4 variants
    // (root.a.b, root.a.b*, root.a.b.**, root.a.b*.**)
    final PipePattern inclusion = prefix("root.a.b");
    // IoTDB "root.a.**" should cover all 4 variants
    final PipePattern exclusion = iotdb("root.a.**");

    // Verify that a PipeException is thrown
    final PipeException e =
        assertThrows(
            PipeException.class,
            () -> PipePattern.checkAndLogPatternCoverage(inclusion, exclusion));
  }

  @Test
  public void testPartialCoveragePrefix() {
    // Prefix "root.a.b" corresponds to 4 variants
    final PipePattern inclusion = prefix("root.a.b");
    // IoTDB "root.a.b" only includes the exact "root.a.b" path
    final PipePattern exclusion = iotdb("root.a.b");

    // 4 inclusion paths, 1 covered
    final int[] counts = PipePattern.checkAndLogPatternCoverage(inclusion, exclusion);
    assertArrayEquals(new int[] {1, 4}, counts);
  }

  @Test
  public void testPrefixNoCoverage() {
    final PipePattern inclusion = prefix("root.a.b");
    final PipePattern exclusion = iotdb("root.x.y.z");

    // 4 inclusion paths, 0 covered
    final int[] counts = PipePattern.checkAndLogPatternCoverage(inclusion, exclusion);
    assertArrayEquals(new int[] {0, 4}, counts);
  }
}
