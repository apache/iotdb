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

package org.apache.iotdb.commons.pipe.datastructure.pattern;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.exception.PipeException;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.StringArrayDeviceID;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class TreePatternTest {

  @Test
  public void testInternalSourcePatternMatchesByRecordedPhysicalPaths() {
    final Map<String, String> attributes = new HashMap<>();
    attributes.put(PipeSourceConstant.SOURCE_PATTERN_INCLUSION_KEY, "root.db.**");
    attributes.put(
        PipeSourceConstant.SOURCE_INTERNAL_PATTERN_INCLUSION_KEY, "root.outside.d1.s_physical");
    attributes.put(
        PipeSourceConstant.SOURCE_INTERNAL_PATTERN_EXCLUSION_KEY, "root.db.d1.s_invalid");

    final TreePattern pattern =
        TreePattern.parsePipePatternFromSourceParameters(new PipeParameters(attributes));
    final IDeviceID userDevice = new StringArrayDeviceID("root.db.d1");
    final IDeviceID internalDevice = new StringArrayDeviceID("root.outside.d1");

    Assert.assertEquals("root.db.**", pattern.getPattern());
    Assert.assertFalse(pattern.isRoot());
    Assert.assertFalse(pattern.coversDevice(userDevice));
    Assert.assertFalse(pattern.matchesMeasurement(userDevice, "s_invalid"));
    Assert.assertTrue(pattern.matchesMeasurement(userDevice, "s_normal"));
    Assert.assertTrue(pattern.matchesMeasurement(internalDevice, "s_physical"));
    Assert.assertFalse(pattern.matchesMeasurement(internalDevice, "s_other"));
  }

  @Test
  public void testInternalSourceExclusionOverridesInternalSourceInclusion() {
    final Map<String, String> attributes = new HashMap<>();
    attributes.put(PipeSourceConstant.SOURCE_PATTERN_INCLUSION_KEY, "root.db.**");
    attributes.put(
        PipeSourceConstant.SOURCE_INTERNAL_PATTERN_INCLUSION_KEY, "root.db.d1.s_physical");
    attributes.put(
        PipeSourceConstant.SOURCE_INTERNAL_PATTERN_EXCLUSION_KEY, "root.db.d1.s_physical");

    final TreePattern pattern =
        TreePattern.parsePipePatternFromSourceParameters(new PipeParameters(attributes));
    final IDeviceID device = new StringArrayDeviceID("root.db.d1");

    Assert.assertFalse(pattern.matchesMeasurement(device, "s_physical"));
  }

  @Test
  public void testInternalSourcePatternSupportsCommaInBackquotedNode() {
    final Map<String, String> attributes = new HashMap<>();
    attributes.put(PipeSourceConstant.SOURCE_PATTERN_INCLUSION_KEY, "root.db.**");
    attributes.put(PipeSourceConstant.SOURCE_INTERNAL_PATTERN_INCLUSION_KEY, "root.db.d1.`s,1`");

    final TreePattern pattern =
        TreePattern.parsePipePatternFromSourceParameters(new PipeParameters(attributes));

    Assert.assertTrue(pattern.matchesMeasurement(new StringArrayDeviceID("root.db.d1"), "s,1"));
  }

  @Test
  public void testPatternListRejectsMalformedSegments() {
    final Map<String, String> attributes = new HashMap<>();
    attributes.put(PipeSourceConstant.SOURCE_PATTERN_INCLUSION_KEY, "root.db.**,");

    Assert.assertThrows(
        PipeException.class,
        () -> TreePattern.parsePipePatternFromSourceParameters(new PipeParameters(attributes)));

    attributes.put(PipeSourceConstant.SOURCE_PATTERN_INCLUSION_KEY, "root.db.`d1");
    Assert.assertThrows(
        PipeException.class,
        () -> TreePattern.parsePipePatternFromSourceParameters(new PipeParameters(attributes)));
  }

  @Test
  public void testInternalSourcePatternUsesUserPatternForMetadataAndInternalPatternForDataRegion()
      throws Exception {
    final Map<String, String> attributes = new HashMap<>();
    attributes.put(PipeSourceConstant.SOURCE_PATTERN_INCLUSION_KEY, "root.db.**");
    attributes.put(
        PipeSourceConstant.SOURCE_INTERNAL_PATTERN_INCLUSION_KEY, "root.outside.d1.s_physical");
    attributes.put(
        PipeSourceConstant.SOURCE_INTERNAL_PATTERN_EXCLUSION_KEY, "root.db.d1.s_invalid");

    final IoTDBTreePatternOperations pattern =
        (IoTDBTreePatternOperations)
            TreePattern.parsePipePatternFromSourceParameters(new PipeParameters(attributes));

    Assert.assertTrue(pattern.matchPrefixPath("root.db.d1"));
    Assert.assertTrue(pattern.matchPrefixPath("root.db.d1.s_invalid"));
    Assert.assertEquals(
        "root.db.d1.s_invalid",
        pattern.getIntersection(new PartialPath("root.db.d1.s_invalid")).get(0).getFullPath());
    Assert.assertTrue(
        pattern.getIntersection(new PartialPath("root.outside.d1.s_physical")).isEmpty());

    final IDeviceID userDevice = new StringArrayDeviceID("root.db.d1");
    final IDeviceID internalDevice = new StringArrayDeviceID("root.outside.d1");
    Assert.assertFalse(pattern.matchesMeasurement(userDevice, "s_invalid"));
    Assert.assertTrue(pattern.matchesMeasurement(internalDevice, "s_physical"));

    final PathPatternTree sourceTree = new PathPatternTree();
    sourceTree.appendPathPattern(new PartialPath("root.db.d1.s_invalid"));
    sourceTree.appendPathPattern(new PartialPath("root.db.d1.s_normal"));
    sourceTree.appendPathPattern(new PartialPath("root.outside.d1.s_physical"));
    sourceTree.constructTree();

    final List<String> intersections =
        pattern.getIntersection(sourceTree).getAllPathPatterns().stream()
            .map(PartialPath::getFullPath)
            .sorted()
            .collect(Collectors.toList());
    Assert.assertEquals(2, intersections.size());
    Assert.assertEquals("root.db.d1.s_invalid", intersections.get(0));
    Assert.assertEquals("root.db.d1.s_normal", intersections.get(1));
  }
}
