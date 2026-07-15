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

package org.apache.iotdb.commons.pipe.config.constant;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.commons.pipe.config.PipeSourceTreePatternUtils;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.exception.PipeException;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PipeSourceConstantTest {

  @Test
  public void testValidateUserProvidedSourceAttributesRejectsInternalPatternKeys() {
    final Map<String, String> attributes = new HashMap<>();
    attributes.put(
        PipeSourceConstant.SOURCE_INTERNAL_PATTERN_INCLUSION_KEY, "root.db.d1.s1_physical");
    try {
      PipeSourceConstant.validateUserProvidedSourceAttributes(attributes, "p", false);
      Assert.fail("Expected PipeException");
    } catch (final PipeException e) {
      Assert.assertTrue(e.getMessage().contains("not allowed"));
      Assert.assertTrue(
          e.getMessage().contains(PipeSourceConstant.SOURCE_INTERNAL_PATTERN_INCLUSION_KEY));
      Assert.assertFalse(e.getMessage().contains("******"));
    }
  }

  @Test
  public void testValidateUserProvidedSourceAttributesShowsSensitiveForbiddenKeys() {
    final Map<String, String> attributes = new HashMap<>();
    attributes.put("__system.source.password", "secret");
    try {
      PipeSourceConstant.validateUserProvidedSourceAttributes(attributes, "p", false);
      Assert.fail("Expected PipeException");
    } catch (final PipeException e) {
      Assert.assertTrue(e.getMessage().contains("__system.source.password"));
      Assert.assertFalse(e.getMessage().contains("secret"));
    }
  }

  @Test
  public void testValidateUserProvidedInternalSourcePatternAttributesAllowsSystemDialect() {
    final Map<String, String> attributes = new HashMap<>();
    attributes.put(SystemConstant.SQL_DIALECT_KEY, SystemConstant.SQL_DIALECT_TREE_VALUE);
    PipeSourceConstant.validateUserProvidedInternalSourcePatternAttributes(attributes, "p", false);
  }

  @Test
  public void testStripInternalSourceAttributes() {
    final Map<String, String> attributes = new HashMap<>();
    attributes.put(PipeSourceConstant.SOURCE_PATH_KEY, "root.db.d1.s1");
    attributes.put(
        PipeSourceConstant.SOURCE_INTERNAL_PATTERN_EXCLUSION_KEY, "root.db.d1.s2_physical");
    PipeSourceConstant.stripInternalSourceAttributes(attributes);
    Assert.assertTrue(attributes.containsKey(PipeSourceConstant.SOURCE_PATH_KEY));
    Assert.assertFalse(
        attributes.containsKey(PipeSourceConstant.SOURCE_INTERNAL_PATTERN_EXCLUSION_KEY));
  }

  @Test
  public void testHasTreePatternSourceAttributesIncludesInternalKeys() {
    final Map<String, String> attributes = new HashMap<>();
    attributes.put(
        PipeSourceConstant.SOURCE_INTERNAL_PATTERN_INCLUSION_KEY, "root.db.d1.s1_physical");
    Assert.assertTrue(
        PipeSourceTreePatternUtils.hasTreePatternSourceAttributes(new PipeParameters(attributes)));
  }

  @Test
  public void testMayDatabaseOverlapInternalInclusion() {
    final Map<String, String> attributes = new HashMap<>();
    attributes.put(
        PipeSourceConstant.SOURCE_INTERNAL_PATTERN_INCLUSION_KEY,
        "root.other_db.d1.s1_physical,root.db.d2.s2_physical");
    final PipeParameters parameters = new PipeParameters(attributes);
    final PathPatternTree inclusionPathPatternTree =
        PipeSourceTreePatternUtils.parseInternalInclusionPathPatternTree(parameters);
    Assert.assertTrue(
        PipeSourceTreePatternUtils.mayDatabaseOverlapInternalInclusion("db", parameters));
    Assert.assertTrue(
        PipeSourceTreePatternUtils.mayDatabaseOverlapInternalInclusion("root.db", parameters));
    Assert.assertTrue(
        PipeSourceTreePatternUtils.mayDatabaseOverlapInternalInclusion(
            "db", inclusionPathPatternTree));
    Assert.assertTrue(
        PipeSourceTreePatternUtils.mayDatabaseOverlapInternalInclusion(
            "root.db", inclusionPathPatternTree));
    Assert.assertFalse(
        PipeSourceTreePatternUtils.mayDatabaseOverlapInternalInclusion("another_db", parameters));
    Assert.assertFalse(
        PipeSourceTreePatternUtils.mayDatabaseOverlapInternalInclusion(
            "another_db", inclusionPathPatternTree));

    attributes.put(PipeSourceConstant.SOURCE_INTERNAL_PATTERN_INCLUSION_KEY, "root.db.**");
    final PathPatternTree wildcardInclusionPathPatternTree =
        PipeSourceTreePatternUtils.parseInternalInclusionPathPatternTree(
            new PipeParameters(attributes));
    Assert.assertTrue(
        PipeSourceTreePatternUtils.mayDatabaseOverlapInternalInclusion(
            "root.db", new PipeParameters(attributes)));
    Assert.assertTrue(
        PipeSourceTreePatternUtils.mayDatabaseOverlapInternalInclusion(
            "root.db", wildcardInclusionPathPatternTree));
    Assert.assertFalse(
        PipeSourceTreePatternUtils.mayDatabaseOverlapInternalInclusion(
            "root.other_db", new PipeParameters(attributes)));
    Assert.assertFalse(
        PipeSourceTreePatternUtils.mayDatabaseOverlapInternalInclusion(
            "root.other_db", wildcardInclusionPathPatternTree));
  }

  @Test
  public void testInternalInclusionPathListSupportsCommaInBackquotedNode() {
    final Map<String, String> attributes = new HashMap<>();
    attributes.put(
        PipeSourceConstant.SOURCE_INTERNAL_PATTERN_INCLUSION_KEY,
        "root.other_db.d1.s1_physical,root.db.d2.`s,2`");
    final PipeParameters parameters = new PipeParameters(attributes);

    Assert.assertEquals(
        2, PipeSourceTreePatternUtils.getInternalInclusionPathList(parameters).size());
    Assert.assertTrue(
        PipeSourceTreePatternUtils.mayDatabaseOverlapInternalInclusion("root.db", parameters));
  }

  @Test
  public void testSplitPatternListSupportsCommaInBackquotedNode() {
    final List<String> patterns =
        PipeSourceTreePatternUtils.splitPatternList(
            " root.db.d1.s1 , root.db.d2.`s,2` , root.db.d3.`s``3` ");

    Assert.assertEquals(3, patterns.size());
    Assert.assertEquals("root.db.d1.s1", patterns.get(0));
    Assert.assertEquals("root.db.d2.`s,2`", patterns.get(1));
    Assert.assertEquals("root.db.d3.`s``3`", patterns.get(2));
  }

  @Test
  public void testSplitPatternListSupportsMultipleQuotedSpecialNodes() {
    final List<String> patterns =
        PipeSourceTreePatternUtils.splitPatternList(
            "root.`db,1`.`d``1`.`s,1`,root.`db``2`.`d,2`.s2,root.db3.`d```");

    Assert.assertEquals(3, patterns.size());
    Assert.assertEquals("root.`db,1`.`d``1`.`s,1`", patterns.get(0));
    Assert.assertEquals("root.`db``2`.`d,2`.s2", patterns.get(1));
    Assert.assertEquals("root.db3.`d```", patterns.get(2));
  }

  @Test
  public void testSplitPatternListReturnsEmptyForBlankInput() {
    Assert.assertTrue(PipeSourceTreePatternUtils.splitPatternList(null).isEmpty());
    Assert.assertTrue(PipeSourceTreePatternUtils.splitPatternList("").isEmpty());
    Assert.assertTrue(PipeSourceTreePatternUtils.splitPatternList("   ").isEmpty());
  }

  @Test
  public void testSplitPatternListRejectsMalformedList() {
    Assert.assertThrows(
        PipeException.class, () -> PipeSourceTreePatternUtils.splitPatternList("root.db.d1.s1,"));
    Assert.assertThrows(
        PipeException.class, () -> PipeSourceTreePatternUtils.splitPatternList("root.db.`d1`,"));
    Assert.assertThrows(
        PipeException.class, () -> PipeSourceTreePatternUtils.splitPatternList("root.db.`d,1.`,"));
    Assert.assertThrows(
        PipeException.class, () -> PipeSourceTreePatternUtils.splitPatternList(",root.db.d1.s1"));
    Assert.assertThrows(
        PipeException.class,
        () -> PipeSourceTreePatternUtils.splitPatternList("root.db.d1.s1,,root.db.d2.s2"));
    Assert.assertThrows(
        PipeException.class, () -> PipeSourceTreePatternUtils.splitPatternList("root.db.d1.`s1"));
    Assert.assertThrows(
        PipeException.class, () -> PipeSourceTreePatternUtils.splitPatternList("root.db.`d1`,,"));
    Assert.assertThrows(
        PipeException.class, () -> PipeSourceTreePatternUtils.splitPatternList("root.db.`d``1"));
  }

  @Test
  public void testQuotePathIfNecessaryEscapesSpecialNodes() {
    Assert.assertEquals(
        "root.db.d1.s1", PipeSourceTreePatternUtils.quotePathIfNecessary("root.db.d1.s1"));

    final String quotedPath =
        PipeSourceTreePatternUtils.quotePathIfNecessary(
            new PartialPath(new String[] {"root", "db", "d1", "s,`1", "select", "1e2"}));
    Assert.assertEquals("root.db.d1.`s,``1`.select.`1e2`", quotedPath);

    final List<String> patterns =
        PipeSourceTreePatternUtils.splitPatternList("root.db.d1.s1," + quotedPath);
    Assert.assertEquals(2, patterns.size());
    Assert.assertEquals(quotedPath, patterns.get(1));
  }

  @Test
  public void testQuotePathIfNecessaryRoundTripForInternalPatternList() {
    final String pathWithComma =
        PipeSourceTreePatternUtils.quotePathIfNecessary(
            new PartialPath(new String[] {"root", "db,1", "d1", "s1"}));
    final String pathWithBackquote =
        PipeSourceTreePatternUtils.quotePathIfNecessary(
            new PartialPath(new String[] {"root", "db2", "d`2", "s2"}));
    final String pathWithRealNumber =
        PipeSourceTreePatternUtils.quotePathIfNecessary(
            new PartialPath(new String[] {"root", "db3", "1", "1.5"}));
    final List<String> patterns =
        PipeSourceTreePatternUtils.splitPatternList(
            pathWithComma + "," + pathWithBackquote + "," + pathWithRealNumber);

    Assert.assertEquals(3, patterns.size());
    Assert.assertEquals("root.`db,1`.d1.s1", patterns.get(0));
    Assert.assertEquals("root.db2.`d``2`.s2", patterns.get(1));
    Assert.assertEquals("root.db3.`1`.`1.5`", patterns.get(2));
  }

  @Test
  public void testQuotePathIfNecessaryKeepsWildcardNodes() {
    Assert.assertEquals("root.db.*", PipeSourceTreePatternUtils.quotePathIfNecessary("root.db.*"));
    Assert.assertEquals(
        "root.db.**", PipeSourceTreePatternUtils.quotePathIfNecessary("root.db.**"));
    Assert.assertEquals(
        "root.db.d*.s",
        PipeSourceTreePatternUtils.quotePathIfNecessary(
            new PartialPath(new String[] {"root", "db", "d*", "s"})));
  }

  @Test
  public void testSplitPatternListSupportsQuotedWildcardAndEscapedBackquote() {
    final List<String> patterns =
        PipeSourceTreePatternUtils.splitPatternList("root.db.`d*`.s,root.db.`d``*`.s");

    Assert.assertEquals(2, patterns.size());
    Assert.assertEquals("root.db.`d*`.s", patterns.get(0));
    Assert.assertEquals("root.db.`d``*`.s", patterns.get(1));
  }

  @Test
  public void testInternalInclusionPathListRejectsMalformedList() {
    final Map<String, String> attributes = new HashMap<>();
    attributes.put(PipeSourceConstant.SOURCE_INTERNAL_PATTERN_INCLUSION_KEY, "root.db.d1.s1,");

    Assert.assertThrows(
        PipeException.class,
        () ->
            PipeSourceTreePatternUtils.getInternalInclusionPathList(
                new PipeParameters(attributes)));

    attributes.put(PipeSourceConstant.SOURCE_INTERNAL_PATTERN_INCLUSION_KEY, "root.db.d1.`s1");
    Assert.assertThrows(
        PipeException.class,
        () ->
            PipeSourceTreePatternUtils.getInternalInclusionPathList(
                new PipeParameters(attributes)));
  }

  @Test
  public void testInternalInclusionPathListSupportsQuotedCommaAndBackquote() {
    final Map<String, String> attributes = new HashMap<>();
    attributes.put(
        PipeSourceConstant.SOURCE_INTERNAL_PATTERN_INCLUSION_KEY,
        "root.`db,1`.d1.s1,root.db2.`d``2`.s2");

    final List<String> inclusionPaths =
        PipeSourceTreePatternUtils.getInternalInclusionPathList(new PipeParameters(attributes));
    Assert.assertEquals(2, inclusionPaths.size());
    Assert.assertEquals("root.`db,1`.d1.s1", inclusionPaths.get(0));
    Assert.assertEquals("root.db2.`d``2`.s2", inclusionPaths.get(1));
  }
}
