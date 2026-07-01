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

package org.apache.iotdb.commons.pipe.datastructure.visibility;

import org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant;
import org.apache.iotdb.commons.pipe.config.constant.SystemConstant;
import org.apache.iotdb.commons.pipe.datastructure.pattern.TablePattern;
import org.apache.iotdb.commons.pipe.datastructure.pattern.TreePattern;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class VisibilityUtilsTest {

  @Test
  public void testStrictVisibilityUsesDialectOnly() {
    final Map<String, String> treeAttributes = new HashMap<>();
    treeAttributes.put(SystemConstant.SQL_DIALECT_KEY, SystemConstant.SQL_DIALECT_TREE_VALUE);
    treeAttributes.put(
        SystemConstant.PIPE_VISIBILITY_KEY, SystemConstant.PIPE_VISIBILITY_STRICT_VALUE);
    treeAttributes.put(PipeSourceConstant.EXTRACTOR_CAPTURE_TREE_KEY, "false");
    treeAttributes.put(PipeSourceConstant.EXTRACTOR_CAPTURE_TABLE_KEY, "true");
    treeAttributes.put(PipeSourceConstant.EXTRACTOR_MODE_DOUBLE_LIVING_KEY, "true");
    assertVisibility(Visibility.TREE_ONLY, treeAttributes, true, false);

    final Map<String, String> tableAttributes = new HashMap<>();
    tableAttributes.put(SystemConstant.SQL_DIALECT_KEY, SystemConstant.SQL_DIALECT_TABLE_VALUE);
    tableAttributes.put(
        SystemConstant.PIPE_VISIBILITY_KEY, SystemConstant.PIPE_VISIBILITY_STRICT_VALUE);
    tableAttributes.put(PipeSourceConstant.SOURCE_CAPTURE_TREE_KEY, "true");
    tableAttributes.put(PipeSourceConstant.SOURCE_CAPTURE_TABLE_KEY, "false");
    tableAttributes.put(PipeSourceConstant.SOURCE_MODE_DOUBLE_LIVING_KEY, "true");
    assertVisibility(Visibility.TABLE_ONLY, tableAttributes, false, true);
  }

  @Test
  public void testLegacyVisibilityKeepsDoubleLivingAndCaptureAttributes() {
    assertVisibility(Visibility.TREE_ONLY, new HashMap<>(), true, false);

    final Map<String, String> tableAttributes = new HashMap<>();
    tableAttributes.put(SystemConstant.SQL_DIALECT_KEY, SystemConstant.SQL_DIALECT_TABLE_VALUE);
    assertVisibility(Visibility.TABLE_ONLY, tableAttributes, false, true);

    final Map<String, String> doubleLivingAttributes = new HashMap<>();
    doubleLivingAttributes.put(
        SystemConstant.SQL_DIALECT_KEY, SystemConstant.SQL_DIALECT_TREE_VALUE);
    doubleLivingAttributes.put(PipeSourceConstant.EXTRACTOR_MODE_DOUBLE_LIVING_KEY, "true");
    doubleLivingAttributes.put(PipeSourceConstant.EXTRACTOR_CAPTURE_TREE_KEY, "false");
    doubleLivingAttributes.put(PipeSourceConstant.EXTRACTOR_CAPTURE_TABLE_KEY, "false");
    assertVisibility(Visibility.BOTH, doubleLivingAttributes, true, true);

    final Map<String, String> captureBothAttributes = new HashMap<>();
    captureBothAttributes.put(PipeSourceConstant.EXTRACTOR_CAPTURE_TREE_KEY, "true");
    captureBothAttributes.put(PipeSourceConstant.EXTRACTOR_CAPTURE_TABLE_KEY, "true");
    assertVisibility(Visibility.BOTH, captureBothAttributes, true, true);

    final Map<String, String> captureNoneAttributes = new HashMap<>();
    captureNoneAttributes.put(PipeSourceConstant.SOURCE_CAPTURE_TREE_KEY, "false");
    captureNoneAttributes.put(PipeSourceConstant.SOURCE_CAPTURE_TABLE_KEY, "false");
    assertVisibility(Visibility.NONE, captureNoneAttributes, false, false);
  }

  private void assertVisibility(
      final Visibility expectedVisibility,
      final Map<String, String> attributes,
      final boolean expectedTreeAllowed,
      final boolean expectedTableAllowed) {
    final PipeParameters parameters = new PipeParameters(attributes);
    Assert.assertEquals(
        expectedVisibility, VisibilityUtils.calculateFromExtractorParameters(parameters));
    Assert.assertEquals(
        expectedTreeAllowed, TreePattern.isTreeModelDataAllowToBeCaptured(parameters));
    Assert.assertEquals(
        expectedTableAllowed, TablePattern.isTableModelDataAllowToBeCaptured(parameters));
  }
}
