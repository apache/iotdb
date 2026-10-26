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

package org.apache.iotdb.commons.pipe.task;

import org.apache.iotdb.commons.pipe.agent.task.meta.PipeMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeMetaKeeper;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeRuntimeMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeStaticMeta;
import org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant;
import org.apache.iotdb.commons.pipe.config.constant.SystemConstant;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class PipeMetaKeeperTest {

  @Test
  public void testStrictSameNameTreeAndTablePipesCanCoexist() {
    final String pipeName = "p";
    final PipeMetaKeeper keeper = new PipeMetaKeeper();
    final PipeStaticMeta treeStaticMeta =
        createStrictStaticMeta(pipeName, 1, SystemConstant.SQL_DIALECT_TREE_VALUE);
    final PipeStaticMeta tableStaticMeta =
        createStrictStaticMeta(pipeName, 2, SystemConstant.SQL_DIALECT_TABLE_VALUE);

    Assert.assertFalse(keeper.containsPipeMetaOverlapped(treeStaticMeta));
    keeper.addPipeMeta(new PipeMeta(treeStaticMeta, new PipeRuntimeMeta()));

    Assert.assertTrue(keeper.containsPipeMetaOverlapped(treeStaticMeta));
    Assert.assertFalse(keeper.containsPipeMetaOverlapped(tableStaticMeta));
    keeper.addPipeMeta(new PipeMeta(tableStaticMeta, new PipeRuntimeMeta()));

    Assert.assertEquals(2, keeper.getPipeMetaCount());
    Assert.assertSame(treeStaticMeta, keeper.getPipeMeta(pipeName, false).getStaticMeta());
    Assert.assertSame(tableStaticMeta, keeper.getPipeMeta(pipeName, true).getStaticMeta());
    Assert.assertSame(treeStaticMeta, keeper.getPipeMeta(pipeName).getStaticMeta());
  }

  @Test
  public void testStrictCaptureAttributesDoNotAffectSameNameConflictScope() {
    final String pipeName = "p";
    final PipeMetaKeeper keeper = new PipeMetaKeeper();
    final PipeStaticMeta treeStaticMeta =
        createStrictStaticMetaWithCaptureAttributes(
            pipeName, 1, SystemConstant.SQL_DIALECT_TREE_VALUE);
    final PipeStaticMeta tableStaticMeta =
        createStrictStaticMetaWithCaptureAttributes(
            pipeName, 2, SystemConstant.SQL_DIALECT_TABLE_VALUE);

    keeper.addPipeMeta(new PipeMeta(treeStaticMeta, new PipeRuntimeMeta()));

    Assert.assertFalse(keeper.containsPipeMetaOverlapped(tableStaticMeta));
    Assert.assertTrue(
        keeper.containsPipeMetaOverlapped(
            createStrictStaticMeta(pipeName, 3, SystemConstant.SQL_DIALECT_TREE_VALUE)));
  }

  @Test
  public void testLegacyNoDialectPipeDefaultsToTreeScope() {
    final String pipeName = "p";
    final PipeMetaKeeper keeper = new PipeMetaKeeper();
    final PipeStaticMeta oldTreeStaticMeta =
        new PipeStaticMeta(pipeName, 1, new HashMap<>(), new HashMap<>(), new HashMap<>());
    final PipeStaticMeta tableStaticMeta =
        createStrictStaticMeta(pipeName, 2, SystemConstant.SQL_DIALECT_TABLE_VALUE);

    keeper.addPipeMeta(new PipeMeta(oldTreeStaticMeta, new PipeRuntimeMeta()));

    Assert.assertSame(oldTreeStaticMeta, keeper.getPipeMeta(pipeName, false).getStaticMeta());
    Assert.assertNull(keeper.getPipeMeta(pipeName, true));
    Assert.assertFalse(keeper.containsPipeMetaOverlapped(tableStaticMeta));
  }

  @Test
  public void testLegacyDoubleLivingPipeOverlapsBothScopes() {
    final String pipeName = "p";
    final PipeMetaKeeper keeper = new PipeMetaKeeper();
    final PipeStaticMeta doubleLivingStaticMeta = createLegacyDoubleLivingStaticMeta(pipeName, 1);

    keeper.addPipeMeta(new PipeMeta(doubleLivingStaticMeta, new PipeRuntimeMeta()));

    Assert.assertSame(doubleLivingStaticMeta, keeper.getPipeMeta(pipeName, false).getStaticMeta());
    Assert.assertSame(doubleLivingStaticMeta, keeper.getPipeMeta(pipeName, true).getStaticMeta());
    Assert.assertTrue(
        keeper.containsPipeMetaOverlapped(
            createStrictStaticMeta(pipeName, 2, SystemConstant.SQL_DIALECT_TREE_VALUE)));
    Assert.assertTrue(
        keeper.containsPipeMetaOverlapped(
            createStrictStaticMeta(pipeName, 3, SystemConstant.SQL_DIALECT_TABLE_VALUE)));
  }

  @Test
  public void testLegacyCaptureBothPipeOverlapsBothScopes() {
    final String pipeName = "p";
    final PipeMetaKeeper keeper = new PipeMetaKeeper();
    final PipeStaticMeta captureBothStaticMeta = createLegacyCaptureBothStaticMeta(pipeName, 1);

    keeper.addPipeMeta(new PipeMeta(captureBothStaticMeta, new PipeRuntimeMeta()));

    Assert.assertSame(captureBothStaticMeta, keeper.getPipeMeta(pipeName, false).getStaticMeta());
    Assert.assertSame(captureBothStaticMeta, keeper.getPipeMeta(pipeName, true).getStaticMeta());
    Assert.assertTrue(
        keeper.containsPipeMetaOverlapped(
            createStrictStaticMeta(pipeName, 2, SystemConstant.SQL_DIALECT_TREE_VALUE)));
    Assert.assertTrue(
        keeper.containsPipeMetaOverlapped(
            createStrictStaticMeta(pipeName, 3, SystemConstant.SQL_DIALECT_TABLE_VALUE)));
  }

  @Test
  public void testLegacyCaptureNonePipeKeepsNoneScope() {
    final String pipeName = "p";
    final PipeMetaKeeper keeper = new PipeMetaKeeper();
    final PipeStaticMeta captureNoneStaticMeta = createLegacyCaptureNoneStaticMeta(pipeName, 1);

    keeper.addPipeMeta(new PipeMeta(captureNoneStaticMeta, new PipeRuntimeMeta()));

    Assert.assertSame(captureNoneStaticMeta, keeper.getPipeMeta(pipeName).getStaticMeta());
    Assert.assertNull(keeper.getPipeMeta(pipeName, false));
    Assert.assertNull(keeper.getPipeMeta(pipeName, true));
    Assert.assertFalse(
        keeper.containsPipeMetaOverlapped(
            createStrictStaticMeta(pipeName, 2, SystemConstant.SQL_DIALECT_TREE_VALUE)));
    Assert.assertFalse(
        keeper.containsPipeMetaOverlapped(
            createStrictStaticMeta(pipeName, 3, SystemConstant.SQL_DIALECT_TABLE_VALUE)));
  }

  private PipeStaticMeta createStrictStaticMeta(
      final String pipeName, final long creationTime, final String dialect) {
    final Map<String, String> attributes = new HashMap<>();
    attributes.put(SystemConstant.SQL_DIALECT_KEY, dialect);
    attributes.put(SystemConstant.PIPE_VISIBILITY_KEY, SystemConstant.PIPE_VISIBILITY_STRICT_VALUE);
    return new PipeStaticMeta(pipeName, creationTime, attributes, new HashMap<>(), new HashMap<>());
  }

  private PipeStaticMeta createStrictStaticMetaWithCaptureAttributes(
      final String pipeName, final long creationTime, final String dialect) {
    final Map<String, String> attributes = new HashMap<>();
    attributes.put(SystemConstant.SQL_DIALECT_KEY, dialect);
    attributes.put(SystemConstant.PIPE_VISIBILITY_KEY, SystemConstant.PIPE_VISIBILITY_STRICT_VALUE);
    attributes.put(PipeSourceConstant.EXTRACTOR_CAPTURE_TREE_KEY, "true");
    attributes.put(PipeSourceConstant.EXTRACTOR_CAPTURE_TABLE_KEY, "true");
    return new PipeStaticMeta(pipeName, creationTime, attributes, new HashMap<>(), new HashMap<>());
  }

  private PipeStaticMeta createLegacyDoubleLivingStaticMeta(
      final String pipeName, final long creationTime) {
    final Map<String, String> attributes = new HashMap<>();
    attributes.put(PipeSourceConstant.EXTRACTOR_MODE_DOUBLE_LIVING_KEY, "true");
    return new PipeStaticMeta(pipeName, creationTime, attributes, new HashMap<>(), new HashMap<>());
  }

  private PipeStaticMeta createLegacyCaptureBothStaticMeta(
      final String pipeName, final long creationTime) {
    final Map<String, String> attributes = new HashMap<>();
    attributes.put(PipeSourceConstant.EXTRACTOR_CAPTURE_TREE_KEY, "true");
    attributes.put(PipeSourceConstant.EXTRACTOR_CAPTURE_TABLE_KEY, "true");
    return new PipeStaticMeta(pipeName, creationTime, attributes, new HashMap<>(), new HashMap<>());
  }

  private PipeStaticMeta createLegacyCaptureNoneStaticMeta(
      final String pipeName, final long creationTime) {
    final Map<String, String> attributes = new HashMap<>();
    attributes.put(PipeSourceConstant.EXTRACTOR_CAPTURE_TREE_KEY, "false");
    attributes.put(PipeSourceConstant.EXTRACTOR_CAPTURE_TABLE_KEY, "false");
    return new PipeStaticMeta(pipeName, creationTime, attributes, new HashMap<>(), new HashMap<>());
  }
}
