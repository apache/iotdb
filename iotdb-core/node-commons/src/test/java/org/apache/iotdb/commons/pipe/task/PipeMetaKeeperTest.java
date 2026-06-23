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
  public void testSameNameTreeAndTablePipesCanCoexist() {
    final String pipeName = "p";
    final PipeMetaKeeper keeper = new PipeMetaKeeper();
    final PipeStaticMeta treeStaticMeta =
        createStaticMeta(pipeName, 1, SystemConstant.SQL_DIALECT_TREE_VALUE);
    final PipeStaticMeta tableStaticMeta =
        createStaticMeta(pipeName, 2, SystemConstant.SQL_DIALECT_TABLE_VALUE);

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
  public void testCaptureAttributesDoNotAffectSameNameConflictScope() {
    final String pipeName = "p";
    final PipeMetaKeeper keeper = new PipeMetaKeeper();
    final PipeStaticMeta treeStaticMeta =
        createStaticMetaWithCaptureAttributes(pipeName, 1, SystemConstant.SQL_DIALECT_TREE_VALUE);
    final PipeStaticMeta tableStaticMeta =
        createStaticMetaWithCaptureAttributes(pipeName, 2, SystemConstant.SQL_DIALECT_TABLE_VALUE);

    keeper.addPipeMeta(new PipeMeta(treeStaticMeta, new PipeRuntimeMeta()));

    Assert.assertFalse(keeper.containsPipeMetaOverlapped(tableStaticMeta));
    Assert.assertTrue(
        keeper.containsPipeMetaOverlapped(
            createStaticMeta(pipeName, 3, SystemConstant.SQL_DIALECT_TREE_VALUE)));
  }

  @Test
  public void testNoDialectPipeDefaultsToTreeScope() {
    final String pipeName = "p";
    final PipeMetaKeeper keeper = new PipeMetaKeeper();
    final PipeStaticMeta oldTreeStaticMeta =
        new PipeStaticMeta(pipeName, 1, new HashMap<>(), new HashMap<>(), new HashMap<>());
    final PipeStaticMeta tableStaticMeta =
        createStaticMeta(pipeName, 2, SystemConstant.SQL_DIALECT_TABLE_VALUE);

    keeper.addPipeMeta(new PipeMeta(oldTreeStaticMeta, new PipeRuntimeMeta()));

    Assert.assertSame(oldTreeStaticMeta, keeper.getPipeMeta(pipeName, false).getStaticMeta());
    Assert.assertNull(keeper.getPipeMeta(pipeName, true));
    Assert.assertFalse(keeper.containsPipeMetaOverlapped(tableStaticMeta));
  }

  private PipeStaticMeta createStaticMeta(
      final String pipeName, final long creationTime, final String dialect) {
    final Map<String, String> attributes = new HashMap<>();
    attributes.put(SystemConstant.SQL_DIALECT_KEY, dialect);
    return new PipeStaticMeta(pipeName, creationTime, attributes, new HashMap<>(), new HashMap<>());
  }

  private PipeStaticMeta createStaticMetaWithCaptureAttributes(
      final String pipeName, final long creationTime, final String dialect) {
    final Map<String, String> attributes = new HashMap<>();
    attributes.put(SystemConstant.SQL_DIALECT_KEY, dialect);
    attributes.put(PipeSourceConstant.EXTRACTOR_CAPTURE_TREE_KEY, "true");
    attributes.put(PipeSourceConstant.EXTRACTOR_CAPTURE_TABLE_KEY, "true");
    return new PipeStaticMeta(pipeName, creationTime, attributes, new HashMap<>(), new HashMap<>());
  }
}
