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

import org.apache.iotdb.commons.pipe.agent.task.meta.PipeStaticMeta;
import org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant;
import org.apache.iotdb.commons.pipe.config.constant.SystemConstant;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class VisibilityUtilsTest {

  @Test
  public void testPipeSourceParametersVisibilityDefaultsToTreeDialect() {
    Assert.assertEquals(
        Visibility.TREE_ONLY,
        VisibilityUtils.calculateFromPipeSourceParameters(
            new PipeParameters(Collections.emptyMap())));
  }

  @Test
  public void testPipeSourceParametersVisibilityUsesSqlDialectOnly() {
    final Map<String, String> treeSourceAttributes = new HashMap<>();
    treeSourceAttributes.put(SystemConstant.SQL_DIALECT_KEY, SystemConstant.SQL_DIALECT_TREE_VALUE);
    treeSourceAttributes.put(PipeSourceConstant.SOURCE_CAPTURE_TREE_KEY, "false");
    treeSourceAttributes.put(PipeSourceConstant.SOURCE_CAPTURE_TABLE_KEY, "true");
    treeSourceAttributes.put(PipeSourceConstant.SOURCE_MODE_DOUBLE_LIVING_KEY, "true");

    Assert.assertEquals(
        Visibility.TREE_ONLY,
        VisibilityUtils.calculateFromPipeSourceParameters(
            new PipeParameters(treeSourceAttributes)));

    final Map<String, String> tableSourceAttributes = new HashMap<>();
    tableSourceAttributes.put(
        SystemConstant.SQL_DIALECT_KEY, SystemConstant.SQL_DIALECT_TABLE_VALUE);
    tableSourceAttributes.put(PipeSourceConstant.SOURCE_CAPTURE_TREE_KEY, "true");
    tableSourceAttributes.put(PipeSourceConstant.SOURCE_CAPTURE_TABLE_KEY, "true");
    tableSourceAttributes.put(PipeSourceConstant.SOURCE_MODE_DOUBLE_LIVING_KEY, "true");

    Assert.assertEquals(
        Visibility.TABLE_ONLY,
        VisibilityUtils.calculateFromPipeSourceParameters(
            new PipeParameters(tableSourceAttributes)));
  }

  @Test
  public void testExtractorParametersVisibilityStillUsesCaptureAndDoubleLiving() {
    final Map<String, String> treeDialectCaptureTableAttributes = new HashMap<>();
    treeDialectCaptureTableAttributes.put(
        SystemConstant.SQL_DIALECT_KEY, SystemConstant.SQL_DIALECT_TREE_VALUE);
    treeDialectCaptureTableAttributes.put(PipeSourceConstant.SOURCE_CAPTURE_TREE_KEY, "false");
    treeDialectCaptureTableAttributes.put(PipeSourceConstant.SOURCE_CAPTURE_TABLE_KEY, "true");

    Assert.assertEquals(
        Visibility.TABLE_ONLY,
        VisibilityUtils.calculateFromExtractorParameters(
            new PipeParameters(treeDialectCaptureTableAttributes)));
    Assert.assertEquals(
        Visibility.TREE_ONLY,
        VisibilityUtils.calculateFromPipeSourceParameters(
            new PipeParameters(treeDialectCaptureTableAttributes)));

    treeDialectCaptureTableAttributes.put(PipeSourceConstant.SOURCE_MODE_DOUBLE_LIVING_KEY, "true");
    Assert.assertEquals(
        Visibility.BOTH,
        VisibilityUtils.calculateFromExtractorParameters(
            new PipeParameters(treeDialectCaptureTableAttributes)));
    Assert.assertEquals(
        Visibility.TREE_ONLY,
        VisibilityUtils.calculateFromPipeSourceParameters(
            new PipeParameters(treeDialectCaptureTableAttributes)));
  }

  @Test
  public void testPipeStaticMetaVisibilityUsesSqlDialect() {
    final Map<String, String> treeSourceAttributes = new HashMap<>();
    treeSourceAttributes.put(SystemConstant.SQL_DIALECT_KEY, SystemConstant.SQL_DIALECT_TREE_VALUE);
    treeSourceAttributes.put(PipeSourceConstant.SOURCE_CAPTURE_TREE_KEY, "false");
    treeSourceAttributes.put(PipeSourceConstant.SOURCE_CAPTURE_TABLE_KEY, "true");

    final Map<String, String> tableSourceAttributes = new HashMap<>();
    tableSourceAttributes.put(
        SystemConstant.SQL_DIALECT_KEY, SystemConstant.SQL_DIALECT_TABLE_VALUE);
    tableSourceAttributes.put(PipeSourceConstant.SOURCE_CAPTURE_TREE_KEY, "true");
    tableSourceAttributes.put(PipeSourceConstant.SOURCE_CAPTURE_TABLE_KEY, "true");

    final PipeStaticMeta treePipeStaticMeta =
        new PipeStaticMeta(
            "treePipe", 1, treeSourceAttributes, Collections.emptyMap(), Collections.emptyMap());
    Assert.assertTrue(treePipeStaticMeta.visibleUnder(false));
    Assert.assertFalse(treePipeStaticMeta.visibleUnder(true));

    final PipeStaticMeta tablePipeStaticMeta =
        new PipeStaticMeta(
            "tablePipe", 1, tableSourceAttributes, Collections.emptyMap(), Collections.emptyMap());
    Assert.assertFalse(tablePipeStaticMeta.visibleUnder(false));
    Assert.assertTrue(tablePipeStaticMeta.visibleUnder(true));
  }

  @Test
  public void testPipeStaticMetaVisibilityDefaultsToTreeDialect() {
    final Map<String, String> sourceAttributes = new HashMap<>();
    sourceAttributes.put(PipeSourceConstant.SOURCE_CAPTURE_TREE_KEY, "false");
    sourceAttributes.put(PipeSourceConstant.SOURCE_CAPTURE_TABLE_KEY, "true");
    sourceAttributes.put(PipeSourceConstant.SOURCE_MODE_DOUBLE_LIVING_KEY, "true");

    final PipeStaticMeta pipeStaticMeta =
        new PipeStaticMeta(
            "defaultTreePipe", 1, sourceAttributes, Collections.emptyMap(), Collections.emptyMap());

    Assert.assertTrue(pipeStaticMeta.visibleUnder(false));
    Assert.assertFalse(pipeStaticMeta.visibleUnder(true));
  }

  @Test
  public void testPipeStaticMetaVisibilityIgnoresDoubleLiving() {
    final Map<String, String> treeSourceAttributes = new HashMap<>();
    treeSourceAttributes.put(SystemConstant.SQL_DIALECT_KEY, SystemConstant.SQL_DIALECT_TREE_VALUE);
    treeSourceAttributes.put(PipeSourceConstant.SOURCE_CAPTURE_TREE_KEY, "true");
    treeSourceAttributes.put(PipeSourceConstant.SOURCE_CAPTURE_TABLE_KEY, "true");
    treeSourceAttributes.put(PipeSourceConstant.SOURCE_MODE_DOUBLE_LIVING_KEY, "true");

    final PipeStaticMeta treePipeStaticMeta =
        new PipeStaticMeta(
            "treeDoubleLivingPipe",
            1,
            treeSourceAttributes,
            Collections.emptyMap(),
            Collections.emptyMap());
    Assert.assertTrue(treePipeStaticMeta.visibleUnder(false));
    Assert.assertFalse(treePipeStaticMeta.visibleUnder(true));

    final Map<String, String> tableSourceAttributes = new HashMap<>();
    tableSourceAttributes.put(
        SystemConstant.SQL_DIALECT_KEY, SystemConstant.SQL_DIALECT_TABLE_VALUE);
    tableSourceAttributes.put(PipeSourceConstant.SOURCE_CAPTURE_TREE_KEY, "true");
    tableSourceAttributes.put(PipeSourceConstant.SOURCE_CAPTURE_TABLE_KEY, "true");
    tableSourceAttributes.put(PipeSourceConstant.SOURCE_MODE_DOUBLE_LIVING_KEY, "true");

    final PipeStaticMeta tablePipeStaticMeta =
        new PipeStaticMeta(
            "tableDoubleLivingPipe",
            1,
            tableSourceAttributes,
            Collections.emptyMap(),
            Collections.emptyMap());
    Assert.assertFalse(tablePipeStaticMeta.visibleUnder(false));
    Assert.assertTrue(tablePipeStaticMeta.visibleUnder(true));
  }

  @Test
  public void testPipeStaticMetaVisibilitySurvivesSerialization() throws IOException {
    final Map<String, String> tableSourceAttributes = new HashMap<>();
    tableSourceAttributes.put(
        SystemConstant.SQL_DIALECT_KEY, SystemConstant.SQL_DIALECT_TABLE_VALUE);
    tableSourceAttributes.put(PipeSourceConstant.SOURCE_CAPTURE_TREE_KEY, "true");
    tableSourceAttributes.put(PipeSourceConstant.SOURCE_CAPTURE_TABLE_KEY, "true");

    final PipeStaticMeta tablePipeStaticMeta =
        new PipeStaticMeta(
            "tablePipe", 1, tableSourceAttributes, Collections.emptyMap(), Collections.emptyMap());

    final PipeStaticMeta byteBufferDeserializedMeta =
        PipeStaticMeta.deserialize(tablePipeStaticMeta.serialize());
    Assert.assertFalse(byteBufferDeserializedMeta.visibleUnder(false));
    Assert.assertTrue(byteBufferDeserializedMeta.visibleUnder(true));

    final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    tablePipeStaticMeta.serialize(outputStream);
    final PipeStaticMeta inputStreamDeserializedMeta =
        PipeStaticMeta.deserialize(new ByteArrayInputStream(outputStream.toByteArray()));
    Assert.assertFalse(inputStreamDeserializedMeta.visibleUnder(false));
    Assert.assertTrue(inputStreamDeserializedMeta.visibleUnder(true));
  }
}
