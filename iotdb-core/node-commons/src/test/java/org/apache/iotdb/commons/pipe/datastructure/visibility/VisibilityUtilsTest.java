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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class VisibilityUtilsTest {

  @Test
  public void testPipeSourceParametersVisibilityUsesSqlDialect() {
    final Map<String, String> treeSourceAttributes = new HashMap<>();
    treeSourceAttributes.put(SystemConstant.SQL_DIALECT_KEY, SystemConstant.SQL_DIALECT_TREE_VALUE);
    treeSourceAttributes.put(PipeSourceConstant.SOURCE_CAPTURE_TREE_KEY, "false");
    treeSourceAttributes.put(PipeSourceConstant.SOURCE_CAPTURE_TABLE_KEY, "true");

    Assert.assertEquals(
        Visibility.TREE_ONLY,
        VisibilityUtils.calculateFromPipeSourceParameters(
            new PipeParameters(treeSourceAttributes)));

    final Map<String, String> tableSourceAttributes = new HashMap<>();
    tableSourceAttributes.put(
        SystemConstant.SQL_DIALECT_KEY, SystemConstant.SQL_DIALECT_TABLE_VALUE);
    tableSourceAttributes.put(PipeSourceConstant.SOURCE_CAPTURE_TREE_KEY, "true");
    tableSourceAttributes.put(PipeSourceConstant.SOURCE_CAPTURE_TABLE_KEY, "true");

    Assert.assertEquals(
        Visibility.TABLE_ONLY,
        VisibilityUtils.calculateFromPipeSourceParameters(
            new PipeParameters(tableSourceAttributes)));

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
}
