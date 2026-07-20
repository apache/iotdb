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

package org.apache.iotdb.db.pipe.agent.task.subtask.sink;

import org.apache.iotdb.commons.pipe.agent.plugin.builtin.BuiltinPipePlugin;
import org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant;
import org.apache.iotdb.commons.pipe.config.constant.SystemConstant;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class PipeSinkSubtaskManagerTest {

  @Test
  public void testGenerateAttributeSortedStringUsesSerializeByRegionAndIgnoresRestartFlag() {
    final Map<String, String> attributes = new HashMap<>();
    attributes.put("z", "1");
    attributes.put("a", "2");
    attributes.put(SystemConstant.RESTART_OR_NEWLY_ADDED_KEY, Boolean.TRUE.toString());

    Assert.assertEquals(
        "data_{a=2, z=1}",
        PipeSinkSubtaskManager.generateAttributeSortedString(
            new PipeParameters(new HashMap<>(attributes)), -1));

    attributes.put(PipeSinkConstant.CONNECTOR_SERIALIZE_BY_REGION_KEY, Boolean.TRUE.toString());
    Assert.assertEquals(
        "data_region_-1_{a=2, connector.serialize-by-region=true, z=1}",
        PipeSinkSubtaskManager.generateAttributeSortedString(
            new PipeParameters(new HashMap<>(attributes)), -1));
  }

  @Test
  public void testCalculateSinkSubtaskNumForDataRegionSink() {
    final Map<String, String> parallelAttributes = new HashMap<>();
    parallelAttributes.put(
        PipeSinkConstant.CONNECTOR_SERIALIZE_BY_REGION_KEY, Boolean.FALSE.toString());
    parallelAttributes.put(PipeSinkConstant.CONNECTOR_IOTDB_PARALLEL_TASKS_KEY, "3");
    Assert.assertEquals(
        3,
        PipeSinkSubtaskManager.calculateSinkSubtaskNum(new PipeParameters(parallelAttributes), -1));

    final Map<String, String> serializedAttributes = new HashMap<>();
    serializedAttributes.put(
        PipeSinkConstant.CONNECTOR_SERIALIZE_BY_REGION_KEY, Boolean.TRUE.toString());
    serializedAttributes.put(PipeSinkConstant.CONNECTOR_IOTDB_PARALLEL_TASKS_KEY, "3");
    Assert.assertEquals(
        1,
        PipeSinkSubtaskManager.calculateSinkSubtaskNum(
            new PipeParameters(serializedAttributes), -1));
  }

  @Test
  public void testCalculateSinkSubtaskNumUsesSingleThreadDefaultSinkAndSchemaRegionLimit() {
    final Map<String, String> singleThreadAttributes = new HashMap<>();
    singleThreadAttributes.put(
        PipeSinkConstant.CONNECTOR_SERIALIZE_BY_REGION_KEY, Boolean.FALSE.toString());
    singleThreadAttributes.put(
        PipeSinkConstant.CONNECTOR_KEY, BuiltinPipePlugin.OPC_UA_SINK.getPipePluginName());
    Assert.assertEquals(
        1,
        PipeSinkSubtaskManager.calculateSinkSubtaskNum(
            new PipeParameters(singleThreadAttributes), -1));

    final Map<String, String> schemaRegionAttributes = new HashMap<>();
    schemaRegionAttributes.put(
        PipeSinkConstant.CONNECTOR_SERIALIZE_BY_REGION_KEY, Boolean.FALSE.toString());
    schemaRegionAttributes.put(PipeSinkConstant.CONNECTOR_IOTDB_PARALLEL_TASKS_KEY, "3");
    Assert.assertEquals(
        1,
        PipeSinkSubtaskManager.calculateSinkSubtaskNum(
            new PipeParameters(schemaRegionAttributes), Integer.MAX_VALUE));
  }
}
