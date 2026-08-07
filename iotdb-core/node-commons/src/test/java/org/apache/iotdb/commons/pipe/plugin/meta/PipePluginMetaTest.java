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

package org.apache.iotdb.commons.pipe.plugin.meta;

import org.apache.iotdb.commons.pipe.agent.plugin.builtin.BuiltinPipePlugin;
import org.apache.iotdb.commons.pipe.agent.plugin.meta.ConfigNodePipePluginMetaKeeper;
import org.apache.iotdb.commons.pipe.agent.plugin.meta.DataNodePipePluginMetaKeeper;
import org.apache.iotdb.commons.pipe.agent.plugin.meta.PipePluginMeta;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class PipePluginMetaTest {

  @Test
  public void testConfigNodePipePluginMetaKeeper() throws IOException {
    ConfigNodePipePluginMetaKeeper keeper = new ConfigNodePipePluginMetaKeeper();
    Assert.assertFalse(keeper.containsJar("test.jar"));
    keeper.addJarNameAndMd5("test.jar", "md5");
    keeper.addJarNameAndMd5("test.jar", "md5");
    Assert.assertTrue(keeper.jarNameExistsAndMatchesMd5("test.jar", "md5"));
    Assert.assertFalse(keeper.jarNameExistsAndMatchesMd5("test.jar", "wrong-md5"));

    final PipePluginMeta pluginMeta =
        new PipePluginMeta(
            "externalPlugin", "org.example.ExternalPlugin", false, "test.jar", "md5");
    keeper.addPipePluginMeta(pluginMeta.getPluginName(), pluginMeta);
    Assert.assertEquals("EXTERNALPLUGIN", keeper.getPluginNameByJarName("test.jar"));

    final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    keeper.processTakeSnapshot(outputStream);

    keeper.removeJarNameAndMd5IfPossible("test.jar");
    Assert.assertTrue(keeper.containsJar("test.jar"));
    keeper.removeJarNameAndMd5IfPossible("test.jar");
    keeper.removePipePluginMeta(pluginMeta.getPluginName());
    Assert.assertFalse(keeper.containsJar("test.jar"));
    Assert.assertFalse(keeper.containsPipePlugin(pluginMeta.getPluginName()));

    keeper.processLoadSnapshot(new ByteArrayInputStream(outputStream.toByteArray()));

    Assert.assertTrue(keeper.jarNameExistsAndMatchesMd5("test.jar", "md5"));
    Assert.assertEquals(pluginMeta, keeper.getPipePluginMeta(pluginMeta.getPluginName()));
    Assert.assertEquals("EXTERNALPLUGIN", keeper.getPluginNameByJarName("test.jar"));
  }

  @Test
  public void testDataNodePipePluginMetaKeeper() {
    final DataNodePipePluginMetaKeeper keeper = new DataNodePipePluginMetaKeeper();
    Assert.assertEquals(
        BuiltinPipePlugin.IOTDB_EXTRACTOR.getPipePluginClass(),
        keeper.getBuiltinPluginClass(BuiltinPipePlugin.IOTDB_EXTRACTOR.getPipePluginName()));
    Assert.assertEquals(
        BuiltinPipePlugin.IOTDB_EXTRACTOR.getPipePluginClass(),
        keeper.getBuiltinPluginClass(
            BuiltinPipePlugin.IOTDB_EXTRACTOR.getPipePluginName().toUpperCase()));
    Assert.assertTrue(
        keeper.containsPipePlugin(BuiltinPipePlugin.IOTDB_EXTRACTOR.getPipePluginName()));
  }
}
