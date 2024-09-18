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

import org.apache.iotdb.commons.pipe.plugin.builtin.BuiltinPipePlugin;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class PipePluginMetaTest {

  @Test
  public void testConfigNodePipePluginMetaKeeper() {
    ConfigNodePipePluginMetaKeeper keeper = new ConfigNodePipePluginMetaKeeper();
    Assert.assertFalse(keeper.containsJar("test.jar"));
    keeper.addJarNameAndMd5("test.jar", "md5");
    Assert.assertTrue(keeper.jarNameExistsAndMatchesMd5("test.jar", "md5"));

    try {
      ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
      keeper.processTakeSnapshot(outputStream);
      keeper.removeJarNameAndMd5IfPossible("test.jar");
      ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
      keeper.processLoadSnapshot(inputStream);
      Assert.assertTrue(keeper.jarNameExistsAndMatchesMd5("test.jar", "md5"));
    } catch (IOException e) {
      Assert.fail();
    }
  }

  @Test
  public void testDataNodePipePluginMetaKeeper() {
    DataNodePipePluginMetaKeeper keeper = new DataNodePipePluginMetaKeeper();
    Assert.assertEquals(
        BuiltinPipePlugin.IOTDB_EXTRACTOR.getPipePluginClass(),
        keeper.getBuiltinPluginClass(BuiltinPipePlugin.IOTDB_EXTRACTOR.getPipePluginName()));
  }
}
