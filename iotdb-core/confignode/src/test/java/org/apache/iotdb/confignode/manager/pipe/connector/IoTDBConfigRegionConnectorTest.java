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

package org.apache.iotdb.confignode.manager.pipe.connector;

import org.apache.iotdb.commons.pipe.agent.plugin.builtin.BuiltinPipePlugin;
import org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant;
import org.apache.iotdb.confignode.manager.pipe.connector.protocol.IoTDBConfigRegionConnector;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;

public class IoTDBConfigRegionConnectorTest {

  @Test
  public void testIoTDBSchemaConnector() {
    try (IoTDBConfigRegionConnector connector = new IoTDBConfigRegionConnector()) {
      connector.validate(
          new PipeParameterValidator(
              new PipeParameters(
                  new HashMap<String, String>() {
                    {
                      put(
                          PipeConnectorConstant.CONNECTOR_KEY,
                          BuiltinPipePlugin.IOTDB_LEGACY_PIPE_CONNECTOR.getPipePluginName());
                      put(PipeConnectorConstant.CONNECTOR_IOTDB_IP_KEY, "127.0.0.1");
                      put(PipeConnectorConstant.CONNECTOR_IOTDB_PORT_KEY, "6668");
                    }
                  })));
    } catch (Exception e) {
      Assert.fail();
    }
  }
}
