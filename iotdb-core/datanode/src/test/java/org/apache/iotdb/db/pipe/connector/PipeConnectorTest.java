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

package org.apache.iotdb.db.pipe.connector;

import org.apache.iotdb.commons.pipe.plugin.builtin.BuiltinPipePlugin;
import org.apache.iotdb.db.pipe.config.constant.PipeConnectorConstant;
import org.apache.iotdb.db.pipe.connector.legacy.IoTDBSyncConnector;
import org.apache.iotdb.db.pipe.connector.v1.IoTDBThriftConnectorV1;
import org.apache.iotdb.db.pipe.connector.v2.IoTDBThriftConnectorV2;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;

public class PipeConnectorTest {
  @Test
  public void testIoTDBSyncConnector() {
    IoTDBSyncConnector connector = new IoTDBSyncConnector();
    try {
      connector.validate(
          new PipeParameterValidator(
              new PipeParameters(
                  new HashMap<String, String>() {
                    {
                      put(
                          PipeConnectorConstant.CONNECTOR_KEY,
                          BuiltinPipePlugin.IOTDB_SYNC_CONNECTOR.getPipePluginName());
                      put(PipeConnectorConstant.CONNECTOR_IOTDB_IP_KEY, "127.0.0.1");
                      put(PipeConnectorConstant.CONNECTOR_IOTDB_PORT_KEY, "6667");
                    }
                  })));
    } catch (Exception e) {
      Assert.fail();
    }
  }

  @Test
  public void testIoTDBThriftConnectorV1() {
    IoTDBThriftConnectorV1 connector = new IoTDBThriftConnectorV1();
    try {
      connector.validate(
          new PipeParameterValidator(
              new PipeParameters(
                  new HashMap<String, String>() {
                    {
                      put(
                          PipeConnectorConstant.CONNECTOR_KEY,
                          BuiltinPipePlugin.IOTDB_THRIFT_CONNECTOR_V1.getPipePluginName());
                      put(PipeConnectorConstant.CONNECTOR_IOTDB_IP_KEY, "127.0.0.1");
                      put(PipeConnectorConstant.CONNECTOR_IOTDB_PORT_KEY, "6667");
                    }
                  })));
    } catch (Exception e) {
      Assert.fail();
    }
  }

  @Test
  public void testIoTDBThriftConnectorV2() {
    IoTDBThriftConnectorV2 connector = new IoTDBThriftConnectorV2();
    try {
      connector.validate(
          new PipeParameterValidator(
              new PipeParameters(
                  new HashMap<String, String>() {
                    {
                      put(
                          PipeConnectorConstant.CONNECTOR_KEY,
                          BuiltinPipePlugin.IOTDB_THRIFT_CONNECTOR_V2.getPipePluginName());
                      put(PipeConnectorConstant.CONNECTOR_IOTDB_NODE_URLS_KEY, "127.0.0.1:6667");
                    }
                  })));
    } catch (Exception e) {
      Assert.fail();
    }
  }
}
