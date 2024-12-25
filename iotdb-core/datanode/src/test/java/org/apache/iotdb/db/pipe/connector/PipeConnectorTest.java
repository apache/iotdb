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

import org.apache.iotdb.commons.pipe.agent.plugin.builtin.BuiltinPipePlugin;
import org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant;
import org.apache.iotdb.db.pipe.connector.protocol.legacy.IoTDBLegacyPipeConnector;
import org.apache.iotdb.db.pipe.connector.protocol.thrift.async.IoTDBDataRegionAsyncConnector;
import org.apache.iotdb.db.pipe.connector.protocol.thrift.sync.IoTDBDataRegionSyncConnector;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.exception.PipeParameterNotValidException;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;

public class PipeConnectorTest {

  @Test(expected = PipeParameterNotValidException.class)
  public void testIoTDBLegacyPipeConnectorToSelf() throws Exception {
    try (IoTDBLegacyPipeConnector connector = new IoTDBLegacyPipeConnector()) {
      connector.validate(
          new PipeParameterValidator(
              new PipeParameters(
                  new HashMap<String, String>() {
                    {
                      put(
                          PipeConnectorConstant.CONNECTOR_KEY,
                          BuiltinPipePlugin.IOTDB_LEGACY_PIPE_CONNECTOR.getPipePluginName());
                      put(PipeConnectorConstant.CONNECTOR_IOTDB_IP_KEY, "127.0.0.1");
                      put(PipeConnectorConstant.CONNECTOR_IOTDB_PORT_KEY, "6667");
                    }
                  })));
    }
  }

  @Test
  public void testIoTDBLegacyPipeConnectorToOthers() {
    try (IoTDBLegacyPipeConnector connector = new IoTDBLegacyPipeConnector()) {
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

  @Test(expected = PipeParameterNotValidException.class)
  public void testIoTDBThriftSyncConnectorToSelf() throws Exception {
    try (IoTDBDataRegionSyncConnector connector = new IoTDBDataRegionSyncConnector()) {
      connector.validate(
          new PipeParameterValidator(
              new PipeParameters(
                  new HashMap<String, String>() {
                    {
                      put(
                          PipeConnectorConstant.CONNECTOR_KEY,
                          BuiltinPipePlugin.IOTDB_THRIFT_SYNC_CONNECTOR.getPipePluginName());
                      put(PipeConnectorConstant.CONNECTOR_IOTDB_IP_KEY, "127.0.0.1");
                      put(PipeConnectorConstant.CONNECTOR_IOTDB_PORT_KEY, "6667");
                    }
                  })));
    }
  }

  @Test
  public void testIoTDBThriftSyncConnectorToOthers() {
    try (IoTDBDataRegionSyncConnector connector = new IoTDBDataRegionSyncConnector()) {
      connector.validate(
          new PipeParameterValidator(
              new PipeParameters(
                  new HashMap<String, String>() {
                    {
                      put(
                          PipeConnectorConstant.CONNECTOR_KEY,
                          BuiltinPipePlugin.IOTDB_THRIFT_SYNC_CONNECTOR.getPipePluginName());
                      put(PipeConnectorConstant.CONNECTOR_IOTDB_IP_KEY, "127.0.0.1");
                      put(PipeConnectorConstant.CONNECTOR_IOTDB_PORT_KEY, "6668");
                    }
                  })));
    } catch (Exception e) {
      Assert.fail();
    }
  }

  @Test(expected = PipeParameterNotValidException.class)
  public void testIoTDBThriftAsyncConnectorToSelf() throws Exception {
    try (IoTDBDataRegionAsyncConnector connector = new IoTDBDataRegionAsyncConnector()) {
      connector.validate(
          new PipeParameterValidator(
              new PipeParameters(
                  new HashMap<String, String>() {
                    {
                      put(
                          PipeConnectorConstant.CONNECTOR_KEY,
                          BuiltinPipePlugin.IOTDB_THRIFT_ASYNC_CONNECTOR.getPipePluginName());
                      put(PipeConnectorConstant.CONNECTOR_IOTDB_NODE_URLS_KEY, "127.0.0.1:6667");
                    }
                  })));
    }
  }

  @Test
  public void testIoTDBThriftAsyncConnectorToOthers() {
    try (IoTDBDataRegionAsyncConnector connector = new IoTDBDataRegionAsyncConnector()) {
      connector.validate(
          new PipeParameterValidator(
              new PipeParameters(
                  new HashMap<String, String>() {
                    {
                      put(
                          PipeConnectorConstant.CONNECTOR_KEY,
                          BuiltinPipePlugin.IOTDB_THRIFT_ASYNC_CONNECTOR.getPipePluginName());
                      put(PipeConnectorConstant.CONNECTOR_IOTDB_NODE_URLS_KEY, "127.0.0.1:6668");
                    }
                  })));
    } catch (Exception e) {
      Assert.fail();
    }
  }
}
