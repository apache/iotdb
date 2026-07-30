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

package org.apache.iotdb.db.pipe.sink;

import org.apache.iotdb.commons.pipe.agent.plugin.builtin.BuiltinPipePlugin;
import org.apache.iotdb.commons.pipe.agent.task.progress.CommitterKey;
import org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant;
import org.apache.iotdb.commons.pipe.config.plugin.configuraion.PipeTaskRuntimeConfiguration;
import org.apache.iotdb.commons.pipe.config.plugin.env.PipeTaskSinkRuntimeEnvironment;
import org.apache.iotdb.db.pipe.event.common.statement.PipeStatementInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.db.pipe.sink.protocol.legacy.IoTDBLegacyPipeSink;
import org.apache.iotdb.db.pipe.sink.protocol.opcua.OpcUaSink;
import org.apache.iotdb.db.pipe.sink.protocol.thrift.async.IoTDBDataRegionAsyncSink;
import org.apache.iotdb.db.pipe.sink.protocol.thrift.sync.IoTDBDataRegionSyncSink;
import org.apache.iotdb.db.pipe.sink.protocol.websocket.WebSocketConnectorServer;
import org.apache.iotdb.db.pipe.sink.protocol.websocket.WebSocketSink;
import org.apache.iotdb.db.pipe.sink.protocol.writeback.WriteBackSink;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertTabletStatement;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.exception.PipeException;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

public class PipeSinkTest {

  @Test
  public void testIoTDBLegacyPipeConnectorToOthers() {
    try (IoTDBLegacyPipeSink connector = new IoTDBLegacyPipeSink()) {
      connector.validate(
          new PipeParameterValidator(
              new PipeParameters(
                  new HashMap<String, String>() {
                    {
                      put(
                          PipeSinkConstant.CONNECTOR_KEY,
                          BuiltinPipePlugin.IOTDB_LEGACY_PIPE_CONNECTOR.getPipePluginName());
                      put(PipeSinkConstant.CONNECTOR_IOTDB_IP_KEY, "127.0.0.1");
                      put(PipeSinkConstant.CONNECTOR_IOTDB_PORT_KEY, "6668");
                    }
                  })));
    } catch (Exception e) {
      Assert.fail();
    }
  }

  @Test
  public void testIoTDBThriftSyncConnectorToOthers() {
    try (IoTDBDataRegionSyncSink connector = new IoTDBDataRegionSyncSink()) {
      connector.validate(
          new PipeParameterValidator(
              new PipeParameters(
                  new HashMap<String, String>() {
                    {
                      put(
                          PipeSinkConstant.CONNECTOR_KEY,
                          BuiltinPipePlugin.IOTDB_THRIFT_SYNC_CONNECTOR.getPipePluginName());
                      put(PipeSinkConstant.CONNECTOR_IOTDB_IP_KEY, "127.0.0.1");
                      put(PipeSinkConstant.CONNECTOR_IOTDB_PORT_KEY, "6668");
                    }
                  })));
    } catch (Exception e) {
      Assert.fail();
    }
  }

  @Test
  public void testIoTDBThriftAsyncConnectorToOthers() {
    try (IoTDBDataRegionAsyncSink connector = new IoTDBDataRegionAsyncSink()) {
      connector.validate(
          new PipeParameterValidator(
              new PipeParameters(
                  new HashMap<String, String>() {
                    {
                      put(
                          PipeSinkConstant.CONNECTOR_KEY,
                          BuiltinPipePlugin.IOTDB_THRIFT_ASYNC_CONNECTOR.getPipePluginName());
                      put(PipeSinkConstant.CONNECTOR_IOTDB_NODE_URLS_KEY, "127.0.0.1:6668");
                    }
                  })));
    } catch (Exception e) {
      Assert.fail();
    }
  }

  @Test
  public void testIoTDBThriftSyncSslSinkAcceptsMutualSslParameters() {
    try (final IoTDBDataRegionSyncSink connector = new IoTDBDataRegionSyncSink()) {
      connector.validate(
          new PipeParameterValidator(
              new PipeParameters(
                  new HashMap<String, String>() {
                    {
                      put(
                          PipeSinkConstant.SINK_KEY,
                          BuiltinPipePlugin.IOTDB_THRIFT_SSL_SINK.getPipePluginName());
                      put(PipeSinkConstant.SINK_IOTDB_IP_KEY, "127.0.0.1");
                      put(PipeSinkConstant.SINK_IOTDB_PORT_KEY, "6668");
                      put(PipeSinkConstant.SINK_IOTDB_SSL_TRUST_STORE_PATH_KEY, "truststore");
                      put(PipeSinkConstant.SINK_IOTDB_SSL_TRUST_STORE_PWD_KEY, "trustpwd");
                      put(PipeSinkConstant.SINK_IOTDB_SSL_KEY_STORE_PATH_KEY, "keystore");
                      put(PipeSinkConstant.SINK_IOTDB_SSL_KEY_STORE_PWD_KEY, "keypwd");
                    }
                  })));
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
  }

  @Test
  public void testIoTDBThriftSyncSslConnectorAcceptsConnectorMutualSslAliases() {
    try (final IoTDBDataRegionSyncSink connector = new IoTDBDataRegionSyncSink()) {
      connector.validate(
          new PipeParameterValidator(
              new PipeParameters(
                  new HashMap<String, String>() {
                    {
                      put(
                          PipeSinkConstant.CONNECTOR_KEY,
                          BuiltinPipePlugin.IOTDB_THRIFT_SSL_CONNECTOR.getPipePluginName());
                      put(PipeSinkConstant.CONNECTOR_IOTDB_IP_KEY, "127.0.0.1");
                      put(PipeSinkConstant.CONNECTOR_IOTDB_PORT_KEY, "6668");
                      put(PipeSinkConstant.CONNECTOR_IOTDB_SSL_TRUST_STORE_PATH_KEY, "truststore");
                      put(PipeSinkConstant.CONNECTOR_IOTDB_SSL_TRUST_STORE_PWD_KEY, "trustpwd");
                      put(PipeSinkConstant.CONNECTOR_IOTDB_SSL_KEY_STORE_PATH_KEY, "keystore");
                      put(PipeSinkConstant.CONNECTOR_IOTDB_SSL_KEY_STORE_PWD_KEY, "keypwd");
                    }
                  })));
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
  }

  @Test
  public void testIoTDBThriftSyncSslSinkRejectsIncompleteKeyStoreParameters() {
    try (final IoTDBDataRegionSyncSink connector = new IoTDBDataRegionSyncSink()) {
      connector.validate(
          new PipeParameterValidator(
              new PipeParameters(
                  new HashMap<String, String>() {
                    {
                      put(
                          PipeSinkConstant.SINK_KEY,
                          BuiltinPipePlugin.IOTDB_THRIFT_SSL_SINK.getPipePluginName());
                      put(PipeSinkConstant.SINK_IOTDB_IP_KEY, "127.0.0.1");
                      put(PipeSinkConstant.SINK_IOTDB_PORT_KEY, "6668");
                      put(PipeSinkConstant.SINK_IOTDB_SSL_TRUST_STORE_PATH_KEY, "truststore");
                      put(PipeSinkConstant.SINK_IOTDB_SSL_TRUST_STORE_PWD_KEY, "trustpwd");
                      put(PipeSinkConstant.SINK_IOTDB_SSL_KEY_STORE_PATH_KEY, "keystore");
                    }
                  })));
      Assert.fail();
    } catch (Exception e) {
      Assert.assertTrue(e.getMessage().contains(PipeSinkConstant.SINK_IOTDB_SSL_KEY_STORE_PWD_KEY));
    }
  }

  @Test
  public void testIoTDBThriftSyncSslSinkRejectsCrossAliasTrustStoreParameters() {
    try (final IoTDBDataRegionSyncSink connector = new IoTDBDataRegionSyncSink()) {
      connector.validate(
          new PipeParameterValidator(
              new PipeParameters(
                  new HashMap<String, String>() {
                    {
                      put(
                          PipeSinkConstant.CONNECTOR_KEY,
                          BuiltinPipePlugin.IOTDB_THRIFT_SSL_CONNECTOR.getPipePluginName());
                      put(PipeSinkConstant.CONNECTOR_IOTDB_IP_KEY, "127.0.0.1");
                      put(PipeSinkConstant.CONNECTOR_IOTDB_PORT_KEY, "6668");
                      put(PipeSinkConstant.CONNECTOR_IOTDB_SSL_TRUST_STORE_PATH_KEY, "truststore");
                      put(PipeSinkConstant.SINK_IOTDB_SSL_TRUST_STORE_PWD_KEY, "trustpwd");
                    }
                  })));
      Assert.fail();
    } catch (Exception e) {
      Assert.assertTrue(
          e.getMessage().contains(PipeSinkConstant.SINK_IOTDB_SSL_TRUST_STORE_PWD_KEY));
    }
  }

  @Test
  public void testIoTDBThriftSyncSslSinkRejectsCrossAliasKeyStoreParameters() {
    try (final IoTDBDataRegionSyncSink connector = new IoTDBDataRegionSyncSink()) {
      connector.validate(
          new PipeParameterValidator(
              new PipeParameters(
                  new HashMap<String, String>() {
                    {
                      put(
                          PipeSinkConstant.SINK_KEY,
                          BuiltinPipePlugin.IOTDB_THRIFT_SSL_SINK.getPipePluginName());
                      put(PipeSinkConstant.SINK_IOTDB_IP_KEY, "127.0.0.1");
                      put(PipeSinkConstant.SINK_IOTDB_PORT_KEY, "6668");
                      put(PipeSinkConstant.SINK_IOTDB_SSL_TRUST_STORE_PATH_KEY, "truststore");
                      put(PipeSinkConstant.SINK_IOTDB_SSL_TRUST_STORE_PWD_KEY, "trustpwd");
                      put(PipeSinkConstant.CONNECTOR_IOTDB_SSL_KEY_STORE_PATH_KEY, "keystore");
                      put(PipeSinkConstant.SINK_IOTDB_SSL_KEY_STORE_PWD_KEY, "keypwd");
                    }
                  })));
      Assert.fail();
    } catch (Exception e) {
      Assert.assertTrue(e.getMessage().contains(PipeSinkConstant.SINK_IOTDB_SSL_KEY_STORE_PWD_KEY));
    }
  }

  @Test
  public void testIoTDBThriftAsyncSinkRejectsSslKeyStoreParameters() {
    try (final IoTDBDataRegionAsyncSink connector = new IoTDBDataRegionAsyncSink()) {
      connector.validate(
          new PipeParameterValidator(
              new PipeParameters(
                  new HashMap<String, String>() {
                    {
                      put(
                          PipeSinkConstant.CONNECTOR_KEY,
                          BuiltinPipePlugin.IOTDB_THRIFT_ASYNC_CONNECTOR.getPipePluginName());
                      put(PipeSinkConstant.CONNECTOR_IOTDB_NODE_URLS_KEY, "127.0.0.1:6668");
                      put(PipeSinkConstant.CONNECTOR_IOTDB_SSL_KEY_STORE_PATH_KEY, "keystore");
                      put(PipeSinkConstant.CONNECTOR_IOTDB_SSL_KEY_STORE_PWD_KEY, "keypwd");
                    }
                  })));
      Assert.fail();
    } catch (Exception e) {
      Assert.assertTrue(e.getMessage().contains("Only 'iotdb-thrift-ssl-sink' supports SSL"));
    }
  }

  @Test
  public void testIoTDBLegacyPipeSinkAcceptsConnectorMutualSslAliases() {
    try (final IoTDBLegacyPipeSink connector = new IoTDBLegacyPipeSink()) {
      connector.validate(
          new PipeParameterValidator(
              new PipeParameters(
                  new HashMap<String, String>() {
                    {
                      put(
                          PipeSinkConstant.CONNECTOR_KEY,
                          BuiltinPipePlugin.IOTDB_LEGACY_PIPE_CONNECTOR.getPipePluginName());
                      put(PipeSinkConstant.CONNECTOR_IOTDB_IP_KEY, "127.0.0.1");
                      put(PipeSinkConstant.CONNECTOR_IOTDB_PORT_KEY, "6668");
                      put(PipeSinkConstant.CONNECTOR_IOTDB_SSL_ENABLE_KEY, Boolean.TRUE.toString());
                      put(PipeSinkConstant.CONNECTOR_IOTDB_SSL_TRUST_STORE_PATH_KEY, "truststore");
                      put(PipeSinkConstant.CONNECTOR_IOTDB_SSL_TRUST_STORE_PWD_KEY, "trustpwd");
                      put(PipeSinkConstant.CONNECTOR_IOTDB_SSL_KEY_STORE_PATH_KEY, "keystore");
                      put(PipeSinkConstant.CONNECTOR_IOTDB_SSL_KEY_STORE_PWD_KEY, "keypwd");
                    }
                  })));
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
  }

  @Test
  public void testIoTDBLegacyPipeSinkRejectsCrossAliasSslParameters() {
    try (final IoTDBLegacyPipeSink connector = new IoTDBLegacyPipeSink()) {
      connector.validate(
          new PipeParameterValidator(
              new PipeParameters(
                  new HashMap<String, String>() {
                    {
                      put(
                          PipeSinkConstant.CONNECTOR_KEY,
                          BuiltinPipePlugin.IOTDB_LEGACY_PIPE_CONNECTOR.getPipePluginName());
                      put(PipeSinkConstant.CONNECTOR_IOTDB_IP_KEY, "127.0.0.1");
                      put(PipeSinkConstant.CONNECTOR_IOTDB_PORT_KEY, "6668");
                      put(PipeSinkConstant.CONNECTOR_IOTDB_SSL_ENABLE_KEY, Boolean.TRUE.toString());
                      put(PipeSinkConstant.CONNECTOR_IOTDB_SSL_TRUST_STORE_PATH_KEY, "truststore");
                      put(PipeSinkConstant.SINK_IOTDB_SSL_TRUST_STORE_PWD_KEY, "trustpwd");
                    }
                  })));
      Assert.fail();
    } catch (Exception e) {
      Assert.assertTrue(
          e.getMessage().contains(PipeSinkConstant.SINK_IOTDB_SSL_TRUST_STORE_PWD_KEY));
    }
  }

  @Test
  public void testAsyncSinkDropDoesNotRequeueDroppedPipeEvents() throws Exception {
    try (final IoTDBDataRegionAsyncSink connector = new IoTDBDataRegionAsyncSink()) {
      final PipeParameters parameters =
          new PipeParameters(
              new HashMap<String, String>() {
                {
                  put(
                      PipeSinkConstant.CONNECTOR_KEY,
                      BuiltinPipePlugin.IOTDB_THRIFT_ASYNC_CONNECTOR.getPipePluginName());
                  put(PipeSinkConstant.CONNECTOR_IOTDB_NODE_URLS_KEY, "127.0.0.1:6668");
                }
              });
      connector.validate(new PipeParameterValidator(parameters));
      connector.customize(
          parameters,
          new PipeTaskRuntimeConfiguration(new PipeTaskSinkRuntimeEnvironment("pipe", 1L, 1)));

      final PipeRawTabletInsertionEvent droppedEvent =
          createPipeRawTabletInsertionEvent("pipe", 1L, 1);
      droppedEvent.increaseReferenceCount("test");
      droppedEvent.setCommitterKeyAndCommitId(new CommitterKey("pipe", 1L, 1, -1), 1L);

      connector.discardEventsOfPipe("pipe", 1L, 1);
      connector.addFailureEventToRetryQueue(droppedEvent, new PipeException("test"));

      Assert.assertEquals(0, connector.getRetryEventQueueSize());
      Assert.assertTrue(droppedEvent.isReleased());

      final PipeRawTabletInsertionEvent recreatedPipeEvent =
          createPipeRawTabletInsertionEvent("pipe", 2L, 1);
      recreatedPipeEvent.increaseReferenceCount("test");
      recreatedPipeEvent.setCommitterKeyAndCommitId(new CommitterKey("pipe", 2L, 1, -1), 1L);

      connector.addFailureEventToRetryQueue(recreatedPipeEvent, new PipeException("test"));

      Assert.assertEquals(1, connector.getRetryEventQueueSize());
    }
  }

  @Test
  public void testWebSocketSinkDropDoesNotRequeueDroppedPipeEvents() {
    final String pipeName = "pipe_" + System.nanoTime();
    final WebSocketConnectorServer server = WebSocketConnectorServer.getOrCreateInstance(0);
    final WebSocketSink connector = Mockito.mock(WebSocketSink.class);
    Mockito.when(connector.getPipeName()).thenReturn(pipeName);

    server.register(connector);
    try {
      final PipeRawTabletInsertionEvent droppedEvent =
          createPipeRawTabletInsertionEvent(pipeName, 1L, 1);
      droppedEvent.increaseReferenceCount(WebSocketSink.class.getName());
      droppedEvent.setCommitterKeyAndCommitId(new CommitterKey(pipeName, 1L, 1, -1), 1L);
      server.addEvent(droppedEvent, connector);

      server.discardEventsOfPipe(pipeName, 1L, 1);
      Assert.assertTrue(droppedEvent.isReleased());

      final PipeRawTabletInsertionEvent recreatedDroppedPipeEvent =
          createPipeRawTabletInsertionEvent(pipeName, 1L, 1);
      recreatedDroppedPipeEvent.increaseReferenceCount(WebSocketSink.class.getName());
      recreatedDroppedPipeEvent.setCommitterKeyAndCommitId(
          new CommitterKey(pipeName, 1L, 1, -1), 2L);
      server.addEvent(recreatedDroppedPipeEvent, connector);

      Assert.assertTrue(recreatedDroppedPipeEvent.isReleased());

      final PipeRawTabletInsertionEvent recreatedPipeEvent =
          createPipeRawTabletInsertionEvent(pipeName, 2L, 1);
      recreatedPipeEvent.increaseReferenceCount(WebSocketSink.class.getName());
      recreatedPipeEvent.setCommitterKeyAndCommitId(new CommitterKey(pipeName, 2L, 1, -1), 3L);
      server.addEvent(recreatedPipeEvent, connector);

      Assert.assertFalse(recreatedPipeEvent.isReleased());
    } finally {
      server.unregister(connector);
    }
  }

  @Test
  public void testOpcUaSink() {
    final List<IMeasurementSchema> schemaList =
        Arrays.asList(
            new MeasurementSchema("s1", TSDataType.INT64),
            new MeasurementSchema("s2", TSDataType.INT64));

    final Tablet tablet = new Tablet("root.db.d1.vector6", schemaList, 100);

    long timestamp = System.currentTimeMillis();
    for (long row = 0; row < 100; row++) {
      final int rowSize = tablet.getRowSize();
      tablet.addTimestamp(rowSize, timestamp);
      for (int i = 0; i < 2; i++) {
        tablet.addValue(
            schemaList.get(i).getMeasurementName(), rowSize, new SecureRandom().nextLong());
      }
      timestamp++;
    }

    final List<IMeasurementSchema> opcSchemaList =
        Arrays.asList(
            new MeasurementSchema("value1", TSDataType.INT64),
            new MeasurementSchema("quality1", TSDataType.BOOLEAN));
    final Tablet qualityTablet = new Tablet("root.db.d1.vector6.s3", opcSchemaList, 100);

    timestamp = System.currentTimeMillis();
    for (long row = 0; row < 100; row++) {
      final int rowSize = qualityTablet.getRowSize();
      qualityTablet.addTimestamp(rowSize, timestamp);
      qualityTablet.addValue(
          opcSchemaList.get(0).getMeasurementName(), rowSize, new SecureRandom().nextLong());
      qualityTablet.addValue(opcSchemaList.get(1).getMeasurementName(), rowSize, true);
      timestamp++;
    }

    try (final OpcUaSink qualityOPC = new OpcUaSink();
        final OpcUaSink normalOPC = new OpcUaSink()) {
      final PipeTaskRuntimeConfiguration configuration =
          new PipeTaskRuntimeConfiguration(new PipeTaskSinkRuntimeEnvironment("temp", 0, 1));
      qualityOPC.customize(
          new PipeParameters(
              new HashMap<String, String>() {
                {
                  put(
                      PipeSinkConstant.CONNECTOR_KEY,
                      BuiltinPipePlugin.OPC_UA_SINK.getPipePluginName());
                  put(PipeSinkConstant.CONNECTOR_OPC_UA_WITH_QUALITY_KEY, "true");
                  put(PipeSinkConstant.CONNECTOR_OPC_UA_VALUE_NAME_KEY, "value1");
                  put(PipeSinkConstant.CONNECTOR_OPC_UA_QUALITY_NAME_KEY, "quality1");
                }
              }),
          configuration);
      normalOPC.customize(
          new PipeParameters(
              new HashMap<String, String>() {
                {
                  put(
                      PipeSinkConstant.CONNECTOR_KEY,
                      BuiltinPipePlugin.OPC_UA_SINK.getPipePluginName());
                }
              }),
          configuration);
      final PipeRawTabletInsertionEvent event =
          new PipeRawTabletInsertionEvent(
              false, "root.db", "db", "root.db", tablet, false, "pipe", 0L, null, null, false);
      event.increaseReferenceCount("");
      normalOPC.transfer(event);
      // Shall not throw
      qualityOPC.transfer(event);
      event.decreaseReferenceCount("", false);

      qualityOPC.transfer(
          new PipeRawTabletInsertionEvent(
              false,
              "root.db",
              "db",
              "root.db",
              qualityTablet,
              false,
              "pipe",
              0L,
              null,
              null,
              false));

    } catch (Exception e) {
      Assert.fail();
    }
  }

  @Test
  public void testWriteBackSinkTargetDatabaseValidation() throws Exception {
    assertWriteBackSinkTargetDatabaseValid("target");
    assertWriteBackSinkTargetDatabaseValid("root.target");
    assertWriteBackSinkTargetDatabaseValid("root.target.db");

    Assert.assertThrows(PipeException.class, () -> assertWriteBackSinkTargetDatabaseValid("a.b"));
    Assert.assertThrows(
        PipeException.class, () -> assertWriteBackSinkTargetDatabaseValid("a".repeat(65)));
    Assert.assertThrows(
        PipeException.class, () -> assertWriteBackSinkTargetDatabaseValid("root.a+b"));
    Assert.assertThrows(
        PipeException.class,
        () -> assertWriteBackSinkTargetDatabaseValid("root." + "a".repeat(60)));
  }

  @Test
  public void testWriteBackSinkTargetDatabaseCustomization() throws Exception {
    try (final WriteBackSink sink = createCustomizedWriteBackSink("TestTarget")) {
      Assert.assertEquals(
          "testtarget", getWriteBackSinkDatabaseName(sink, "targetTableModelDatabaseName"));
      Assert.assertNull(getWriteBackSinkDatabaseName(sink, "invalidTargetTableModelDatabaseName"));
      Assert.assertEquals(
          "root.testtarget", getWriteBackSinkDatabaseName(sink, "targetTreeModelDatabaseName"));
    }

    try (final WriteBackSink sink = createCustomizedWriteBackSink("root.target")) {
      Assert.assertEquals(
          "target", getWriteBackSinkDatabaseName(sink, "targetTableModelDatabaseName"));
      Assert.assertNull(getWriteBackSinkDatabaseName(sink, "invalidTargetTableModelDatabaseName"));
      Assert.assertEquals(
          "root.target", getWriteBackSinkDatabaseName(sink, "targetTreeModelDatabaseName"));
    }

    try (final WriteBackSink sink = createCustomizedWriteBackSink("root.target.db")) {
      Assert.assertNull(getWriteBackSinkDatabaseName(sink, "targetTableModelDatabaseName"));
      Assert.assertEquals(
          "target.db", getWriteBackSinkDatabaseName(sink, "invalidTargetTableModelDatabaseName"));
      Assert.assertEquals(
          "root.target.db", getWriteBackSinkDatabaseName(sink, "targetTreeModelDatabaseName"));
    }
  }

  @Test
  public void testWriteBackSinkRejectsInvalidTableModelDatabaseFromEvent() {
    try (final WriteBackSink sink = new WriteBackSink()) {
      final PipeRawTabletInsertionEvent event = createTableModelRawTabletInsertionEvent("root.a.b");
      Assert.assertThrows(PipeException.class, () -> sink.transfer(event));
    } catch (final Exception e) {
      Assert.fail(e.getMessage());
    }
  }

  @Test
  public void testWriteBackSinkRejectsInvalidTableModelDatabaseFromEventWithTargetDatabase()
      throws Exception {
    final PipeParameters parameters =
        new PipeParameters(Collections.singletonMap("sink.database", "target"));

    try (final WriteBackSink sink = new WriteBackSink()) {
      sink.validate(new PipeParameterValidator(parameters));
      sink.customize(
          parameters,
          new PipeTaskRuntimeConfiguration(new PipeTaskSinkRuntimeEnvironment("pipe", 1L, 1)));

      final PipeRawTabletInsertionEvent event = createTableModelRawTabletInsertionEvent("root.a.b");
      Assert.assertThrows(PipeException.class, () -> sink.transfer(event));
    }
  }

  @Test
  public void testWriteBackSinkRejectsInvalidTreeModelDatabaseFromEventWithTargetDatabase()
      throws Exception {
    final PipeParameters parameters =
        new PipeParameters(Collections.singletonMap("sink.database", "root.target"));

    try (final WriteBackSink sink = new WriteBackSink()) {
      sink.validate(new PipeParameterValidator(parameters));
      sink.customize(
          parameters,
          new PipeTaskRuntimeConfiguration(new PipeTaskSinkRuntimeEnvironment("pipe", 1L, 1)));

      final PipeRawTabletInsertionEvent event = createTreeModelRawTabletInsertionEvent("root.a+b");
      Assert.assertThrows(PipeException.class, () -> sink.transfer(event));
    }
  }

  @Test
  public void testWriteBackSinkRejectsInvalidStatementEventDatabases() throws Exception {
    try (final WriteBackSink sink = createCustomizedWriteBackSink("target")) {
      Assert.assertThrows(
          PipeException.class,
          () -> sink.transfer(createTableModelStatementInsertionEvent("root.a.b")));
    }

    try (final WriteBackSink sink = createCustomizedWriteBackSink("root.target")) {
      Assert.assertThrows(
          PipeException.class,
          () -> sink.transfer(createTreeModelStatementInsertionEvent("root.a+b")));
    }

    try (final WriteBackSink sink = createCustomizedWriteBackSink("root.target.db")) {
      Assert.assertThrows(
          PipeException.class,
          () -> sink.transfer(createTableModelStatementInsertionEvent("valid_db")));
    }
  }

  @Test
  public void testWriteBackSinkRejectsInvalidTableModelDatabaseFromTreeTarget() throws Exception {
    final PipeParameters parameters =
        new PipeParameters(
            new HashMap<String, String>() {
              {
                put("sink.database", "root.target.db");
              }
            });

    try (final WriteBackSink sink = new WriteBackSink()) {
      sink.validate(new PipeParameterValidator(parameters));
      sink.customize(
          parameters,
          new PipeTaskRuntimeConfiguration(new PipeTaskSinkRuntimeEnvironment("pipe", 1L, 1)));

      final PipeRawTabletInsertionEvent event = createTableModelRawTabletInsertionEvent("valid_db");
      Assert.assertThrows(PipeException.class, () -> sink.transfer(event));
    }
  }

  private void assertWriteBackSinkTargetDatabaseValid(final String targetDatabase)
      throws Exception {
    try (final WriteBackSink sink = new WriteBackSink()) {
      sink.validate(
          new PipeParameterValidator(
              new PipeParameters(Collections.singletonMap("sink.database", targetDatabase))));
    }
  }

  private WriteBackSink createCustomizedWriteBackSink(final String targetDatabase)
      throws Exception {
    final PipeParameters parameters =
        new PipeParameters(Collections.singletonMap("sink.database", targetDatabase));
    final WriteBackSink sink = new WriteBackSink();
    sink.validate(new PipeParameterValidator(parameters));
    sink.customize(
        parameters,
        new PipeTaskRuntimeConfiguration(new PipeTaskSinkRuntimeEnvironment("pipe", 1L, 1)));
    return sink;
  }

  private String getWriteBackSinkDatabaseName(final WriteBackSink sink, final String fieldName)
      throws Exception {
    final Field field = WriteBackSink.class.getDeclaredField(fieldName);
    field.setAccessible(true);
    return (String) field.get(sink);
  }

  private PipeRawTabletInsertionEvent createTableModelRawTabletInsertionEvent(
      final String databaseName) {
    final List<IMeasurementSchema> schemaList =
        Arrays.asList(new MeasurementSchema("s1", TSDataType.INT64));
    final Tablet tablet = new Tablet("table", schemaList, 1);
    tablet.addTimestamp(0, 1L);
    tablet.addValue("s1", 0, 1L);
    return new PipeRawTabletInsertionEvent(
        true, databaseName, null, null, tablet, false, "pipe", 0L, null, null, false);
  }

  private PipeRawTabletInsertionEvent createTreeModelRawTabletInsertionEvent(
      final String databaseName) {
    final List<IMeasurementSchema> schemaList =
        Arrays.asList(new MeasurementSchema("s1", TSDataType.INT64));
    final Tablet tablet = new Tablet(databaseName + ".d1", schemaList, 1);
    tablet.addTimestamp(0, 1L);
    tablet.addValue("s1", 0, 1L);
    return new PipeRawTabletInsertionEvent(
        false, databaseName, null, databaseName, tablet, false, "pipe", 0L, null, null, false);
  }

  private PipeStatementInsertionEvent createTableModelStatementInsertionEvent(
      final String databaseName) {
    return createStatementInsertionEvent(true, databaseName);
  }

  private PipeStatementInsertionEvent createTreeModelStatementInsertionEvent(
      final String databaseName) {
    return createStatementInsertionEvent(false, databaseName);
  }

  private PipeStatementInsertionEvent createStatementInsertionEvent(
      final boolean isTableModelEvent, final String databaseName) {
    final InsertTabletStatement statement = new InsertTabletStatement();
    statement.setRamBytesUsed(1L);
    return new PipeStatementInsertionEvent(
        "pipe",
        0L,
        null,
        null,
        null,
        null,
        null,
        null,
        true,
        isTableModelEvent,
        databaseName,
        statement);
  }

  private PipeRawTabletInsertionEvent createPipeRawTabletInsertionEvent(
      final String pipeName, final long creationTime, final int regionId) {
    final List<IMeasurementSchema> schemaList =
        Arrays.asList(new MeasurementSchema("s1", TSDataType.INT64));
    final Tablet tablet = new Tablet("root.db.d" + regionId, schemaList, 1);
    tablet.addTimestamp(0, 1L);
    tablet.addValue("s1", 0, 1L);
    return new PipeRawTabletInsertionEvent(
        false,
        "root.db",
        "db",
        "root.db",
        tablet,
        false,
        pipeName,
        creationTime,
        null,
        null,
        false);
  }
}
