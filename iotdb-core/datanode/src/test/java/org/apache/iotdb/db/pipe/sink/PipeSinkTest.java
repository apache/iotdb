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
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.db.pipe.sink.protocol.legacy.IoTDBLegacyPipeSink;
import org.apache.iotdb.db.pipe.sink.protocol.thrift.async.IoTDBDataRegionAsyncSink;
import org.apache.iotdb.db.pipe.sink.protocol.thrift.sync.IoTDBDataRegionSyncSink;
import org.apache.iotdb.db.pipe.sink.protocol.websocket.WebSocketConnectorServer;
import org.apache.iotdb.db.pipe.sink.protocol.websocket.WebSocketSink;
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

import java.util.Arrays;
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
