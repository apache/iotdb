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

package org.apache.iotdb.commons.pipe.plugin.builtin;

import org.apache.iotdb.commons.pipe.agent.plugin.builtin.connector.donothing.DoNothingConnector;
import org.apache.iotdb.commons.pipe.agent.plugin.builtin.connector.iotdb.thrift.IoTDBLegacyPipeConnector;
import org.apache.iotdb.commons.pipe.agent.plugin.builtin.connector.iotdb.thrift.IoTDBThriftConnector;
import org.apache.iotdb.commons.pipe.agent.plugin.builtin.extractor.iotdb.IoTDBExtractor;
import org.apache.iotdb.commons.pipe.agent.plugin.builtin.processor.donothing.DoNothingProcessor;
import org.apache.iotdb.pipe.api.PipeConnector;
import org.apache.iotdb.pipe.api.PipeExtractor;
import org.apache.iotdb.pipe.api.PipeProcessor;
import org.apache.iotdb.pipe.api.collector.EventCollector;
import org.apache.iotdb.pipe.api.customizer.configuration.PipeConnectorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.configuration.PipeExtractorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.configuration.PipeProcessorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.event.dml.insertion.TsFileInsertionEvent;

import org.junit.Assert;
import org.junit.Test;

import static org.mockito.Mockito.mock;

public class BuiltinPipePluginTest {
  @Test
  public void testBuildInPipePlugin() {
    PipeExtractor extractor = new IoTDBExtractor();
    try {
      extractor.validate(mock(PipeParameterValidator.class));
      Assert.fail();
    } catch (Exception ignored) {
    }
    try {
      extractor.customize(
          mock(PipeParameters.class), mock(PipeExtractorRuntimeConfiguration.class));
      Assert.fail();
    } catch (Exception ignored) {
    }
    try {
      extractor.start();
      Assert.fail();
    } catch (Exception ignored) {
    }
    try {
      extractor.supply();
      Assert.fail();
    } catch (Exception ignored) {
    }
    try {
      extractor.close();
      Assert.fail();
    } catch (Exception ignored) {
    }

    PipeProcessor processor = new DoNothingProcessor();
    try {
      processor.validate(mock(PipeParameterValidator.class));
    } catch (Exception ignored) {
      Assert.fail();
    }
    try {
      processor.customize(
          mock(PipeParameters.class), mock(PipeProcessorRuntimeConfiguration.class));
    } catch (Exception ignored) {
      Assert.fail();
    }
    try {
      processor.process(mock(TabletInsertionEvent.class), mock(EventCollector.class));
    } catch (Exception ignored) {
      Assert.fail();
    }
    try {
      processor.process(mock(TsFileInsertionEvent.class), mock(EventCollector.class));
    } catch (Exception ignored) {
      Assert.fail();
    }
    try {
      processor.process(mock(Event.class), mock(EventCollector.class));
    } catch (Exception ignored) {
      Assert.fail();
    }
    try {
      processor.close();
    } catch (Exception ignored) {
      Assert.fail();
    }

    PipeConnector connector = new DoNothingConnector();
    try {
      connector.validate(mock(PipeParameterValidator.class));
      connector.customize(
          mock(PipeParameters.class), mock(PipeConnectorRuntimeConfiguration.class));
      connector.handshake();
      connector.heartbeat();
      connector.transfer(mock(TabletInsertionEvent.class));
      connector.transfer(mock(TsFileInsertionEvent.class));
      connector.transfer(mock(Event.class));
      connector.close();
    } catch (Exception e) {
      Assert.fail();
    }

    testConnectorAllThrow(new IoTDBLegacyPipeConnector());
    testConnectorAllThrow(new IoTDBThriftConnector());
  }

  private void testConnectorAllThrow(PipeConnector connector) {
    try {
      connector.validate(mock(PipeParameterValidator.class));
      Assert.fail();
    } catch (Exception ignored) {
    }
    try {
      connector.customize(
          mock(PipeParameters.class), mock(PipeConnectorRuntimeConfiguration.class));
      Assert.fail();
    } catch (Exception ignored) {
    }
    try {
      connector.handshake();
      Assert.fail();
    } catch (Exception ignored) {
    }
    try {
      connector.heartbeat();
      Assert.fail();
    } catch (Exception ignored) {
    }
    try {
      connector.transfer(mock(TabletInsertionEvent.class));
      Assert.fail();
    } catch (Exception ignored) {
    }
    try {
      connector.transfer(mock(TsFileInsertionEvent.class));
      Assert.fail();
    } catch (Exception ignored) {
    }
    try {
      connector.transfer(mock(Event.class));
      Assert.fail();
    } catch (Exception ignored) {
    }
    try {
      connector.close();
      Assert.fail();
    } catch (Exception ignored) {
    }
  }
}
