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

import org.apache.iotdb.commons.pipe.agent.plugin.builtin.processor.donothing.DoNothingProcessor;
import org.apache.iotdb.commons.pipe.agent.plugin.builtin.sink.donothing.DoNothingSink;
import org.apache.iotdb.commons.pipe.agent.plugin.builtin.sink.iotdb.thrift.IoTDBLegacyPipeSink;
import org.apache.iotdb.commons.pipe.agent.plugin.builtin.sink.iotdb.thrift.IoTDBThriftSink;
import org.apache.iotdb.commons.pipe.agent.plugin.builtin.source.iotdb.IoTDBSource;
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
import static org.mockito.Mockito.verify;

public class BuiltinPipePluginTest {
  @Test
  public void testBuildInPipePlugin() {
    testExtractorAllThrow(new IoTDBSource());

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

    final EventCollector collector = mock(EventCollector.class);
    final TabletInsertionEvent tabletInsertionEvent = mock(TabletInsertionEvent.class);
    final TsFileInsertionEvent tsFileInsertionEvent = mock(TsFileInsertionEvent.class);
    final Event event = mock(Event.class);
    try {
      processor.process(tabletInsertionEvent, collector);
      verify(collector).collect(tabletInsertionEvent);
    } catch (Exception ignored) {
      Assert.fail();
    }
    try {
      processor.process(tsFileInsertionEvent, collector);
      verify(collector).collect(tsFileInsertionEvent);
    } catch (Exception ignored) {
      Assert.fail();
    }
    try {
      processor.process(event, collector);
      verify(collector).collect(event);
    } catch (Exception ignored) {
      Assert.fail();
    }
    try {
      processor.close();
    } catch (Exception ignored) {
      Assert.fail();
    }

    PipeConnector connector = new DoNothingSink();
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

    testConnectorAllThrow(new IoTDBLegacyPipeSink());
    testConnectorAllThrow(new IoTDBThriftSink());
  }

  private void testExtractorAllThrow(final PipeExtractor extractor) {
    Assert.assertThrows(
        UnsupportedOperationException.class,
        () -> extractor.validate(mock(PipeParameterValidator.class)));
    Assert.assertThrows(
        UnsupportedOperationException.class,
        () ->
            extractor.customize(
                mock(PipeParameters.class), mock(PipeExtractorRuntimeConfiguration.class)));
    Assert.assertThrows(UnsupportedOperationException.class, extractor::start);
    Assert.assertThrows(UnsupportedOperationException.class, extractor::supply);
    Assert.assertThrows(UnsupportedOperationException.class, extractor::close);
  }

  private void testConnectorAllThrow(final PipeConnector connector) {
    Assert.assertThrows(
        UnsupportedOperationException.class,
        () -> connector.validate(mock(PipeParameterValidator.class)));
    Assert.assertThrows(
        UnsupportedOperationException.class,
        () ->
            connector.customize(
                mock(PipeParameters.class), mock(PipeConnectorRuntimeConfiguration.class)));
    Assert.assertThrows(UnsupportedOperationException.class, connector::handshake);
    Assert.assertThrows(UnsupportedOperationException.class, connector::heartbeat);
    Assert.assertThrows(
        UnsupportedOperationException.class,
        () -> connector.transfer(mock(TabletInsertionEvent.class)));
    Assert.assertThrows(
        UnsupportedOperationException.class,
        () -> connector.transfer(mock(TsFileInsertionEvent.class)));
    Assert.assertThrows(
        UnsupportedOperationException.class, () -> connector.transfer(mock(Event.class)));
    Assert.assertThrows(UnsupportedOperationException.class, connector::close);
  }
}
