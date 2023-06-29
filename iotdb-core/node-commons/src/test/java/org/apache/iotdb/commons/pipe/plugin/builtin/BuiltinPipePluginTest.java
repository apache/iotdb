package org.apache.iotdb.commons.pipe.plugin.builtin;

import org.apache.iotdb.commons.pipe.plugin.builtin.connector.DoNothingConnector;
import org.apache.iotdb.commons.pipe.plugin.builtin.connector.IoTDBSyncConnector;
import org.apache.iotdb.commons.pipe.plugin.builtin.connector.IoTDBThriftConnector;
import org.apache.iotdb.commons.pipe.plugin.builtin.extractor.IoTDBExtractor;
import org.apache.iotdb.commons.pipe.plugin.builtin.processor.DoNothingProcessor;
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
      Assert.fail();
    } catch (Exception ignored) {
    }
    try {
      processor.customize(
          mock(PipeParameters.class), mock(PipeProcessorRuntimeConfiguration.class));
      Assert.fail();
    } catch (Exception ignored) {
    }
    try {
      processor.process(mock(TabletInsertionEvent.class), mock(EventCollector.class));
      Assert.fail();
    } catch (Exception ignored) {
    }
    try {
      processor.process(mock(TsFileInsertionEvent.class), mock(EventCollector.class));
      Assert.fail();
    } catch (Exception ignored) {
    }
    try {
      processor.process(mock(Event.class), mock(EventCollector.class));
      Assert.fail();
    } catch (Exception ignored) {
    }
    try {
      processor.close();
      Assert.fail();
    } catch (Exception ignored) {
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

    testConnectorAllThrow(new IoTDBSyncConnector());
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
