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

package org.apache.iotdb.pipe.plugin.sink.opcua;

import org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant;
import org.apache.iotdb.commons.pipe.config.plugin.configuraion.PipeTaskRuntimeConfiguration;
import org.apache.iotdb.commons.pipe.config.plugin.env.PipeTaskSinkRuntimeEnvironment;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.exception.PipeException;
import org.apache.iotdb.pipe.api.exception.PipeParameterNotValidException;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.eclipse.milo.opcua.stack.core.types.builtin.StatusCode;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class OpcUaSinkTest {

  @Test
  public void testValidateClientServerOnlyOptions() {
    assertValidationFailure(
        "must be client-server",
        PipeSinkConstant.CONNECTOR_OPC_UA_WITH_QUALITY_KEY,
        "true",
        PipeSinkConstant.CONNECTOR_OPC_UA_MODEL_KEY,
        PipeSinkConstant.CONNECTOR_OPC_UA_MODEL_PUB_SUB_VALUE);

    assertValidationFailure(
        "must be client-server",
        PipeSinkConstant.CONNECTOR_OPC_UA_NODE_URL_KEY,
        "opc.tcp://127.0.0.1:12686/iotdb",
        PipeSinkConstant.CONNECTOR_OPC_UA_MODEL_KEY,
        PipeSinkConstant.CONNECTOR_OPC_UA_MODEL_PUB_SUB_VALUE);
  }

  @Test
  public void testCustomizeQualityParameters() throws Exception {
    final int[] qualityPorts = findTwoFreePorts();
    final int[] normalPorts = findTwoFreePorts();
    try (final OpcUaSink qualitySink = new OpcUaSink();
        final OpcUaSink normalSink = new OpcUaSink()) {
      qualitySink.customize(
          createParameters(
              PipeSinkConstant.CONNECTOR_KEY,
              PipeSinkConstant.OPC_UA_SINK_NAME,
              PipeSinkConstant.CONNECTOR_OPC_UA_WITH_QUALITY_KEY,
              "true",
              PipeSinkConstant.CONNECTOR_OPC_UA_VALUE_NAME_KEY,
              "value1",
              PipeSinkConstant.CONNECTOR_OPC_UA_QUALITY_NAME_KEY,
              "quality1",
              PipeSinkConstant.CONNECTOR_OPC_UA_DEFAULT_QUALITY_KEY,
              "BAD",
              PipeSinkConstant.CONNECTOR_OPC_UA_SECURITY_POLICY_KEY,
              "None",
              PipeSinkConstant.CONNECTOR_OPC_UA_TCP_BIND_PORT_KEY,
              Integer.toString(qualityPorts[0]),
              PipeSinkConstant.CONNECTOR_OPC_UA_HTTPS_BIND_PORT_KEY,
              Integer.toString(qualityPorts[1]),
              PipeSinkConstant.CONNECTOR_OPC_UA_SECURITY_DIR_KEY,
              createSecurityDir()),
          createRuntimeConfiguration());
      normalSink.customize(
          createOpcUaServerParameters(normalPorts[0], normalPorts[1], "root", "root"),
          createRuntimeConfiguration());

      Assert.assertEquals("value1", qualitySink.getValueName());
      Assert.assertEquals("quality1", qualitySink.getQualityName());
      Assert.assertEquals(StatusCode.BAD, qualitySink.getDefaultQuality());
      Assert.assertNull(normalSink.getValueName());
      Assert.assertNull(normalSink.getQualityName());
      Assert.assertEquals(StatusCode.GOOD, normalSink.getDefaultQuality());
    }
  }

  @Test
  public void testTransferWithQualityAndNormalTablets() throws Exception {
    final List<IMeasurementSchema> schemaList =
        Arrays.asList(
            new MeasurementSchema("s1", TSDataType.INT64),
            new MeasurementSchema("s2", TSDataType.INT64));
    final Tablet tablet = new Tablet("root.db.d1.vector6", schemaList, 100);
    long timestamp = System.currentTimeMillis();
    for (long row = 0; row < 100; row++) {
      final int rowSize = tablet.getRowSize();
      tablet.addTimestamp(rowSize, timestamp++);
      for (int i = 0; i < 2; i++) {
        tablet.addValue(
            schemaList.get(i).getMeasurementName(), rowSize, new SecureRandom().nextLong());
      }
    }

    final List<IMeasurementSchema> opcSchemaList =
        Arrays.asList(
            new MeasurementSchema("value1", TSDataType.INT64),
            new MeasurementSchema("quality1", TSDataType.BOOLEAN));
    final Tablet qualityTablet = new Tablet("root.db.d1.vector6.s3", opcSchemaList, 100);
    timestamp = System.currentTimeMillis();
    for (long row = 0; row < 100; row++) {
      final int rowSize = qualityTablet.getRowSize();
      qualityTablet.addTimestamp(rowSize, timestamp++);
      qualityTablet.addValue(
          opcSchemaList.get(0).getMeasurementName(), rowSize, new SecureRandom().nextLong());
      qualityTablet.addValue(opcSchemaList.get(1).getMeasurementName(), rowSize, true);
    }

    final int[] qualityPorts = findTwoFreePorts();
    final int[] normalPorts = findTwoFreePorts();
    try (final OpcUaSink qualitySink = new OpcUaSink();
        final OpcUaSink normalSink = new OpcUaSink()) {
      qualitySink.customize(
          createQualityServerParameters(qualityPorts[0], qualityPorts[1]),
          createRuntimeConfiguration());
      normalSink.customize(
          createOpcUaServerParameters(normalPorts[0], normalPorts[1], "root", "root"),
          createRuntimeConfiguration());

      final PipeRawTabletInsertionEvent event =
          new PipeRawTabletInsertionEvent(
              false, "root.db", "db", "root.db", tablet, false, "pipe", 0L, null, null, false);
      event.increaseReferenceCount("");
      normalSink.transfer(event);
      qualitySink.transfer(event);
      event.decreaseReferenceCount("", false);

      qualitySink.transfer(
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
    }
  }

  @Test
  public void testSharedServerLifecycle() throws Exception {
    final int[] ports = findTwoFreePorts();
    final PipeTaskRuntimeConfiguration configuration = createRuntimeConfiguration();
    final PipeParameters parameters =
        createOpcUaServerParameters(ports[0], ports[1], "root", "root");
    final PipeParameters conflictingParameters =
        createOpcUaServerParameters(ports[0], ports[1], "root", "conflict");

    try (final OpcUaSink firstSink = new OpcUaSink();
        final OpcUaSink secondSink = new OpcUaSink()) {
      firstSink.customize(parameters, configuration);
      secondSink.customize(parameters, configuration);

      assertCustomizeFailure(conflictingParameters, configuration);
      secondSink.close();
      secondSink.close();
      assertCustomizeFailure(conflictingParameters, configuration);
    }
  }

  private static void assertCustomizeFailure(
      final PipeParameters parameters, final PipeTaskRuntimeConfiguration configuration) {
    try (final OpcUaSink conflictingSink = new OpcUaSink()) {
      final PipeException exception =
          Assert.assertThrows(
              PipeException.class, () -> conflictingSink.customize(parameters, configuration));
      Assert.assertTrue(exception.getMessage(), exception.getMessage().contains("reject reusing"));
    } catch (final Exception e) {
      throw new AssertionError(e);
    }
  }

  private static void assertValidationFailure(
      final String expectedMessagePart, final String... keyValues) {
    final PipeParameterNotValidException exception =
        Assert.assertThrows(
            PipeParameterNotValidException.class,
            () ->
                new OpcUaSink().validate(new PipeParameterValidator(createParameters(keyValues))));
    Assert.assertTrue(exception.getMessage(), exception.getMessage().contains(expectedMessagePart));
  }

  private static PipeParameters createQualityServerParameters(
      final int tcpPort, final int httpsPort) {
    final Map<String, String> attributes =
        new HashMap<>(
            createOpcUaServerParameters(tcpPort, httpsPort, "root", "root").getAttribute());
    attributes.put(PipeSinkConstant.CONNECTOR_OPC_UA_WITH_QUALITY_KEY, "true");
    attributes.put(PipeSinkConstant.CONNECTOR_OPC_UA_VALUE_NAME_KEY, "value1");
    attributes.put(PipeSinkConstant.CONNECTOR_OPC_UA_QUALITY_NAME_KEY, "quality1");
    return new PipeParameters(attributes);
  }

  private static PipeParameters createOpcUaServerParameters(
      final int tcpPort, final int httpsPort, final String user, final String password) {
    return createParameters(
        PipeSinkConstant.CONNECTOR_KEY,
        PipeSinkConstant.OPC_UA_SINK_NAME,
        PipeSinkConstant.CONNECTOR_OPC_UA_TCP_BIND_PORT_KEY,
        Integer.toString(tcpPort),
        PipeSinkConstant.CONNECTOR_OPC_UA_HTTPS_BIND_PORT_KEY,
        Integer.toString(httpsPort),
        PipeSinkConstant.CONNECTOR_OPC_UA_SECURITY_POLICY_KEY,
        "None",
        PipeSinkConstant.CONNECTOR_IOTDB_USER_KEY,
        user,
        PipeSinkConstant.CONNECTOR_IOTDB_PASSWORD_KEY,
        password,
        PipeSinkConstant.CONNECTOR_OPC_UA_SECURITY_DIR_KEY,
        createSecurityDir(),
        PipeSinkConstant.CONNECTOR_OPC_UA_DEBOUNCE_TIME_MS_KEY,
        "1");
  }

  private static PipeParameters createParameters(final String... keyValues) {
    final Map<String, String> attributes = new HashMap<>();
    for (int i = 0; i < keyValues.length; i += 2) {
      attributes.put(keyValues[i], keyValues[i + 1]);
    }
    return new PipeParameters(attributes);
  }

  private static PipeTaskRuntimeConfiguration createRuntimeConfiguration() {
    return new PipeTaskRuntimeConfiguration(new PipeTaskSinkRuntimeEnvironment("temp", 0, 1));
  }

  private static String createSecurityDir() {
    return new File(
            "target"
                + File.separatorChar
                + "opc-ua-sink-test"
                + File.separatorChar
                + UUID.randomUUID())
        .getAbsolutePath();
  }

  private static int[] findTwoFreePorts() throws IOException {
    final int firstPort = findFreePort();
    int secondPort;
    do {
      secondPort = findFreePort();
    } while (secondPort == firstPort);
    return new int[] {firstPort, secondPort};
  }

  private static int findFreePort() throws IOException {
    try (final ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    }
  }
}
