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

package org.apache.iotdb.db.pipe.sink.protocol.opcua.client;

import org.apache.iotdb.db.pipe.sink.protocol.opcua.OpcUaSink;
import org.apache.iotdb.pipe.api.exception.PipeException;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.eclipse.milo.opcua.sdk.client.OpcUaClient;
import org.eclipse.milo.opcua.sdk.client.identity.AnonymousProvider;
import org.eclipse.milo.opcua.stack.core.StatusCodes;
import org.eclipse.milo.opcua.stack.core.security.SecurityPolicy;
import org.eclipse.milo.opcua.stack.core.types.builtin.NodeId;
import org.eclipse.milo.opcua.stack.core.types.builtin.StatusCode;
import org.eclipse.milo.opcua.stack.core.types.structured.AddNodesItem;
import org.eclipse.milo.opcua.stack.core.types.structured.AddNodesResponse;
import org.eclipse.milo.opcua.stack.core.types.structured.AddNodesResult;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentMatcher;
import org.mockito.InOrder;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class IoTDBOpcUaClientTest {

  @Test
  public void testTransferWritesAllMeasurementsInOneRequest() throws Exception {
    final OpcUaClient miloClient = Mockito.mock(OpcUaClient.class);
    Mockito.when(miloClient.writeValuesAsync(Mockito.anyList(), Mockito.anyList()))
        .thenReturn(
            CompletableFuture.completedFuture(Arrays.asList(StatusCode.GOOD, StatusCode.GOOD)));
    final IoTDBOpcUaClient client = createClient(miloClient);

    client.transfer(createTablet(), createSink());

    Mockito.verify(miloClient)
        .writeValuesAsync(
            Mockito.argThat(nodeIds("root/db/d1/s1", "root/db/d1/s2")),
            Mockito.argThat(listWithSize(2)));
  }

  @Test
  public void testTransferCreatesAndRetriesOnlyMissingNodes() throws Exception {
    final OpcUaClient miloClient = Mockito.mock(OpcUaClient.class);
    Mockito.when(miloClient.writeValuesAsync(Mockito.anyList(), Mockito.anyList()))
        .thenReturn(
            CompletableFuture.completedFuture(
                Arrays.asList(new StatusCode(StatusCodes.Bad_NodeIdUnknown), StatusCode.GOOD)))
        .thenReturn(CompletableFuture.completedFuture(Collections.singletonList(StatusCode.GOOD)));

    final AddNodesResponse addNodesResponse = Mockito.mock(AddNodesResponse.class);
    final AddNodesResult addNodesResult = Mockito.mock(AddNodesResult.class);
    Mockito.when(addNodesResult.getStatusCode()).thenReturn(StatusCode.GOOD);
    Mockito.when(addNodesResponse.getResults()).thenReturn(new AddNodesResult[] {addNodesResult});
    Mockito.when(miloClient.addNodesAsync(Mockito.anyList()))
        .thenReturn(CompletableFuture.completedFuture(addNodesResponse));

    final IoTDBOpcUaClient client = Mockito.spy(createClient(miloClient));
    final AddNodesItem nodeToAdd = Mockito.mock(AddNodesItem.class);
    Mockito.doReturn(Arrays.asList(nodeToAdd, nodeToAdd))
        .when(client)
        .getNodesToAdd(
            Mockito.any(String[].class),
            Mockito.eq("s1"),
            Mockito.any(NodeId.class),
            Mockito.any());

    client.transfer(createTablet(), createSink());

    final InOrder inOrder = Mockito.inOrder(miloClient);
    inOrder
        .verify(miloClient)
        .writeValuesAsync(Mockito.argThat(listWithSize(2)), Mockito.argThat(listWithSize(2)));
    inOrder.verify(miloClient).addNodesAsync(Mockito.argThat(listWithSize(1)));
    inOrder
        .verify(miloClient)
        .writeValuesAsync(
            Mockito.argThat(nodeIds("root/db/d1/s1")), Mockito.argThat(listWithSize(1)));
  }

  @Test
  public void testTransferFailsOnNonRecoverableStatus() throws Exception {
    final OpcUaClient miloClient = Mockito.mock(OpcUaClient.class);
    Mockito.when(miloClient.writeValuesAsync(Mockito.anyList(), Mockito.anyList()))
        .thenReturn(
            CompletableFuture.completedFuture(
                Arrays.asList(new StatusCode(StatusCodes.Bad_NotWritable), StatusCode.GOOD)));
    final IoTDBOpcUaClient client = createClient(miloClient);

    try {
      client.transfer(createTablet(), createSink());
      Assert.fail();
    } catch (final PipeException e) {
      Assert.assertTrue(e.getMessage().contains("root.db.d1.s1"));
      Assert.assertTrue(e.getMessage().contains("Bad_NotWritable"));
    }

    Mockito.verify(miloClient, Mockito.never()).addNodesAsync(Mockito.anyList());
  }

  private static IoTDBOpcUaClient createClient(final OpcUaClient miloClient) throws Exception {
    final IoTDBOpcUaClient client =
        new IoTDBOpcUaClient(
            "opc.tcp://127.0.0.1:12686", SecurityPolicy.None, new AnonymousProvider(), false);
    final ClientRunner runner = Mockito.mock(ClientRunner.class);
    Mockito.when(runner.getTimeoutSeconds()).thenReturn(1L);
    client.setRunner(runner);
    final CompletableFuture<OpcUaClient> connectFuture =
        CompletableFuture.completedFuture(miloClient);
    Mockito.when(miloClient.connectAsync()).thenReturn(connectFuture);
    client.run(miloClient);
    return client;
  }

  private static OpcUaSink createSink() {
    final OpcUaSink sink = Mockito.mock(OpcUaSink.class);
    Mockito.when(sink.getDefaultQuality()).thenReturn(StatusCode.GOOD);
    return sink;
  }

  private static Tablet createTablet() {
    final Tablet tablet =
        new Tablet(
            "root.db.d1",
            Arrays.asList(
                new MeasurementSchema("s1", TSDataType.INT64),
                new MeasurementSchema("s2", TSDataType.DOUBLE)),
            1);
    tablet.addTimestamp(0, 1L);
    tablet.addValue("s1", 0, 1L);
    tablet.addValue("s2", 0, 2.0D);
    tablet.rowSize = 1;
    return tablet;
  }

  private static ArgumentMatcher<List<NodeId>> nodeIds(final String... identifiers) {
    return nodeIds -> {
      if (nodeIds.size() != identifiers.length) {
        return false;
      }
      for (int i = 0; i < identifiers.length; ++i) {
        if (!new NodeId(2, identifiers[i]).equals(nodeIds.get(i))) {
          return false;
        }
      }
      return true;
    };
  }

  private static <T> ArgumentMatcher<List<T>> listWithSize(final int size) {
    return values -> values.size() == size;
  }
}
