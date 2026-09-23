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

package org.apache.iotdb.opcua;

import org.eclipse.milo.opcua.sdk.client.OpcUaClient;
import org.eclipse.milo.opcua.sdk.client.subscriptions.OpcUaMonitoredItem;
import org.eclipse.milo.opcua.sdk.client.subscriptions.OpcUaSubscription;
import org.eclipse.milo.opcua.stack.core.AttributeId;
import org.eclipse.milo.opcua.stack.core.Identifiers;
import org.eclipse.milo.opcua.stack.core.types.builtin.QualifiedName;
import org.eclipse.milo.opcua.stack.core.types.enumerated.MonitoringMode;
import org.eclipse.milo.opcua.stack.core.types.structured.ContentFilter;
import org.eclipse.milo.opcua.stack.core.types.structured.EventFilter;
import org.eclipse.milo.opcua.stack.core.types.structured.ReadValueId;
import org.eclipse.milo.opcua.stack.core.types.structured.SimpleAttributeOperand;

import java.util.concurrent.CompletableFuture;

import static org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.Unsigned.uint;

public class ClientTest implements ClientExample {

  public static void main(String[] args) {
    final ClientTest example = new ClientTest();

    new ClientExampleRunner(example).run();
  }

  @Override
  public void run(OpcUaClient client, CompletableFuture<OpcUaClient> future) throws Exception {
    // synchronous connect
    client.connect();

    // create a subscription and a monitored item
    final OpcUaSubscription subscription = new OpcUaSubscription(client, 200.0);
    subscription.create();

    final ReadValueId readValueId =
        new ReadValueId(
            Identifiers.Server, AttributeId.EventNotifier.uid(), null, QualifiedName.NULL_VALUE);

    final EventFilter eventFilter =
        new EventFilter(
            new SimpleAttributeOperand[] {
              new SimpleAttributeOperand(
                  Identifiers.BaseEventType,
                  new QualifiedName[] {new QualifiedName(0, "Time")},
                  AttributeId.Value.uid(),
                  null),
              new SimpleAttributeOperand(
                  Identifiers.BaseEventType,
                  new QualifiedName[] {new QualifiedName(0, "Message")},
                  AttributeId.Value.uid(),
                  null),
              new SimpleAttributeOperand(
                  Identifiers.BaseEventType,
                  new QualifiedName[] {new QualifiedName(0, "SourceName")},
                  AttributeId.Value.uid(),
                  null),
              new SimpleAttributeOperand(
                  Identifiers.BaseEventType,
                  new QualifiedName[] {new QualifiedName(0, "SourceNode")},
                  AttributeId.Value.uid(),
                  null)
            },
            new ContentFilter(null));

    final OpcUaMonitoredItem monitoredItem =
        new OpcUaMonitoredItem(readValueId, MonitoringMode.Reporting);
    monitoredItem.setSamplingInterval(0.0);
    monitoredItem.setFilter(eventFilter);
    monitoredItem.setQueueSize(uint(10000));
    monitoredItem.setDiscardOldest(true);
    subscription.addMonitoredItem(monitoredItem);
    subscription.synchronizeMonitoredItems();

    // do something with the value updates
    monitoredItem.setEventValueListener(
        (item, vs) -> {
          System.out.println("Event Received from " + item.getReadValueId().getNodeId());

          for (int i = 0; i < vs.length; i++) {
            System.out.println(("\tvariant[" + i + "]: " + vs[i].getValue()));
          }
        });
  }
}
