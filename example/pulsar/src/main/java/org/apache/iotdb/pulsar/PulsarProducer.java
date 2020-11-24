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
package org.apache.iotdb.pulsar;

import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;

public class PulsarProducer {
  private static final String SERVICE_URL = "pulsar://localhost:6650";
  private static final String TOPIC_NAME = "test-topic";
  private final Producer<byte[]> producer ;

  public PulsarProducer(Producer<byte[]> producer) {
    this.producer = producer;
  }

  public void produce() throws PulsarClientException {
    producer.send("My message".getBytes());
  }

  public static void main(String[] args) throws PulsarClientException {
    PulsarClient client = PulsarClient.builder()
        .serviceUrl(SERVICE_URL)
        .build();

    PulsarProducer pulsarProducer = new PulsarProducer(client.newProducer()
        .topic(TOPIC_NAME).create());

    pulsarProducer.produce();
    System.out.println("Producing finished");
  }
}
