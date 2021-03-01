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

import org.apache.pulsar.client.api.BatcherBuilder;
import org.apache.pulsar.client.api.HashingScheme;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.schema.StringSchema;

public class PulsarProducer {
  private static final String SERVICE_URL = "pulsar://localhost:6650";
  private final Producer<String> producer;
  private final PulsarClient client = PulsarClient.builder().serviceUrl(SERVICE_URL).build();
  protected static final String[] ALL_DATA = {
    "device1,sensor1,2017/10/24 19:30:00,606162908",
    "device1,sensor2,2017/10/24 19:30:00,160161162",
    "device2,sensor1,2017/10/24 19:30:00,360361362",
    "device2,sensor2,2017/10/24 19:30:00,818182346",
    "device3,sensor1,2017/10/24 19:30:00,296150221",
    "device3,sensor2,2017/10/24 19:30:00,360361362",
    "device1,sensor1,2017/10/24 19:31:00,752187168",
    "device1,sensor2,2017/10/24 19:31:00,201286412",
    "device2,sensor1,2017/10/24 19:31:00,280281282",
    "device2,sensor2,2017/10/24 19:31:00,868159192",
    "device3,sensor1,2017/10/24 19:31:00,260261262",
    "device3,sensor2,2017/10/24 19:31:00,380381382",
    "device1,sensor1,2017/10/24 19:32:00,505152421",
    "device1,sensor2,2017/10/24 19:32:00,150151152",
    "device2,sensor1,2017/10/24 19:32:00,250251252",
    "device2,sensor2,2017/10/24 19:32:00,350351352",
    "device3,sensor1,2017/10/24 19:32:00,404142234",
    "device3,sensor2,2017/10/24 19:32:00,140141142",
    "deivce1,sensor1,2017/10/24 19:33:00,240241242",
    "device1,sensor2,2017/10/24 19:33:00,340341342",
    "device2,sensor1,2017/10/24 19:33:00,404142234",
    "device2,sensor2,2017/10/24 19:33:00,140141142",
    "device3,sensor1,2017/10/24 19:33:00,957190242",
    "device3,sensor2,2017/10/24 19:33:00,521216677",
    "device1,sensor1,2017/10/24 19:34:00,101112567",
    "device1,sensor2,2017/10/24 19:34:00,110111112",
    "device2,sensor1,2017/10/24 19:34:00,615126321",
    "device2,sensor2,2017/10/24 19:34:00,350351352",
    "device3,sensor1,2017/10/24 19:34:00,122618247",
    "device3,sensor2,2017/10/24 19:34:00,782148991"
  };

  public PulsarProducer() throws PulsarClientException {
    this.producer =
        client
            .newProducer(new StringSchema())
            .topic(Constant.TOPIC_NAME)
            .batcherBuilder(BatcherBuilder.KEY_BASED)
            .hashingScheme(HashingScheme.Murmur3_32Hash)
            .create();
  }

  public void produce() throws PulsarClientException {
    for (String s : ALL_DATA) {
      String[] line = s.split(",");
      producer.newMessage().key(line[0]).value(s).send();
    }
  }

  public static void main(String[] args) throws PulsarClientException {
    PulsarProducer pulsarProducer = new PulsarProducer();
    pulsarProducer.produce();
  }
}
