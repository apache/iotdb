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

package org.apache.iotdb.session.subscription.consumer.base;

import org.apache.iotdb.rpc.subscription.config.ConsumerConstant;
import org.apache.iotdb.session.subscription.SubscriptionSessionWrapper;
import org.apache.iotdb.session.subscription.SubscriptionTableSessionBuilder;
import org.apache.iotdb.session.subscription.consumer.table.SubscriptionTablePullConsumerBuilder;
import org.apache.iotdb.session.subscription.consumer.tree.SubscriptionTreePullConsumer;

import org.junit.Test;
import org.powermock.reflect.Whitebox;

import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class SubscriptionSslConfigTest {

  @Test
  public void testConsumerBuilderPropagatesSslConfigToProvider() {
    final SubscriptionTablePullConsumerBuilder builder =
        new SubscriptionTablePullConsumerBuilder()
            .host("127.0.0.1")
            .port(6667)
            .username("root")
            .password("root")
            .consumerId("consumer")
            .consumerGroupId("group")
            .useSSL(true)
            .trustStore("trust-store")
            .trustStorePwd("trust-password")
            .keyStore("key-store")
            .keyStorePwd("key-password")
            .sslProtocol("TLSv1.2");
    final AbstractSubscriptionConsumer consumer = (AbstractSubscriptionConsumer) builder.build();
    final AbstractSubscriptionProvider provider = mock(AbstractSubscriptionProvider.class);

    consumer.configureProvider(provider);

    verify(provider)
        .setSslConfig(
            true, "trust-store", "trust-password", "key-store", "key-password", "TLSv1.2");
  }

  @Test
  public void testConsumerPropertiesPopulateSslConfig() {
    final Properties properties = new Properties();
    properties.put(ConsumerConstant.HOST_KEY, "127.0.0.1");
    properties.put(ConsumerConstant.PORT_KEY, 6667);
    properties.put(ConsumerConstant.USERNAME_KEY, "root");
    properties.put(ConsumerConstant.PASSWORD_KEY, "root");
    properties.put(ConsumerConstant.CONSUMER_ID_KEY, "consumer");
    properties.put(ConsumerConstant.CONSUMER_GROUP_ID_KEY, "group");
    properties.put(ConsumerConstant.USE_SSL_KEY, "true");
    properties.put(ConsumerConstant.TRUST_STORE_KEY, "trust-store");
    properties.put(ConsumerConstant.TRUST_STORE_PWD_KEY, "trust-password");
    properties.put(ConsumerConstant.KEY_STORE_KEY, "key-store");
    properties.put(ConsumerConstant.KEY_STORE_PWD_KEY, "key-password");
    properties.put(ConsumerConstant.SSL_PROTOCOL_KEY, "TLSv1.2");

    final AbstractSubscriptionConsumer consumer =
        (AbstractSubscriptionConsumer) new SubscriptionTreePullConsumer(properties);

    assertTrue((Boolean) Whitebox.getInternalState(consumer, "useSSL"));
    assertEquals("trust-store", Whitebox.getInternalState(consumer, "trustStore"));
    assertEquals("trust-password", Whitebox.getInternalState(consumer, "trustStorePwd"));
    assertEquals("key-store", Whitebox.getInternalState(consumer, "keyStore"));
    assertEquals("key-password", Whitebox.getInternalState(consumer, "keyStorePwd"));
    assertEquals("TLSv1.2", Whitebox.getInternalState(consumer, "sslProtocol"));
  }

  @Test
  public void testSubscriptionSessionWrapperRetainsSslConfigBeforeOpen() {
    final SubscriptionTableSessionBuilder builder =
        new SubscriptionTableSessionBuilder()
            .host("127.0.0.1")
            .port(6667)
            .username("root")
            .password("root");
    final SubscriptionSessionWrapper session = new SubscriptionSessionWrapper(builder);

    session.setSslConfig(
        true, "trust-store", "trust-password", "key-store", "key-password", "TLSv1.2");

    assertTrue((Boolean) Whitebox.getInternalState(session, "useSSL"));
    assertEquals("trust-store", Whitebox.getInternalState(session, "trustStore"));
    assertEquals("trust-password", Whitebox.getInternalState(session, "trustStorePwd"));
    assertEquals("key-store", Whitebox.getInternalState(session, "keyStore"));
    assertEquals("key-password", Whitebox.getInternalState(session, "keyStorePwd"));
    assertEquals("TLSv1.2", Whitebox.getInternalState(session, "sslProtocol"));
  }
}
