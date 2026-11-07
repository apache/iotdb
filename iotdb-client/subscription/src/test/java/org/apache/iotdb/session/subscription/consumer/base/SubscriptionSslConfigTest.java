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

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.rpc.subscription.config.ConsumerConstant;
import org.apache.iotdb.session.AbstractSessionBuilder;
import org.apache.iotdb.session.subscription.SubscriptionTreeSessionBuilder;
import org.apache.iotdb.session.subscription.consumer.table.SubscriptionTablePullConsumerBuilder;
import org.apache.iotdb.session.subscription.consumer.tree.SubscriptionTreePullConsumer;

import org.junit.Test;

import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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
    final RecordingProvider provider = new RecordingProvider();

    consumer.configureProvider(provider);

    assertSslConfig(provider);
  }

  @Test
  public void testConsumerPropertiesPropagateSslConfigToProvider() {
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
    final RecordingProvider provider = new RecordingProvider();

    consumer.configureProvider(provider);

    assertSslConfig(provider);
  }

  private static void assertSslConfig(final RecordingProvider provider) {
    assertTrue(provider.useSSL);
    assertEquals("trust-store", provider.trustStore);
    assertEquals("trust-password", provider.trustStorePwd);
    assertEquals("key-store", provider.keyStore);
    assertEquals("key-password", provider.keyStorePwd);
    assertEquals("TLSv1.2", provider.sslProtocol);
  }

  private static final class RecordingProvider extends AbstractSubscriptionProvider {

    private boolean useSSL;
    private String trustStore;
    private String trustStorePwd;
    private String keyStore;
    private String keyStorePwd;
    private String sslProtocol;

    private RecordingProvider() {
      super(
          new TEndPoint("127.0.0.1", 6667),
          "root",
          "root",
          null,
          "consumer",
          "group",
          null,
          null,
          1024 * 1024,
          1000,
          1000);
    }

    @Override
    protected AbstractSessionBuilder constructSubscriptionSessionBuilder(
        final String host,
        final int port,
        final String username,
        final String password,
        final String encryptedPassword,
        final int thriftMaxFrameSize,
        final int connectionTimeoutInMs) {
      return new SubscriptionTreeSessionBuilder()
          .host(host)
          .port(port)
          .username(username)
          .password(password)
          .thriftMaxFrameSize(thriftMaxFrameSize)
          .connectionTimeoutInMs(connectionTimeoutInMs);
    }

    @Override
    void setSslConfig(
        final boolean useSSL,
        final String trustStore,
        final String trustStorePwd,
        final String keyStore,
        final String keyStorePwd,
        final String sslProtocol) {
      this.useSSL = useSSL;
      this.trustStore = trustStore;
      this.trustStorePwd = trustStorePwd;
      this.keyStore = keyStore;
      this.keyStorePwd = keyStorePwd;
      this.sslProtocol = sslProtocol;
    }

    @Override
    void setConsumerInstanceId(final String consumerInstanceId) {}
  }
}
