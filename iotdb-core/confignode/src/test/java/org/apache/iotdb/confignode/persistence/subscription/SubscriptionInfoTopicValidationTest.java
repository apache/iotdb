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

package org.apache.iotdb.confignode.persistence.subscription;

import org.apache.iotdb.commons.pipe.config.constant.SystemConstant;
import org.apache.iotdb.commons.subscription.meta.topic.TopicMeta;
import org.apache.iotdb.confignode.consensus.request.write.subscription.topic.CreateTopicPlan;
import org.apache.iotdb.confignode.rpc.thrift.TCreateTopicReq;
import org.apache.iotdb.rpc.subscription.config.TopicConstant;
import org.apache.iotdb.rpc.subscription.exception.SubscriptionException;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class SubscriptionInfoTopicValidationTest {

  @Test
  public void testValidateConsensusTableColumnPatternOnCreate() throws Exception {
    final SubscriptionInfo subscriptionInfo = new SubscriptionInfo();
    final Map<String, String> attributes = newLiveTableTopicAttributes();
    attributes.put(TopicConstant.COLUMN_KEY, "(id1|m1)");

    Assert.assertTrue(
        subscriptionInfo.validateBeforeCreatingTopic(
            new TCreateTopicReq("table_topic").setTopicAttributes(attributes)));
  }

  @Test
  public void testRejectColumnPatternOnTreeTopic() {
    final SubscriptionInfo subscriptionInfo = new SubscriptionInfo();
    final Map<String, String> attributes = new HashMap<>();
    attributes.put(TopicConstant.COLUMN_KEY, "id1");

    assertCreateRejected(subscriptionInfo, attributes, "only supported for table topics");
  }

  @Test
  public void testRejectColumnPatternOnTsFileTopic() {
    final SubscriptionInfo subscriptionInfo = new SubscriptionInfo();
    final Map<String, String> attributes = newLiveTableTopicAttributes();
    attributes.put(TopicConstant.FORMAT_KEY, TopicConstant.FORMAT_TS_FILE_VALUE);
    attributes.put(TopicConstant.COLUMN_KEY, "id1");

    assertCreateRejected(
        subscriptionInfo, attributes, "only supported for consensus-based live table topics");
  }

  @Test
  public void testRejectIllegalColumnRegex() {
    final SubscriptionInfo subscriptionInfo = new SubscriptionInfo();
    final Map<String, String> attributes = newLiveTableTopicAttributes();
    attributes.put(TopicConstant.COLUMN_KEY, "[");

    assertCreateRejected(subscriptionInfo, attributes, "illegal column");
  }

  @Test
  public void testRejectAlteringColumnPattern() throws Exception {
    final SubscriptionInfo subscriptionInfo = new SubscriptionInfo();
    final Map<String, String> originalAttributes = newLiveTableTopicAttributes();
    originalAttributes.put(TopicConstant.COLUMN_KEY, "id1");
    subscriptionInfo.createTopic(
        new CreateTopicPlan(new TopicMeta("table_topic", 1L, originalAttributes)));

    final Map<String, String> updatedAttributes = newLiveTableTopicAttributes();
    updatedAttributes.put(TopicConstant.COLUMN_KEY, "m1");

    try {
      subscriptionInfo.validateBeforeAlteringTopic(
          new TopicMeta("table_topic", 2L, updatedAttributes));
      Assert.fail("Expected altering the column pattern to be rejected");
    } catch (final SubscriptionException e) {
      Assert.assertTrue(e.getMessage().contains("changing column is not supported"));
    }
  }

  @Test
  public void testValidateRetentionConfigOnCreate() throws Exception {
    final SubscriptionInfo subscriptionInfo = new SubscriptionInfo();
    final Map<String, String> attributes = newLiveTableTopicAttributes();
    attributes.put(TopicConstant.RETENTION_BYTES_KEY, "1048576");
    attributes.put(TopicConstant.RETENTION_MS_KEY, "-1");

    Assert.assertTrue(
        subscriptionInfo.validateBeforeCreatingTopic(
            new TCreateTopicReq("table_topic").setTopicAttributes(attributes)));
  }

  @Test
  public void testRejectRetentionOnTsFileTopic() {
    final SubscriptionInfo subscriptionInfo = new SubscriptionInfo();
    final Map<String, String> attributes = newLiveTableTopicAttributes();
    attributes.put(TopicConstant.FORMAT_KEY, TopicConstant.FORMAT_TS_FILE_VALUE);
    attributes.put(TopicConstant.RETENTION_BYTES_KEY, "1024");

    assertCreateRejected(
        subscriptionInfo,
        attributes,
        "retention.bytes and retention.ms are only supported for consensus-based live topics");
  }

  @Test
  public void testRejectIllegalRetentionValue() {
    final SubscriptionInfo subscriptionInfo = new SubscriptionInfo();
    final Map<String, String> attributes = newLiveTableTopicAttributes();
    attributes.put(TopicConstant.RETENTION_BYTES_KEY, "0");

    assertCreateRejected(subscriptionInfo, attributes, "expected -1 or a positive long value");
  }

  @Test
  public void testRejectIllegalRetentionFormat() {
    final SubscriptionInfo subscriptionInfo = new SubscriptionInfo();
    final Map<String, String> attributes = newLiveTableTopicAttributes();
    attributes.put(TopicConstant.RETENTION_MS_KEY, "1h");

    assertCreateRejected(subscriptionInfo, attributes, "expected a long value");
  }

  @Test
  public void testRejectAlteringRetentionConfig() throws Exception {
    final SubscriptionInfo subscriptionInfo = new SubscriptionInfo();
    final Map<String, String> originalAttributes = newLiveTableTopicAttributes();
    originalAttributes.put(TopicConstant.RETENTION_BYTES_KEY, "1024");
    subscriptionInfo.createTopic(
        new CreateTopicPlan(new TopicMeta("table_topic", 1L, originalAttributes)));

    final Map<String, String> updatedAttributes = newLiveTableTopicAttributes();
    updatedAttributes.put(TopicConstant.RETENTION_BYTES_KEY, "2048");

    try {
      subscriptionInfo.validateBeforeAlteringTopic(
          new TopicMeta("table_topic", 2L, updatedAttributes));
      Assert.fail("Expected altering retention.bytes to be rejected");
    } catch (final SubscriptionException e) {
      Assert.assertTrue(e.getMessage().contains("changing retention.bytes is not supported"));
    }
  }

  private static Map<String, String> newLiveTableTopicAttributes() {
    final Map<String, String> attributes = new HashMap<>();
    attributes.put(SystemConstant.SQL_DIALECT_KEY, SystemConstant.SQL_DIALECT_TABLE_VALUE);
    attributes.put(TopicConstant.MODE_KEY, TopicConstant.MODE_LIVE_VALUE);
    attributes.put(TopicConstant.FORMAT_KEY, TopicConstant.FORMAT_RECORD_HANDLER_VALUE);
    return attributes;
  }

  private static void assertCreateRejected(
      final SubscriptionInfo subscriptionInfo,
      final Map<String, String> attributes,
      final String expectedMessagePart) {
    try {
      subscriptionInfo.validateBeforeCreatingTopic(
          new TCreateTopicReq("table_topic").setTopicAttributes(attributes));
      Assert.fail("Expected topic validation to fail");
    } catch (final SubscriptionException e) {
      Assert.assertTrue(e.getMessage().contains(expectedMessagePart));
    }
  }
}
