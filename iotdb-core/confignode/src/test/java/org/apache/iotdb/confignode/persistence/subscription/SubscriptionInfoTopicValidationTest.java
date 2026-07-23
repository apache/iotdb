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
import org.apache.iotdb.commons.subscription.config.SubscriptionConfig;
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
  public void testValidateColumnFilterOnCreate() throws Exception {
    final SubscriptionInfo subscriptionInfo = new SubscriptionInfo();
    final Map<String, String> attributes = newConsensusTableTopicAttributes();
    attributes.put(TopicConstant.COLUMN_FILTER_KEY, "column_name IN (\"id1\", \"m1\")");

    Assert.assertTrue(
        subscriptionInfo.validateBeforeCreatingTopic(
            new TCreateTopicReq("table_topic").setTopicAttributes(attributes)));
  }

  @Test
  public void testRejectColumnFilterOnTreeTopic() {
    final SubscriptionInfo subscriptionInfo = new SubscriptionInfo();
    final Map<String, String> attributes = new HashMap<>();
    attributes.put(TopicConstant.COLUMN_FILTER_KEY, "column_name = \"id1\"");

    assertCreateRejected(subscriptionInfo, attributes, "only supported for table topics");
  }

  @Test
  public void testColumnFilterKeyIsCaseInsensitiveOnCreate() throws Exception {
    final SubscriptionInfo subscriptionInfo = new SubscriptionInfo();
    final Map<String, String> attributes = newLiveTableTopicAttributes();
    attributes.put("Column-Filter", "column_name = \"id1\"");

    Assert.assertTrue(
        subscriptionInfo.validateBeforeCreatingTopic(
            new TCreateTopicReq("table_topic").setTopicAttributes(attributes)));
  }

  @Test
  public void testRejectDuplicateColumnFilterKeys() {
    final SubscriptionInfo subscriptionInfo = new SubscriptionInfo();
    final Map<String, String> attributes = newLiveTableTopicAttributes();
    attributes.put(TopicConstant.COLUMN_FILTER_KEY, "column_name = \"id1\"");
    attributes.put("Column-Filter", "column_name = \"m1\"");

    assertCreateRejected(subscriptionInfo, attributes, "duplicate column-filter");
  }

  @Test
  public void testRejectMixedCaseColumnFilterOnTreeTopic() {
    final SubscriptionInfo subscriptionInfo = new SubscriptionInfo();
    final Map<String, String> attributes = new HashMap<>();
    attributes.put("Column-Filter", "column_name = \"id1\"");

    assertCreateRejected(subscriptionInfo, attributes, "only supported for table topics");
  }

  @Test
  public void testRejectDuplicateTopicConfigKeys() {
    final SubscriptionInfo subscriptionInfo = new SubscriptionInfo();
    final Map<String, String> attributes = newLiveTableTopicAttributes();
    attributes.put("Mode", TopicConstant.MODE_SNAPSHOT_VALUE);

    assertCreateRejected(subscriptionInfo, attributes, "duplicate mode");
  }

  @Test
  public void testAcceptColumnFilterOnLiveTsFileTableTopic() throws Exception {
    final SubscriptionInfo subscriptionInfo = new SubscriptionInfo();
    final Map<String, String> attributes = newLiveTableTopicAttributes();
    attributes.put(TopicConstant.FORMAT_KEY, TopicConstant.FORMAT_TS_FILE_VALUE);
    attributes.put(TopicConstant.COLUMN_FILTER_KEY, "column_name = \"id1\"");

    Assert.assertTrue(
        subscriptionInfo.validateBeforeCreatingTopic(
            new TCreateTopicReq("table_topic").setTopicAttributes(attributes)));
  }

  @Test
  public void testRejectLegacyTsFileAliasOnConsensusTopic() {
    final SubscriptionInfo subscriptionInfo = new SubscriptionInfo();
    final Map<String, String> attributes = newConsensusTableTopicAttributes();
    attributes.put(TopicConstant.FORMAT_KEY, "TsFileHandler");

    assertCreateRejected(subscriptionInfo, attributes, "mode=consensus only supports format");
  }

  @Test
  public void testRejectUnsupportedAttributesOnConsensusTopic() {
    final SubscriptionInfo subscriptionInfo = new SubscriptionInfo();
    final Map<String, String> attributes = newConsensusTableTopicAttributes();
    attributes.put(TopicConstant.START_TIME_KEY, "0");
    attributes.put(TopicConstant.STRICT_KEY, "false");
    attributes.put("processor", "custom-processor");

    assertCreateRejected(
        subscriptionInfo,
        attributes,
        "mode=consensus does not support topic attributes [processor, start-time, strict]");
  }

  @Test
  public void testRejectUnknownAttributeOnConsensusTopic() {
    final SubscriptionInfo subscriptionInfo = new SubscriptionInfo();
    final Map<String, String> attributes = newConsensusTableTopicAttributes();
    attributes.put("unknown-attribute", "value");

    assertCreateRejected(
        subscriptionInfo,
        attributes,
        "mode=consensus does not support topic attributes [unknown-attribute]");
  }

  @Test
  public void testAllowPipeAttributesOnLiveTopic() throws Exception {
    final SubscriptionInfo subscriptionInfo = new SubscriptionInfo();
    final Map<String, String> attributes = newLiveTableTopicAttributes();
    attributes.put(TopicConstant.START_TIME_KEY, "0");
    attributes.put(TopicConstant.STRICT_KEY, "false");
    attributes.put("processor", "custom-processor");

    Assert.assertTrue(
        subscriptionInfo.validateBeforeCreatingTopic(
            new TCreateTopicReq("table_topic").setTopicAttributes(attributes)));
  }

  @Test
  public void testRejectEmptyColumnFilter() {
    final SubscriptionInfo subscriptionInfo = new SubscriptionInfo();
    final Map<String, String> attributes = newConsensusTableTopicAttributes();
    attributes.put(TopicConstant.COLUMN_FILTER_KEY, " ");

    assertCreateRejected(subscriptionInfo, attributes, "column-filter should not be empty");
  }

  @Test
  public void testAcceptAlteringColumnFilter() throws Exception {
    final SubscriptionInfo subscriptionInfo = new SubscriptionInfo();
    final Map<String, String> originalAttributes = newConsensusTableTopicAttributes();
    originalAttributes.put(TopicConstant.COLUMN_FILTER_KEY, "column_name = \"id1\"");
    subscriptionInfo.createTopic(
        new CreateTopicPlan(new TopicMeta("table_topic", 1L, originalAttributes)));

    final Map<String, String> updatedAttributes = newConsensusTableTopicAttributes();
    updatedAttributes.put(TopicConstant.COLUMN_FILTER_KEY, "column_name = \"m1\"");

    subscriptionInfo.validateBeforeAlteringTopic(
        new TopicMeta("table_topic", 2L, updatedAttributes));
  }

  @Test
  public void testValidateRetentionConfigOnCreate() throws Exception {
    final SubscriptionInfo subscriptionInfo = new SubscriptionInfo();
    final Map<String, String> attributes = newConsensusTableTopicAttributes();
    attributes.put(TopicConstant.RETENTION_BYTES_KEY, "1048576");
    attributes.put(TopicConstant.RETENTION_MS_KEY, "-1");

    Assert.assertTrue(
        subscriptionInfo.validateBeforeCreatingTopic(
            new TCreateTopicReq("table_topic").setTopicAttributes(attributes)));
  }

  @Test
  public void testRejectRetentionOnTsFileTopic() {
    final SubscriptionInfo subscriptionInfo = new SubscriptionInfo();
    final Map<String, String> attributes = newConsensusTableTopicAttributes();
    attributes.put(TopicConstant.FORMAT_KEY, TopicConstant.FORMAT_TS_FILE_VALUE);
    attributes.put(TopicConstant.RETENTION_BYTES_KEY, "1024");

    assertCreateRejected(subscriptionInfo, attributes, "mode=consensus only supports format");
  }

  @Test
  public void testRejectIllegalRetentionValue() {
    final SubscriptionInfo subscriptionInfo = new SubscriptionInfo();
    final Map<String, String> attributes = newConsensusTableTopicAttributes();
    attributes.put(TopicConstant.RETENTION_BYTES_KEY, "0");

    assertCreateRejected(subscriptionInfo, attributes, "expected -1 or a positive long value");
  }

  @Test
  public void testRejectIllegalRetentionFormat() {
    final SubscriptionInfo subscriptionInfo = new SubscriptionInfo();
    final Map<String, String> attributes = newConsensusTableTopicAttributes();
    attributes.put(TopicConstant.RETENTION_MS_KEY, "1h");

    assertCreateRejected(subscriptionInfo, attributes, "expected a long value");
  }

  @Test
  public void testRejectAlteringRetentionConfig() throws Exception {
    final SubscriptionInfo subscriptionInfo = new SubscriptionInfo();
    final Map<String, String> originalAttributes = newConsensusTableTopicAttributes();
    originalAttributes.put(TopicConstant.RETENTION_BYTES_KEY, "1024");
    subscriptionInfo.createTopic(
        new CreateTopicPlan(new TopicMeta("table_topic", 1L, originalAttributes)));

    final Map<String, String> updatedAttributes = newConsensusTableTopicAttributes();
    updatedAttributes.put(TopicConstant.RETENTION_BYTES_KEY, "2048");

    try {
      subscriptionInfo.validateBeforeAlteringTopic(
          new TopicMeta("table_topic", 2L, updatedAttributes));
      Assert.fail("Expected altering retention.bytes to be rejected");
    } catch (final SubscriptionException e) {
      Assert.assertTrue(e.getMessage().contains("changing retention.bytes is not supported"));
    }
  }

  @Test
  public void testRejectIllegalMode() {
    final SubscriptionInfo subscriptionInfo = new SubscriptionInfo();
    final Map<String, String> attributes = new HashMap<>();
    attributes.put(TopicConstant.MODE_KEY, "wal");

    assertCreateRejected(subscriptionInfo, attributes, "unsupported mode");
  }

  @Test
  public void testAcceptColumnFilterOnLiveTableTopic() throws Exception {
    final SubscriptionInfo subscriptionInfo = new SubscriptionInfo();
    final Map<String, String> attributes = newLiveTableTopicAttributes();
    attributes.put(TopicConstant.COLUMN_FILTER_KEY, "column_name = \"id1\"");

    Assert.assertTrue(
        subscriptionInfo.validateBeforeCreatingTopic(
            new TCreateTopicReq("table_topic").setTopicAttributes(attributes)));
  }

  @Test
  public void testRejectConsensusOnlyRetentionOnLiveTopic() {
    final SubscriptionInfo subscriptionInfo = new SubscriptionInfo();
    final Map<String, String> attributes = newLiveTableTopicAttributes();
    attributes.put(TopicConstant.RETENTION_BYTES_KEY, "1024");

    assertCreateRejected(subscriptionInfo, attributes, "only supported for consensus topics");
  }

  @Test
  public void testRejectOwnerLeaseDurationBelowMin() {
    final SubscriptionInfo subscriptionInfo = new SubscriptionInfo();
    final Map<String, String> attributes = new HashMap<>();
    attributes.put(TopicConstant.OWNER_ID_KEY, "owner1");
    attributes.put(TopicConstant.OWNER_EPOCH_KEY, "1");
    // Well below the default 1-minute floor.
    attributes.put(TopicConstant.OWNER_LEASE_DURATION_MS_KEY, "5000");

    assertCreateRejected(subscriptionInfo, attributes, "below the minimum allowed");
  }

  @Test
  public void testAcceptOwnerLeaseDurationAtMin() throws Exception {
    final SubscriptionInfo subscriptionInfo = new SubscriptionInfo();
    final Map<String, String> attributes = new HashMap<>();
    attributes.put(TopicConstant.OWNER_ID_KEY, "owner1");
    attributes.put(TopicConstant.OWNER_EPOCH_KEY, "1");
    attributes.put(
        TopicConstant.OWNER_LEASE_DURATION_MS_KEY,
        String.valueOf(SubscriptionConfig.getInstance().getSubscriptionOwnerLeaseDurationMsMin()));

    Assert.assertTrue(
        subscriptionInfo.validateBeforeCreatingTopic(
            new TCreateTopicReq("owner_topic").setTopicAttributes(attributes)));
  }

  private static Map<String, String> newConsensusTableTopicAttributes() {
    final Map<String, String> attributes = new HashMap<>();
    attributes.put(SystemConstant.SQL_DIALECT_KEY, SystemConstant.SQL_DIALECT_TABLE_VALUE);
    attributes.put(TopicConstant.MODE_KEY, TopicConstant.MODE_CONSENSUS_VALUE);
    attributes.put(TopicConstant.FORMAT_KEY, TopicConstant.FORMAT_RECORD_HANDLER_VALUE);
    return attributes;
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
