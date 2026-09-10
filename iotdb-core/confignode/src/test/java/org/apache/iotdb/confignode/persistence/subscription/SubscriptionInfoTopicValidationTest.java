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

import org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant;
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
    final Map<String, String> attributes = newIncrementalTableTopicAttributes();
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
    final Map<String, String> attributes = newInitialTableTopicAttributes();
    attributes.put("Column-Filter", "column_name = \"id1\"");

    Assert.assertTrue(
        subscriptionInfo.validateBeforeCreatingTopic(
            new TCreateTopicReq("table_topic").setTopicAttributes(attributes)));
  }

  @Test
  public void testRejectDuplicateColumnFilterKeys() {
    final SubscriptionInfo subscriptionInfo = new SubscriptionInfo();
    final Map<String, String> attributes = newInitialTableTopicAttributes();
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
  public void testRejectTopicThatOnlySelectsAuditDatabase() {
    final SubscriptionInfo subscriptionInfo = new SubscriptionInfo();

    final Map<String, String> tableAttributes = newInitialTableTopicAttributes();
    tableAttributes.put(TopicConstant.DATABASE_KEY, "__audit");
    assertCreateRejected(subscriptionInfo, tableAttributes, "only to the __audit database");

    tableAttributes.put(TopicConstant.DATABASE_KEY, "__AUDIT");
    assertCreateRejected(subscriptionInfo, tableAttributes, "only to the __audit database");

    tableAttributes.put(TopicConstant.DATABASE_KEY, "__audit_data");
    assertCreateAccepted(subscriptionInfo, "table_topic", tableAttributes);

    tableAttributes.put(TopicConstant.DATABASE_KEY, "__audit|user_db");
    assertCreateAccepted(subscriptionInfo, "table_regex_topic", tableAttributes);

    tableAttributes.put(TopicConstant.DATABASE_KEY, "__audit");
    tableAttributes.put(PipeSourceConstant.SOURCE_DATABASE_NAME_KEY, "user_db");
    assertCreateAccepted(subscriptionInfo, "table_source_override_topic", tableAttributes);

    tableAttributes.put(TopicConstant.DATABASE_KEY, "user_db");
    tableAttributes.put(PipeSourceConstant.SOURCE_DATABASE_NAME_KEY, "__audit");
    assertCreateRejected(subscriptionInfo, tableAttributes, "only to the __audit database");

    final Map<String, String> treeAttributes = new HashMap<>();
    treeAttributes.put(TopicConstant.PATH_KEY, "root.__audit.**");
    assertCreateRejected(subscriptionInfo, treeAttributes, "only to the __audit database");

    treeAttributes.put(TopicConstant.PATH_KEY, "root.__audit.log.device");
    assertCreateRejected(subscriptionInfo, treeAttributes, "only to the __audit database");

    treeAttributes.put(TopicConstant.PATH_KEY, "root.__audit.log.**,root.__audit.user.**");
    assertCreateRejected(subscriptionInfo, treeAttributes, "only to the __audit database");

    treeAttributes.put(TopicConstant.PATH_KEY, "root.__audit.**,root.user.**");
    assertCreateAccepted(subscriptionInfo, "tree_topic", treeAttributes);

    treeAttributes.put(TopicConstant.PATH_KEY, "root.__audit_data.**");
    assertCreateAccepted(subscriptionInfo, "tree_similar_name_topic", treeAttributes);

    treeAttributes.remove(TopicConstant.PATH_KEY);
    treeAttributes.put(TopicConstant.PATTERN_KEY, "root.__audit.log");
    assertCreateRejected(subscriptionInfo, treeAttributes, "only to the __audit database");

    treeAttributes.put(TopicConstant.PATTERN_KEY, "root.__audit");
    assertCreateAccepted(subscriptionInfo, "tree_prefix_topic", treeAttributes);
  }

  @Test
  public void testAuditOnlyValidationHandlesSourceInclusionAndExclusion() {
    final SubscriptionInfo subscriptionInfo = new SubscriptionInfo();
    final Map<String, String> attributes = new HashMap<>();
    attributes.put(TopicConstant.PATH_KEY, "root.__audit.**");
    attributes.put("source.path.exclusion", "root.__audit.internal.**");
    assertCreateRejected(subscriptionInfo, attributes, "only to the __audit database");

    attributes.put("source.path.inclusion", "root.user.**");
    assertCreateAccepted(subscriptionInfo, "tree_source_inclusion_topic", attributes);

    final Map<String, String> attributesWithFullyExcludedUserPath = new HashMap<>();
    attributesWithFullyExcludedUserPath.put(TopicConstant.PATH_KEY, "root.__audit.**,root.user.**");
    attributesWithFullyExcludedUserPath.put("source.path.exclusion", "root.user.**");
    assertCreateRejected(
        subscriptionInfo, attributesWithFullyExcludedUserPath, "only to the __audit database");
  }

  @Test
  public void testRejectAlteringTopicToOnlySelectAuditDatabase() throws Exception {
    final SubscriptionInfo subscriptionInfo = new SubscriptionInfo();
    final Map<String, String> originalAttributes = new HashMap<>();
    originalAttributes.put(TopicConstant.PATH_KEY, "root.user.**");
    subscriptionInfo.createTopic(
        new CreateTopicPlan(new TopicMeta("tree_topic", 1L, originalAttributes)));

    final Map<String, String> updatedAttributes = new HashMap<>();
    updatedAttributes.put(TopicConstant.PATH_KEY, "root.__audit.log.**");
    try {
      subscriptionInfo.validateBeforeAlteringTopic(
          new TopicMeta("tree_topic", 2L, updatedAttributes));
      Assert.fail("Expected audit-only topic validation to fail");
    } catch (final SubscriptionException e) {
      Assert.assertTrue(e.getMessage().contains("only to the __audit database"));
    }
  }

  @Test
  public void testRejectDuplicateTopicConfigKeys() {
    final SubscriptionInfo subscriptionInfo = new SubscriptionInfo();
    final Map<String, String> attributes = newInitialTableTopicAttributes();
    attributes.put("Mode", TopicConstant.MODE_SNAPSHOT_VALUE);

    assertCreateRejected(subscriptionInfo, attributes, "duplicate mode");
  }

  @Test
  public void testAcceptColumnFilterOnInitialTsFileTableTopic() throws Exception {
    final SubscriptionInfo subscriptionInfo = new SubscriptionInfo();
    final Map<String, String> attributes = newInitialTableTopicAttributes();
    attributes.put(TopicConstant.FORMAT_KEY, TopicConstant.FORMAT_TS_FILE_VALUE);
    attributes.put(TopicConstant.COLUMN_FILTER_KEY, "column_name = \"id1\"");

    Assert.assertTrue(
        subscriptionInfo.validateBeforeCreatingTopic(
            new TCreateTopicReq("table_topic").setTopicAttributes(attributes)));
  }

  @Test
  public void testRejectLegacyTsFileAliasOnIncrementalTopic() {
    final SubscriptionInfo subscriptionInfo = new SubscriptionInfo();
    final Map<String, String> attributes = newIncrementalTableTopicAttributes();
    attributes.put(TopicConstant.FORMAT_KEY, "TsFileHandler");

    assertCreateRejected(subscriptionInfo, attributes, "mode=incremental only supports format");
  }

  @Test
  public void testRejectUnsupportedAttributesOnIncrementalTopic() {
    final SubscriptionInfo subscriptionInfo = new SubscriptionInfo();
    final Map<String, String> attributes = newIncrementalTableTopicAttributes();
    attributes.put(TopicConstant.START_TIME_KEY, "0");
    attributes.put(TopicConstant.STRICT_KEY, "false");
    attributes.put("processor", "custom-processor");

    assertCreateRejected(
        subscriptionInfo,
        attributes,
        "mode=incremental does not support topic attributes [processor, start-time, strict]");
  }

  @Test
  public void testRejectUnknownAttributeOnIncrementalTopic() {
    final SubscriptionInfo subscriptionInfo = new SubscriptionInfo();
    final Map<String, String> attributes = newIncrementalTableTopicAttributes();
    attributes.put("unknown-attribute", "value");

    assertCreateRejected(
        subscriptionInfo,
        attributes,
        "mode=incremental does not support topic attributes [unknown-attribute]");
  }

  @Test
  public void testAllowPipeAttributesOnInitialTopic() throws Exception {
    final SubscriptionInfo subscriptionInfo = new SubscriptionInfo();
    final Map<String, String> attributes = newInitialTableTopicAttributes();
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
    final Map<String, String> attributes = newIncrementalTableTopicAttributes();
    attributes.put(TopicConstant.COLUMN_FILTER_KEY, " ");

    assertCreateRejected(subscriptionInfo, attributes, "column-filter should not be empty");
  }

  @Test
  public void testAcceptAlteringColumnFilter() throws Exception {
    final SubscriptionInfo subscriptionInfo = new SubscriptionInfo();
    final Map<String, String> originalAttributes = newIncrementalTableTopicAttributes();
    originalAttributes.put(TopicConstant.COLUMN_FILTER_KEY, "column_name = \"id1\"");
    subscriptionInfo.createTopic(
        new CreateTopicPlan(new TopicMeta("table_topic", 1L, originalAttributes)));

    final Map<String, String> updatedAttributes = newIncrementalTableTopicAttributes();
    updatedAttributes.put(TopicConstant.COLUMN_FILTER_KEY, "column_name = \"m1\"");

    subscriptionInfo.validateBeforeAlteringTopic(
        new TopicMeta("table_topic", 2L, updatedAttributes));
  }

  @Test
  public void testValidateRetentionConfigOnCreate() throws Exception {
    final SubscriptionInfo subscriptionInfo = new SubscriptionInfo();
    final Map<String, String> attributes = newIncrementalTableTopicAttributes();
    attributes.put(TopicConstant.RETENTION_BYTES_KEY, "1048576");
    attributes.put(TopicConstant.RETENTION_MS_KEY, "-1");

    Assert.assertTrue(
        subscriptionInfo.validateBeforeCreatingTopic(
            new TCreateTopicReq("table_topic").setTopicAttributes(attributes)));
  }

  @Test
  public void testRejectRetentionOnTsFileTopic() {
    final SubscriptionInfo subscriptionInfo = new SubscriptionInfo();
    final Map<String, String> attributes = newIncrementalTableTopicAttributes();
    attributes.put(TopicConstant.FORMAT_KEY, TopicConstant.FORMAT_TS_FILE_VALUE);
    attributes.put(TopicConstant.RETENTION_BYTES_KEY, "1024");

    assertCreateRejected(subscriptionInfo, attributes, "mode=incremental only supports format");
  }

  @Test
  public void testRejectIllegalRetentionValue() {
    final SubscriptionInfo subscriptionInfo = new SubscriptionInfo();
    final Map<String, String> attributes = newIncrementalTableTopicAttributes();
    attributes.put(TopicConstant.RETENTION_BYTES_KEY, "0");

    assertCreateRejected(subscriptionInfo, attributes, "expected -1 or a positive long value");
  }

  @Test
  public void testRejectIllegalRetentionFormat() {
    final SubscriptionInfo subscriptionInfo = new SubscriptionInfo();
    final Map<String, String> attributes = newIncrementalTableTopicAttributes();
    attributes.put(TopicConstant.RETENTION_MS_KEY, "1h");

    assertCreateRejected(subscriptionInfo, attributes, "expected a long value");
  }

  @Test
  public void testRejectAlteringRetentionConfig() throws Exception {
    final SubscriptionInfo subscriptionInfo = new SubscriptionInfo();
    final Map<String, String> originalAttributes = newIncrementalTableTopicAttributes();
    originalAttributes.put(TopicConstant.RETENTION_BYTES_KEY, "1024");
    subscriptionInfo.createTopic(
        new CreateTopicPlan(new TopicMeta("table_topic", 1L, originalAttributes)));

    final Map<String, String> updatedAttributes = newIncrementalTableTopicAttributes();
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

  @SuppressWarnings("deprecation")
  @Test
  public void testAcceptLegacyModeValues() throws Exception {
    final SubscriptionInfo subscriptionInfo = new SubscriptionInfo();

    final Map<String, String> liveAttributes = newInitialTableTopicAttributes();
    liveAttributes.put(TopicConstant.MODE_KEY, TopicConstant.MODE_LIVE_VALUE);
    Assert.assertTrue(
        subscriptionInfo.validateBeforeCreatingTopic(
            new TCreateTopicReq("live_topic").setTopicAttributes(liveAttributes)));

    final Map<String, String> consensusAttributes = newIncrementalTableTopicAttributes();
    consensusAttributes.put(TopicConstant.MODE_KEY, TopicConstant.MODE_CONSENSUS_VALUE);
    Assert.assertTrue(
        subscriptionInfo.validateBeforeCreatingTopic(
            new TCreateTopicReq("consensus_topic").setTopicAttributes(consensusAttributes)));
  }

  @SuppressWarnings("deprecation")
  @Test
  public void testAllowAlteringModeFromLegacyAliasToCanonicalValue() throws Exception {
    final SubscriptionInfo subscriptionInfo = new SubscriptionInfo();
    final Map<String, String> originalAttributes = newInitialTableTopicAttributes();
    originalAttributes.put(TopicConstant.MODE_KEY, TopicConstant.MODE_LIVE_VALUE);
    subscriptionInfo.createTopic(
        new CreateTopicPlan(new TopicMeta("table_topic", 1L, originalAttributes)));

    subscriptionInfo.validateBeforeAlteringTopic(
        new TopicMeta("table_topic", 2L, newInitialTableTopicAttributes()));
  }

  @Test
  public void testAcceptColumnFilterOnInitialTableTopic() throws Exception {
    final SubscriptionInfo subscriptionInfo = new SubscriptionInfo();
    final Map<String, String> attributes = newInitialTableTopicAttributes();
    attributes.put(TopicConstant.COLUMN_FILTER_KEY, "column_name = \"id1\"");

    Assert.assertTrue(
        subscriptionInfo.validateBeforeCreatingTopic(
            new TCreateTopicReq("table_topic").setTopicAttributes(attributes)));
  }

  @Test
  public void testRejectIncrementalOnlyRetentionOnInitialTopic() {
    final SubscriptionInfo subscriptionInfo = new SubscriptionInfo();
    final Map<String, String> attributes = newInitialTableTopicAttributes();
    attributes.put(TopicConstant.RETENTION_BYTES_KEY, "1024");

    assertCreateRejected(subscriptionInfo, attributes, "only supported for incremental topics");
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

  private static Map<String, String> newIncrementalTableTopicAttributes() {
    final Map<String, String> attributes = new HashMap<>();
    attributes.put(SystemConstant.SQL_DIALECT_KEY, SystemConstant.SQL_DIALECT_TABLE_VALUE);
    attributes.put(TopicConstant.MODE_KEY, TopicConstant.MODE_INCREMENTAL_VALUE);
    attributes.put(TopicConstant.FORMAT_KEY, TopicConstant.FORMAT_RECORD_HANDLER_VALUE);
    return attributes;
  }

  private static Map<String, String> newInitialTableTopicAttributes() {
    final Map<String, String> attributes = new HashMap<>();
    attributes.put(SystemConstant.SQL_DIALECT_KEY, SystemConstant.SQL_DIALECT_TABLE_VALUE);
    attributes.put(TopicConstant.MODE_KEY, TopicConstant.MODE_INITIAL_VALUE);
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

  private static void assertCreateAccepted(
      final SubscriptionInfo subscriptionInfo,
      final String topicName,
      final Map<String, String> attributes) {
    try {
      Assert.assertTrue(
          subscriptionInfo.validateBeforeCreatingTopic(
              new TCreateTopicReq(topicName).setTopicAttributes(attributes)));
    } catch (final SubscriptionException e) {
      Assert.fail(e.getMessage());
    }
  }
}
