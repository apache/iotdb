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

package org.apache.iotdb.db.queryengine.plan.relational.sql.parser;

import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Statement;
import org.apache.iotdb.commons.subscription.meta.topic.TopicMeta;
import org.apache.iotdb.db.queryengine.plan.execution.config.sys.subscription.ShowCreateTopicTask;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.AlterTopic;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.CreateTopic;
import org.apache.iotdb.db.queryengine.plan.relational.sql.util.DataNodeSqlFormatter;
import org.apache.iotdb.db.subscription.tagfilter.TagFilterEvaluator;
import org.apache.iotdb.db.subscription.tagfilter.TagFilterParser;

import org.junit.Assert;
import org.junit.Test;

import java.time.ZoneId;
import java.util.Map;

public class TopicStatementTest {

  private final SqlParser sqlParser = new SqlParser();

  @Test
  public void testTagFilterQuotesInCreateAndAlterTopic() throws Exception {
    final String attributes =
        "('tag-filter'='region IN (\"O''Reilly\", \"a''''b\", \"a\"\"\"\"b\", \"O''Reilly \"\"A\"\"\"\"B\"\"\")')";
    final String expectedFilter =
        "region IN (\"O'Reilly\", \"a''b\", \"a\"\"\"\"b\", \"O'Reilly \"\"A\"\"\"\"B\"\"\")";
    for (final String command :
        new String[] {
          "CREATE TOPIC topic1",
          "ALTER TOPIC topic1",
          "CREATE TOPIC IF NOT EXISTS \"topic-1\"",
          "ALTER TOPIC \"topic-1\""
        }) {
      final Statement statement =
          sqlParser.createStatement(command + " WITH " + attributes, ZoneId.systemDefault(), null);
      final Statement formattedStatement =
          sqlParser.createStatement(
              DataNodeSqlFormatter.formatDataNodeSql(statement), ZoneId.systemDefault(), null);
      Assert.assertEquals(statement, formattedStatement);
      final String filter =
          statement instanceof CreateTopic
              ? ((CreateTopic) statement).getTopicAttributes().get("tag-filter")
              : ((AlterTopic) statement).getTopicAttributes().get("tag-filter");
      Assert.assertEquals(expectedFilter, filter);

      final TagFilterParser parser = new TagFilterParser();
      for (final String value :
          new String[] {"O'Reilly", "a''b", "a\"\"b", "O'Reilly \"A\"\"B\""}) {
        Assert.assertTrue(
            command + ": " + value,
            TagFilterEvaluator.evaluate(parser.parseAndValidate(filter), identifier -> value));
      }
      Assert.assertFalse(
          TagFilterEvaluator.evaluate(parser.parseAndValidate(filter), identifier -> "a\"b"));
      Assert.assertFalse(
          TagFilterEvaluator.evaluate(parser.parseAndValidate(filter), identifier -> "O''Reilly"));
    }
  }

  @Test
  public void testShowCreateTopicPreservesTagFilterQuotes() {
    final String filter = "region = \"O'Reilly \"\"A\"\"\"\"B\"\"\"";
    final TopicMeta topicMeta = new TopicMeta("topic-1", 0L, Map.of("tag-filter", filter));
    final CreateTopic recreated =
        (CreateTopic)
            sqlParser.createStatement(
                ShowCreateTopicTask.getShowCreateTopicSQL(topicMeta), ZoneId.systemDefault(), null);
    Assert.assertEquals("topic-1", recreated.getTopicName());
    Assert.assertEquals(filter, recreated.getTopicAttributes().get("tag-filter"));
  }

  @Test
  public void testAlterTopicStatement() {
    final Statement statement =
        sqlParser.createStatement(
            "ALTER TOPIC topic1 WITH ('owner-id'='owner2','owner-epoch'='6')",
            ZoneId.systemDefault(),
            null);

    Assert.assertTrue(statement instanceof AlterTopic);
    final AlterTopic alterTopic = (AlterTopic) statement;
    Assert.assertEquals("topic1", alterTopic.getTopicName());
    Assert.assertEquals("owner2", alterTopic.getTopicAttributes().get("owner-id"));
    Assert.assertEquals("6", alterTopic.getTopicAttributes().get("owner-epoch"));
  }
}
