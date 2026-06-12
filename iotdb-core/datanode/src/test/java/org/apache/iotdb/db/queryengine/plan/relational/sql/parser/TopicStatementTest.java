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
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.AlterTopic;

import org.junit.Assert;
import org.junit.Test;

import java.time.ZoneId;

public class TopicStatementTest {

  private final SqlParser sqlParser = new SqlParser();

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
