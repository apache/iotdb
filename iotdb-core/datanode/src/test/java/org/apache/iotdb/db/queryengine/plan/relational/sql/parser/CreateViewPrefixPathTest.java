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

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Statement;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.CreateView;

import org.junit.Test;

import java.time.ZoneId;

import static org.junit.Assert.assertEquals;

public class CreateViewPrefixPathTest {

  @Test
  public void testQuotedTreePathSegment() throws Exception {
    final Statement statement =
        new SqlParser()
            .createStatement(
                "CREATE VIEW tv143_cmp_3.v (device STRING TAG, value INT32 FIELD FROM s1) "
                    + "WITH (ttl='INF') AS root.\"`tv143_cmp_3`\".\"`a b`\".**",
                ZoneId.systemDefault(),
                null);

    assertEquals(
        new PartialPath("root.tv143_cmp_3.`a b`.**"), ((CreateView) statement).getPrefixPath());
  }
}
