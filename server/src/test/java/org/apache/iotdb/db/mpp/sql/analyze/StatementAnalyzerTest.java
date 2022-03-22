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

package org.apache.iotdb.db.mpp.sql.analyze;

import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.mpp.common.MPPQueryContext;
import org.apache.iotdb.db.mpp.sql.parser.StatementGenerator;

import org.junit.Assert;
import org.junit.Test;

import java.time.ZonedDateTime;

import static org.junit.Assert.fail;

public class StatementAnalyzerTest {

  @Test
  public void samePropertyKeyTest() {
    assertAnalyzeSemanticException(
        "CREATE TIMESERIES root.sg1.d1.s1 INT32 TAGS(a=1) ATTRIBUTES(a=1)",
        "Tag and attribute shouldn't have the same property key");
  }

  private void assertAnalyzeSemanticException(String sql, String message) {
    try {
      StatementAnalyzer analyzer = new StatementAnalyzer(new Analysis(), new MPPQueryContext());
      analyzer.analyze(StatementGenerator.createStatement(sql, ZonedDateTime.now().getOffset()));
      fail();
    } catch (SemanticException e) {
      Assert.assertTrue(e.getMessage().contains(message));
    }
  }
}
