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

import org.apache.iotdb.commons.exception.SemanticException;
import org.apache.iotdb.db.protocol.session.InternalClientSession;
import org.apache.iotdb.db.queryengine.plan.parser.StatementGenerator;

import org.junit.Test;

import java.time.ZoneOffset;
import java.util.function.Consumer;

import static org.junit.Assert.assertThrows;

public class DateExpressionBoundaryTest {

  @Test
  public void testTreeDateExpressionBoundaries() {
    assertDateExpressionBoundaries(
        expression ->
            StatementGenerator.createStatement(
                "select s1 from root.sg.d1 where time > " + expression, ZoneOffset.UTC));
  }

  @Test
  public void testTableDateExpressionBoundaries() {
    SqlParser parser = new SqlParser();
    assertDateExpressionBoundaries(
        expression ->
            parser.createStatement(
                "select s1 from table1 where time > " + expression,
                ZoneOffset.UTC,
                new InternalClientSession("date_boundary")));
  }

  private void assertDateExpressionBoundaries(Consumer<String> parse) {
    parse.accept("1970-01-01T00:00:00.000 + 9223372036854775807ms");
    parse.accept("1969-12-31T23:59:59.999 - 9223372036854775807ms");
    assertThrows(
        SemanticException.class,
        () -> parse.accept("1970-01-01T00:00:00.001 + 9223372036854775807ms"));
    assertThrows(
        SemanticException.class,
        () -> parse.accept("1969-12-31T23:59:59.999 - 9223372036854775807ms - 1ms"));
  }
}
