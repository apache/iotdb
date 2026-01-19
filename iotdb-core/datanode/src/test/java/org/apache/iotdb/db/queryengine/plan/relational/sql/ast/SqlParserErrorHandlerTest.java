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

package org.apache.iotdb.db.queryengine.plan.relational.sql.ast;

import org.apache.iotdb.db.queryengine.plan.relational.sql.parser.ParsingException;
import org.apache.iotdb.db.queryengine.plan.relational.sql.parser.SqlParser;

import org.junit.Test;

import java.time.ZoneId;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class SqlParserErrorHandlerTest {

  @Test
  public void testParseLimitOffsetError() {
    String sql = "select * from information_schema.queries offset 48 limit1";
    SqlParser sqlParser = new SqlParser();
    ExecutorService executor = Executors.newSingleThreadExecutor();
    Future<?> future =
        executor.submit(
            () -> {
              try {
                sqlParser.createStatement(sql, ZoneId.systemDefault(), null);
                fail("Expected ParsingException to be thrown");
              } catch (ParsingException e) {
                assertTrue(e.getMessage().contains("mismatched input 'limit1'"));
              }
            });

    try {
      // The parsing should fail quickly. If it hangs (OOM), this timeout will
      // trigger.
      future.get(5, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      fail("Interrupted");
    } catch (ExecutionException e) {
      // If parsing exception propagates here (which it shouldn't as it's caught in
      // the task), handle it
      if (e.getCause() instanceof ParsingException) {
        assertTrue(e.getCause().getMessage().contains("mismatched input 'limit1'"));
      } else {
        fail("Unexpected exception: " + e.getCause());
      }
    } catch (TimeoutException e) {
      fail("Parsing timed out - potential endless loop/OOM detected");
    } finally {
      executor.shutdownNow();
    }
  }
}
