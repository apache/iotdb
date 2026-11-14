/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iotdb.db.protocol.rest.handler;

import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.rpc.TSStatusCode;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.antlr.v4.runtime.misc.ParseCancellationException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

@RunWith(Parameterized.class)
public class ExceptionHandlerTest {
  @Parameterized.Parameters(name = "REST v{0}")
  public static Collection<Object[]> versions() {
    return Arrays.asList(new Object[][] {{1}, {2}});
  }

  private final int version;

  public ExceptionHandlerTest(int version) {
    this.version = version;
  }

  @Test
  public void testParseAndSemanticErrorsDoNotLogStackTraces() {
    assertException(
        new ParseCancellationException("invalid SQL"), TSStatusCode.SQL_PARSE_ERROR, Level.INFO);
    assertException(
        new SemanticException("invalid query"), TSStatusCode.SEMANTIC_ERROR, Level.INFO);
  }

  @Test
  public void testExpectedStatusCodesDoNotLogStackTraces() {
    for (TSStatusCode code :
        new TSStatusCode[] {
          TSStatusCode.SQL_PARSE_ERROR,
          TSStatusCode.SEMANTIC_ERROR,
          TSStatusCode.NO_PERMISSION,
          TSStatusCode.ILLEGAL_PATH,
          TSStatusCode.NUMERIC_VALUE_OUT_OF_RANGE,
          TSStatusCode.DIVISION_BY_ZERO,
          TSStatusCode.DATE_OUT_OF_RANGE,
          TSStatusCode.UDF_LOAD_CLASS_ERROR,
          TSStatusCode.PLAN_FAILED_NETWORK_PARTITION,
          TSStatusCode.SYNC_CONNECTION_ERROR,
          TSStatusCode.CANNOT_FETCH_FI_STATE,
          TSStatusCode.TEMPLATE_INCOMPATIBLE,
          TSStatusCode.PATH_ALREADY_EXIST,
          TSStatusCode.PIPE_NOT_EXIST_ERROR,
          TSStatusCode.QUERY_TIMEOUT
        }) {
      assertException(
          new IoTDBException("expected failure", code.getStatusCode()), code, Level.INFO);
    }
  }

  @Test
  public void testUnexpectedErrorsKeepStackTraces() {
    assertException(
        new NullPointerException("unexpected failure"),
        TSStatusCode.INTERNAL_SERVER_ERROR,
        Level.WARN);
    assertException(
        new IoTDBException("query failure", TSStatusCode.QUERY_PROCESS_ERROR.getStatusCode()),
        TSStatusCode.QUERY_PROCESS_ERROR,
        Level.WARN);
  }

  @Test
  public void testStorageEngineNotReadyIsNotLogged() {
    assertException(
        new IoTDBException("not ready", TSStatusCode.STORAGE_ENGINE_NOT_READY.getStatusCode()),
        TSStatusCode.STORAGE_ENGINE_NOT_READY,
        null);
  }

  private void assertException(Exception exception, TSStatusCode code, Level expectedLevel) {
    Logger logger =
        (Logger)
            LoggerFactory.getLogger(
                version == 1
                    ? org.apache.iotdb.db.protocol.rest.v1.handler.ExceptionHandler.class
                    : org.apache.iotdb.db.protocol.rest.v2.handler.ExceptionHandler.class);
    Level originalLevel = logger.getLevel();
    ListAppender<ILoggingEvent> appender = new ListAppender<>();
    appender.start();
    logger.addAppender(appender);
    logger.setLevel(Level.INFO);
    try {
      if (version == 1) {
        org.apache.iotdb.db.protocol.rest.v1.model.ExecutionStatus response =
            org.apache.iotdb.db.protocol.rest.v1.handler.ExceptionHandler.tryCatchException(
                exception);
        assertEquals(code.getStatusCode(), response.getCode().intValue());
        assertEquals(exception.getMessage(), response.getMessage());
      } else {
        org.apache.iotdb.db.protocol.rest.model.ExecutionStatus response =
            org.apache.iotdb.db.protocol.rest.v2.handler.ExceptionHandler.tryCatchException(
                exception);
        assertEquals(code.getStatusCode(), response.getCode().intValue());
        assertEquals(exception.getMessage(), response.getMessage());
      }
      assertEquals(expectedLevel == null ? 0 : 1, appender.list.size());
      if (expectedLevel != null) {
        ILoggingEvent event = appender.list.get(0);
        assertEquals(expectedLevel, event.getLevel());
        assertEquals(exception.getMessage(), event.getFormattedMessage());
        if (expectedLevel == Level.INFO) {
          assertNull(event.getThrowableProxy());
        } else {
          assertNotNull(event.getThrowableProxy());
          assertEquals(exception.getClass().getName(), event.getThrowableProxy().getClassName());
        }
      }
    } finally {
      logger.detachAppender(appender);
      logger.setLevel(originalLevel);
      appender.stop();
    }
  }
}
