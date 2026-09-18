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

package org.apache.iotdb.commons.pipe.datastructure;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.exception.pipe.PipeRuntimeSinkNonReportTimeConfigurableException;
import org.apache.iotdb.commons.pipe.receiver.PipeReceiverStatusHandler;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;

import java.util.Arrays;
import java.util.Collections;

import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_EXCEPTION_CONFLICT_RECORD_IGNORED_DATA_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_EXCEPTION_CONFLICT_RESOLVE_STRATEGY_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_EXCEPTION_CONFLICT_RETRY_MAX_TIME_SECONDS_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_EXCEPTION_OTHERS_RECORD_IGNORED_DATA_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_EXCEPTION_OTHERS_RETRY_MAX_TIME_SECONDS_DEFAULT_VALUE;

public class PipeReceiverStatusHandlerTest {
  @Test
  public void testNestedStatusMessageOverridesGenericExceptionMessage() {
    final PipeReceiverStatusHandler handler =
        new PipeReceiverStatusHandler(false, 60, false, 60, false, false);
    final TSStatus status =
        new TSStatus(TSStatusCode.PIPE_RECEIVER_TEMPORARY_UNAVAILABLE_EXCEPTION.getStatusCode())
            .setMessage("aggregate receiver error")
            .setSubStatus(
                Arrays.asList(
                    new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()),
                    new TSStatus(TSStatusCode.METADATA_ERROR.getStatusCode())
                        .setMessage("receiver rejected request")));

    try {
      handler.handle(status, "generic sink transfer error", "record");
      Assert.fail("Expected a retry exception");
    } catch (final PipeRuntimeSinkNonReportTimeConfigurableException e) {
      Assert.assertEquals("receiver rejected request", e.getMessage());
    }
  }

  @Test
  public void testStatusMessagePrefersNestedFailureOverOuterAggregateMessage() {
    final TSStatus status =
        new TSStatus(TSStatusCode.MULTIPLE_ERROR.getStatusCode())
            .setMessage("outer message")
            .setSubStatus(
                Arrays.asList(
                    new TSStatus(TSStatusCode.METADATA_ERROR.getStatusCode())
                        .setMessage("inner message")));

    Assert.assertEquals("inner message", PipeReceiverStatusHandler.getStatusMessage(status));
  }

  @Test
  public void testStatusMessagePrefersConcreteFailureOverClassifiedWrapper() {
    final TSStatus status =
        new TSStatus(TSStatusCode.PIPE_RECEIVER_TEMPORARY_UNAVAILABLE_EXCEPTION.getStatusCode())
            .setMessage("outer transfer failure")
            .setSubStatus(
                Arrays.asList(
                    new TSStatus(
                            TSStatusCode.PIPE_RECEIVER_TEMPORARY_UNAVAILABLE_EXCEPTION
                                .getStatusCode())
                        .setMessage("generic receiver failure"),
                    new TSStatus(TSStatusCode.METADATA_ERROR.getStatusCode())
                        .setMessage("actual metadata failure")));

    Assert.assertEquals(
        "actual metadata failure", PipeReceiverStatusHandler.getStatusMessage(status));
  }

  @Test
  public void testStatusMessageFindsDeepNestedFailure() {
    final TSStatus status =
        new TSStatus(TSStatusCode.PIPE_RECEIVER_TEMPORARY_UNAVAILABLE_EXCEPTION.getStatusCode())
            .setMessage("outer transfer failure")
            .setSubStatus(
                Collections.singletonList(
                    new TSStatus(TSStatusCode.MULTIPLE_ERROR.getStatusCode())
                        .setMessage("batch failure")
                        .setSubStatus(
                            Collections.singletonList(
                                new TSStatus(TSStatusCode.LOAD_FILE_ERROR.getStatusCode())
                                    .setMessage("receiver disk is full")))));

    Assert.assertEquals(
        "receiver disk is full", PipeReceiverStatusHandler.getStatusMessage(status));
  }

  @Test
  public void testStatusMessageUsesHighestPriorityClassifiedFailure() {
    final TSStatus status =
        new TSStatus(TSStatusCode.MULTIPLE_ERROR.getStatusCode())
            .setSubStatus(
                Arrays.asList(
                    new TSStatus(
                            TSStatusCode.PIPE_RECEIVER_IDEMPOTENT_CONFLICT_EXCEPTION
                                .getStatusCode())
                        .setMessage("idempotent conflict"),
                    new TSStatus(TSStatusCode.PIPE_RECEIVER_USER_CONFLICT_EXCEPTION.getStatusCode())
                        .setMessage("user conflict"),
                    new TSStatus(
                            TSStatusCode.PIPE_RECEIVER_TEMPORARY_UNAVAILABLE_EXCEPTION
                                .getStatusCode())
                        .setMessage("receiver unavailable")));

    Assert.assertEquals("receiver unavailable", PipeReceiverStatusHandler.getStatusMessage(status));
  }

  @Test
  public void testStatusMessagePrefersFailureOverRedirectionMessage() {
    final TSStatus status =
        new TSStatus(TSStatusCode.PIPE_RECEIVER_TEMPORARY_UNAVAILABLE_EXCEPTION.getStatusCode())
            .setSubStatus(
                Arrays.asList(
                    new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode())
                        .setRedirectNode(new TEndPoint("127.0.0.1", 6667))
                        .setMessage("root.sg.device"),
                    new TSStatus(TSStatusCode.METADATA_ERROR.getStatusCode())
                        .setMessage("receiver rejected request")));

    Assert.assertEquals(
        "receiver rejected request", PipeReceiverStatusHandler.getStatusMessage(status));
  }

  @Test
  public void testStatusMessageDoesNotUseRedirectionDevicePath() {
    final TSStatus status =
        new TSStatus(TSStatusCode.PIPE_RECEIVER_TEMPORARY_UNAVAILABLE_EXCEPTION.getStatusCode())
            .setSubStatus(
                Arrays.asList(
                    new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode())
                        .setRedirectNode(new TEndPoint("127.0.0.1", 6667))
                        .setMessage("root.sg.device")));

    Assert.assertNull(PipeReceiverStatusHandler.getStatusMessage(status));
  }

  @Test
  public void testStatusMessageIgnoresSuccessAndRedirectionMessages() {
    Assert.assertNull(
        PipeReceiverStatusHandler.getStatusMessage(
            new TSStatus(TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode())
                .setMessage("root.sg.device")));

    final TSStatus status =
        new TSStatus(TSStatusCode.PIPE_RECEIVER_TEMPORARY_UNAVAILABLE_EXCEPTION.getStatusCode())
            .setSubStatus(
                Arrays.asList(
                    new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()).setMessage("ok"),
                    new TSStatus(TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode())
                        .setMessage("root.sg.redirected")));
    Assert.assertNull(PipeReceiverStatusHandler.getStatusMessage(status));

    final PipeReceiverStatusHandler handler =
        new PipeReceiverStatusHandler(false, 60, false, 60, false, false);
    try {
      handler.handle(status, "generic sink transfer error", "record");
      Assert.fail("Expected a retry exception");
    } catch (final PipeRuntimeSinkNonReportTimeConfigurableException e) {
      Assert.assertEquals("generic sink transfer error", e.getMessage());
    }
  }

  @Test
  public void testStatusMessageKeepsCustomMessageOnSuccessfulRoot() {
    Assert.assertEquals(
        "receiver accepted the request",
        PipeReceiverStatusHandler.getStatusMessage(
            new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode())
                .setMessage("receiver accepted the request")));
  }

  @Test
  public void testPriorStatusUsesLaterMessageForSameCode() {
    final TSStatus status =
        PipeReceiverStatusHandler.getPriorStatus(
            Arrays.asList(
                new TSStatus(
                    TSStatusCode.PIPE_RECEIVER_TEMPORARY_UNAVAILABLE_EXCEPTION.getStatusCode()),
                new TSStatus(
                        TSStatusCode.PIPE_RECEIVER_TEMPORARY_UNAVAILABLE_EXCEPTION.getStatusCode())
                    .setMessage("receiver disk is full")));

    Assert.assertEquals(
        TSStatusCode.PIPE_RECEIVER_TEMPORARY_UNAVAILABLE_EXCEPTION.getStatusCode(),
        status.getCode());
    Assert.assertEquals("receiver disk is full", status.getMessage());
  }

  @Test
  public void testPriorStatusPreservesCustomSuccessMessage() {
    final TSStatus status =
        PipeReceiverStatusHandler.getPriorStatus(
            Collections.singletonList(
                new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode())
                    .setMessage("receiver accepted the request")));

    Assert.assertEquals("receiver accepted the request", status.getMessage());
  }

  @Test
  public void testPriorStatusPrefersNestedConcreteFailureMessage() {
    final TSStatus status =
        PipeReceiverStatusHandler.getPriorStatus(
            Arrays.asList(
                new TSStatus(
                        TSStatusCode.PIPE_RECEIVER_TEMPORARY_UNAVAILABLE_EXCEPTION.getStatusCode())
                    .setMessage("generic receiver failure")
                    .setSubStatus(
                        Collections.singletonList(
                            new TSStatus(TSStatusCode.LOAD_FILE_ERROR.getStatusCode())
                                .setMessage("receiver disk is full"))),
                new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode())));

    Assert.assertEquals("receiver disk is full", status.getMessage());
  }

  @Test
  public void testPriorStatusPropagatesSelectedFailureMessage() {
    final TSStatus status =
        PipeReceiverStatusHandler.getPriorStatus(
            Arrays.asList(
                new TSStatus(
                        TSStatusCode.PIPE_RECEIVER_IDEMPOTENT_CONFLICT_EXCEPTION.getStatusCode())
                    .setMessage("receiver already contains this point"),
                new TSStatus(TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode())
                    .setMessage("root.sg.device"),
                new TSStatus(
                        TSStatusCode.PIPE_RECEIVER_TEMPORARY_UNAVAILABLE_EXCEPTION.getStatusCode())
                    .setMessage("receiver disk is full")));

    Assert.assertEquals(
        TSStatusCode.PIPE_RECEIVER_TEMPORARY_UNAVAILABLE_EXCEPTION.getStatusCode(),
        status.getCode());
    Assert.assertEquals("receiver disk is full", status.getMessage());
    Assert.assertEquals(
        "receiver disk is full", PipeReceiverStatusHandler.getStatusMessage(status));
  }

  @Test
  public void testAuthLogger() {
    final PipeReceiverStatusHandler handler =
        new PipeReceiverStatusHandler(
            CONNECTOR_EXCEPTION_CONFLICT_RESOLVE_STRATEGY_DEFAULT_VALUE.equals("retry"),
            CONNECTOR_EXCEPTION_CONFLICT_RETRY_MAX_TIME_SECONDS_DEFAULT_VALUE,
            CONNECTOR_EXCEPTION_CONFLICT_RECORD_IGNORED_DATA_DEFAULT_VALUE,
            CONNECTOR_EXCEPTION_OTHERS_RETRY_MAX_TIME_SECONDS_DEFAULT_VALUE,
            CONNECTOR_EXCEPTION_OTHERS_RECORD_IGNORED_DATA_DEFAULT_VALUE,
            true);
    PipeReceiverStatusHandler.setLogger(
        new Logger() {
          @Override
          public String getName() {
            return null;
          }

          @Override
          public boolean isTraceEnabled() {
            return false;
          }

          @Override
          public void trace(String msg) {}

          @Override
          public void trace(String format, Object arg) {}

          @Override
          public void trace(String format, Object arg1, Object arg2) {}

          @Override
          public void trace(String format, Object... arguments) {}

          @Override
          public void trace(String msg, Throwable t) {}

          @Override
          public boolean isTraceEnabled(Marker marker) {
            return false;
          }

          @Override
          public void trace(Marker marker, String msg) {}

          @Override
          public void trace(Marker marker, String format, Object arg) {}

          @Override
          public void trace(Marker marker, String format, Object arg1, Object arg2) {}

          @Override
          public void trace(Marker marker, String format, Object... argArray) {}

          @Override
          public void trace(Marker marker, String msg, Throwable t) {}

          @Override
          public boolean isDebugEnabled() {
            return false;
          }

          @Override
          public void debug(String msg) {}

          @Override
          public void debug(String format, Object arg) {}

          @Override
          public void debug(String format, Object arg1, Object arg2) {}

          @Override
          public void debug(String format, Object... arguments) {}

          @Override
          public void debug(String msg, Throwable t) {}

          @Override
          public boolean isDebugEnabled(Marker marker) {
            return false;
          }

          @Override
          public void debug(Marker marker, String msg) {}

          @Override
          public void debug(Marker marker, String format, Object arg) {}

          @Override
          public void debug(Marker marker, String format, Object arg1, Object arg2) {}

          @Override
          public void debug(Marker marker, String format, Object... arguments) {}

          @Override
          public void debug(Marker marker, String msg, Throwable t) {}

          @Override
          public boolean isInfoEnabled() {
            return false;
          }

          @Override
          public void info(String msg) {}

          @Override
          public void info(String format, Object arg) {}

          @Override
          public void info(String format, Object arg1, Object arg2) {}

          @Override
          public void info(String format, Object... arguments) {}

          @Override
          public void info(String msg, Throwable t) {}

          @Override
          public boolean isInfoEnabled(Marker marker) {
            return false;
          }

          @Override
          public void info(Marker marker, String msg) {}

          @Override
          public void info(Marker marker, String format, Object arg) {}

          @Override
          public void info(Marker marker, String format, Object arg1, Object arg2) {}

          @Override
          public void info(Marker marker, String format, Object... arguments) {}

          @Override
          public void info(Marker marker, String msg, Throwable t) {}

          // Warn
          @Override
          public boolean isWarnEnabled() {
            return true;
          }

          @Override
          public void warn(String msg) {
            throw new UnsupportedOperationException();
          }

          @Override
          public void warn(String format, Object arg) {
            throw new UnsupportedOperationException();
          }

          @Override
          public void warn(String format, Object... arguments) {
            throw new UnsupportedOperationException();
          }

          @Override
          public void warn(String format, Object arg1, Object arg2) {
            throw new UnsupportedOperationException();
          }

          @Override
          public void warn(String msg, Throwable t) {
            throw new UnsupportedOperationException();
          }

          @Override
          public boolean isWarnEnabled(Marker marker) {
            return true;
          }

          @Override
          public void warn(Marker marker, String msg) {
            throw new UnsupportedOperationException();
          }

          @Override
          public void warn(Marker marker, String format, Object arg) {
            throw new UnsupportedOperationException();
          }

          @Override
          public void warn(Marker marker, String format, Object arg1, Object arg2) {
            throw new UnsupportedOperationException();
          }

          @Override
          public void warn(Marker marker, String format, Object... arguments) {
            throw new UnsupportedOperationException();
          }

          @Override
          public void warn(Marker marker, String msg, Throwable t) {
            throw new UnsupportedOperationException();
          }

          @Override
          public boolean isErrorEnabled() {
            return false;
          }

          @Override
          public void error(String msg) {}

          @Override
          public void error(String format, Object arg) {}

          @Override
          public void error(String format, Object arg1, Object arg2) {}

          @Override
          public void error(String format, Object... arguments) {}

          @Override
          public void error(String msg, Throwable t) {}

          @Override
          public boolean isErrorEnabled(Marker marker) {
            return false;
          }

          @Override
          public void error(Marker marker, String msg) {}

          @Override
          public void error(Marker marker, String format, Object arg) {}

          @Override
          public void error(Marker marker, String format, Object arg1, Object arg2) {}

          @Override
          public void error(Marker marker, String format, Object... arguments) {}

          @Override
          public void error(Marker marker, String msg, Throwable t) {}
        });
    handler.handle(
        new TSStatus(TSStatusCode.PIPE_RECEIVER_IDEMPOTENT_CONFLICT_EXCEPTION.getStatusCode()),
        "",
        "");
    handler.handle(new TSStatus(TSStatusCode.NO_PERMISSION.getStatusCode()), "", "");
    try {
      handler.handle(new TSStatus(TSStatusCode.NO_PERMISSION.getStatusCode()), "", "", true);
      Assert.fail();
    } catch (final UnsupportedOperationException e) {
      // Expected
    }
    handler.handle(new TSStatus(TSStatusCode.NO_PERMISSION.getStatusCode()), "", "");
    try {
      handler.handle(
          new TSStatus(TSStatusCode.METADATA_ERROR.getStatusCode())
              .setMessage("No permissions for this operation, please add privilege WRITE_DATA"),
          "",
          "",
          true);
      Assert.fail();
    } catch (final UnsupportedOperationException e) {
      // Expected
    }
    PipeReceiverStatusHandler.setLogger(LoggerFactory.getLogger(PipeReceiverStatusHandler.class));
  }
}
